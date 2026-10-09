'''
    根据交易所全部用户持仓做对冲
    版本信息:v1.0.0
    日期:2026-04-09
    作者:sky

    重点：
    1、判断哪些持仓需要对冲
       判断条件:
       a. 历史盈亏,历史交易笔数大于50笔,整体盈利超过1000u的对冲
       b. 交易风格,高胜率低盈亏的不对冲
       c. 持仓时间小于5min占比超过30%的持仓不对冲
'''

import sys
import time
import copy
import datetime
import importlib
import traceback
import asyncio
import numpy as np
import pos_hedge_config
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.redis_cluster_shard import RedisClusterShard
from config.redis_cfg import RedisConfigClusterAwsPro
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest


strategy_name = "pos_hedge对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()

        self._load_config(config)   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
        self.wss_log = tb.Log(f'wss_log/all_wss_data.log')
        self.deal_log = tb.Log(f'deal_log/all_hedge_deals.log')
        
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号     # CHECK 账户
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.ws_rest = Contract(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        self.rest.DEBUG = False
        self.ws_rest.DEBUG = False
        self.okx_rest.DEBUG = False

    def _initParams(self):
        """初始化参数
        """
        self.symbols_markprice = {}         # 保存内盘标记价格
        self.contract_unit = {}             # 保存内盘合约单位
        self.symbols_bid_ask = {}           # 保存外盘wss推送来的一档价格
        self.hedge_symbol_precision = {}    # 保存交易对精度
        self.symbol_precision = {}          # 保存ok交易对精度+面值
        self.unhedge_vol_dict = {}          # 保存未对冲量
        self.hedge_clock = False            # 对冲锁
        self.check_hedge_clock = False      # 追单锁
        self.wss_filled_list = {}           # 保存okx的wss推送过来的成交订单id
        self.hedge_symbols = []             # 保存对冲端交易对列表
        self.user_symbol_pos = {}           # 每个用户的持仓
        self.last_fetch_time = time.time()  # 上次查询持仓时间
        self.this_wss_data = ""
        
        self.each_vol_check_ts = 0          # 上次查询成交量的时间戳
        self.each_vol_dict = {}             # 保存每次对冲数量
        self.hedge_time = 60                # 对冲时长
        self.hedge_type = 'maker'           # 对冲类型
        self.duration = 1                   # maker挂单持续时间
        self.max_plan_num = self.hedge_time/self.duration    # 最大计划下单次数,根据对冲时长和挂单持续时间计算
        
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_all_data.log')
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})    # 保存所有成交聚合数据
    
    async def listen(self, channel: str, callback):
        # 复用同一个 redis 订阅连接，避免每个频道创建独立连接导致协程异常退出
        if not hasattr(self, "_redis_subscribe_task") or self._redis_subscribe_task is None or self._redis_subscribe_task.done():
            self._redis_subscribe_task = self.loop.create_task(
                self.redis_conn.subscribe_async(channel=[], callback=callback)
            )
            await asyncio.sleep(0.1)
        await self.redis_conn.sub_channel(channel)
        self.log.info(f"订阅成功: {channel}")

    async def on_first(self):
        self.log.add(f"log/all_pos_hedge.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        # 通过rest获取当前所有普通用户的持仓
        await self.get_base_precision()
        pos = await self.fetch_all_hold_list()
        symbols_pos = await self.count_symbols_pos(pos)
        # 订阅外盘一档价格
        await self.sub_symbols(symbols_pos)
        # 订阅普通用户持仓变化推送
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(10)
        await self.redis_conn.sub_channel(f"market.profit.hold")   # 普通用户持仓变化推送
        await self.hedge_contract_info()    # 更新币对信息
        await self.risk(first_tag=True)     # 初始化风险验证
        self.log.info(f"初始化完成")

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/10"))  # 每1s执行一次
        self.schedule.add_job(self.risk, CronTrigger(minute="*/5"))
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*/5"))  # 每5min执行一次
    
    # 订阅外盘一档价格
    async def sub_symbols(self, symbols_pos: dict):
        symbol_list = list(symbols_pos.keys())
        print(f"持仓交易对数量: {len(symbol_list)}")
        for symbol in symbol_list:
            channel = f"contract.bids_asks.{symbol}.binance"
            await self.listen(channel=channel, callback=self.on_message)

    async def send_tg(self, mess):
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1001973998142, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1001973998142, content=send_text)
    
    async def reload_config(self):
        try:
            importlib.reload(pos_hedge_config)
            [setattr(self, k, v) for k, v in vars(pos_hedge_config).items()]
        except:
            self.log.warning(f"reload_config报错{traceback.format_exc()}")
    
    # 获取内盘合约单位
    async def get_base_precision(self):
        res = await self.rest.fetch_precision()
        for s, v in res.items():
            self.contract_unit[s] = v['faceValue']
        self.log.info(f"内盘合约单位:{self.contract_unit}")

    # rest获取对冲端交易对详情
    async def hedge_contract_info(self):
        res = await self.okx_rest.fetch_precision()
        for s, v in res.items():
            self.hedge_symbols.append(s)
            price_precision = int(-np.log10(v['price']))    # 价格精度
            amount_precision = int(-np.log10(v['amount']*v['faceValue']))  # 数量精度
            face_value = v['faceValue']  # 合约面试,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']
            max_limit_amt = v['info']['maxLmtAmt']          # 单笔最大委托金额
            max_limit_sz = v['info']['maxLmtSz']            # 单笔最大委托数量(张)
            self.symbol_precision[s] = (price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz)
        self.log.info(f"外盘交易对精度+面值:{self.symbol_precision}")
    
    async def fetch_all_hold_list(self, page_size: int = 100) -> List[dict]:
        rows: List[dict] = []
        last_page = 0
        while True:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_hold_list(page_size=page_size)
                    total_page = int(data["pager"]["total_page"])
                    for i in data["data"]:
                        rows.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, total_page + 1):
                    last_page = n
                    data = await self.rest.fetch_hold_list(page=n, page_size=page_size)
                    for i in data["data"]:
                        rows.append(i)
                    await asyncio.sleep(0.5)
                break
            except Exception:
                print(traceback.format_exc())
                await asyncio.sleep(5)
        return rows
    
    async def count_symbols_pos(self, pos: List[dict]):
        check_uid_type = []
        for i in pos:
            tag = i['tag']
            if 'E' in tag:
                continue
            uid = i['user_id']
            symbol = i['symbol']
            side = i['openDirection']
            user_pos = float(i['tradeNum']) if side == 1 else -float(i['tradeNum'])  # 币的数量(非张数)
            # profit = float(i['profit_loss'])
            if symbol in self.user_symbol_pos:
                if uid in self.user_symbol_pos[symbol]:
                    if user_pos > 0:
                        self.user_symbol_pos[symbol][uid][0] = user_pos
                    else:
                        self.user_symbol_pos[symbol][uid][1] = user_pos
                else:
                    if user_pos > 0:
                        self.user_symbol_pos[symbol][uid] = [user_pos, 0]
                    else:
                        self.user_symbol_pos[symbol][uid] = [0, user_pos]
            else:
                if user_pos > 0:
                    self.user_symbol_pos[symbol] = {uid: [user_pos, 0]}
                else:
                    self.user_symbol_pos[symbol] = {uid: [0, user_pos]}
        # user_symbol_pos数据聚合成{symbol: sum(pos)}
        symbols_pos = {}
        for symbol, pos_dict in self.user_symbol_pos.items():
            # print(symbol, pos_dict)
            symbols_pos[symbol] = sum([v[0] + v[1] for v in pos_dict.values()])
        self.log.info(f"初始化用户持仓1:{symbols_pos}")
        return symbols_pos
    
    # 发送报警信息    测试成功
    async def send_warning(self, title, warning_mess):
        try:
            self.log.error(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            self.log.warning(f"send_warning发送报警信息失败:{traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    # 接收订阅的外盘一档价格    测试成功
    async def on_message(self, channel: str, item: dict):
        # print('redis订阅信息:', channel, item)
        if 'bids_asks' in channel and 'binance' in channel:
            # contract.bids_asks.ALLO-USDT.binance {'exchange': 'binance', 
            # 'symbol': 'ALLO-USDT', 'ask': 0.11006, 'askVolume': 517, 
            # 'bid': 0.11003, 'bidVolume': 11867, 'timestamp': 1775721420637, 
            # 'messageType': 'message', 'info': {'data': {'A': '517', 'B': '11867', 'E': 1775721420637, 'T': 1775721420637, 'a': '0.1100600', 'b': '0.1100300', 'e': 'bookTicker', 's': 'ALLOUSDT', 'u': 10285949731370}, 'stream': 'allousdt@bookTicker'}}
            self.symbols_bid_ask[item['symbol']] = [item['bid'], item['ask']]
            return
        if 'hold' in channel and isinstance(item, dict) and 'msg' not in item:
            self.wss_log.write(f"{datetime.datetime.now()} wss订阅信息: {item}")
            # {'userId': 482679, 'userUid': 59025574, 'symbol': 'ETH-USDT', 
            # 'markPrice': '2179.9232295564516', 'avgPrice': '2180.453636363636363636', 
            # 'openDirection': 2, 'amount': '0.11', 'margin_type': 'crossed'}
            uid = item['userId']
            symbol = item['symbol']
            side = item['openDirection']
            pos = float(item['amount'])
            # 当pos是0的时候，openDirection的方向表示平仓的方向。比如openDirection是1表示平多，openDirection是2表示平空
            if symbol in self.user_symbol_pos:
                if uid in self.user_symbol_pos[symbol]:
                    if pos > 0:
                        self.user_symbol_pos[symbol][uid][0] = pos
                    elif pos < 0:
                        self.user_symbol_pos[symbol][uid][1] = pos
                    else:
                        if side == 1:
                            self.user_symbol_pos[symbol][uid][0] = 0
                        else:
                            self.user_symbol_pos[symbol][uid][1] = 0
                else:
                    if pos > 0:
                        self.user_symbol_pos[symbol][uid] = [pos, 0]
                    elif pos < 0:
                        self.user_symbol_pos[symbol][uid] = [0, pos]
            else:
                if pos > 0:
                    self.user_symbol_pos[symbol] = {uid: [pos, 0]}
                else:
                    self.user_symbol_pos[symbol] = {uid: [0, pos]}
    
    # 获取对冲端一档最新价格    测试成功
    async def get_price(self, symbol):
        while True:
            try:
                # depth = await self.rest.fetch_depth(symbol=symbol, limit=1)
                # bid_one = depth['bids'][0][0]
                # ask_one = depth['asks'][0][0]
                if symbol in self.symbols_bid_ask:
                    bid_one, ask_one = self.symbols_bid_ask[symbol][0], self.symbols_bid_ask[symbol][1]
                else:
                    depth = await self.rest.fetch_depth(symbol=symbol, limit=1)
                    bid_one = depth['bids'][0][0]
                    ask_one = depth['asks'][0][0]
                    self.log.info(f"rest查询 {symbol}内盘最新盘口价格:{bid_one} {ask_one}")
                return bid_one, ask_one
            except:
                self.log.error(f"获取{symbol}最新价格失败{traceback.format_exc()}")
            await asyncio.sleep(0.5)
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self, first_tag: bool = False):
        self.log.info(f"风控线程")
        if first_tag:
            # 获取外盘仓位
            text = 'ok账户持仓:\n'
            hedge_pos = {}
            res = await self.okx_rest.fetch_position()  # 仓位带正负
            for i in res:
                hedge_pos[i['symbol']] = i['contracts']
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
            self.log.info(text)
            # 初始化时更新本地数据
            for symbol, pos_dict in self.user_symbol_pos.items():
                if symbol in self.symbols:
                    base_pos = sum([v[0] + v[1] for v in pos_dict.values()])
                    await self.local_save_hedge_pos(symbol, base_pos, hedge_pos.get(symbol, 0))
            return
        try:
            # 获取账户信息
            balance = await self.okx_rest.fetch_balance()
            text = f"USDT权益:{balance['USDT']['total']} 可用:{balance['USDT']['free']} 冻结:{balance['USDT']['used']}"
            self.log.info(f"ok账户:{text}")
            # 获取内盘仓位
            temp_hold_list = []
            last_page = 0
            while 1:
                try:
                    if last_page == 0:
                        data = await self.rest.fetch_hold_list(page_size=100)
                        total_page = int(data["pager"]["total_page"])
                        temp_hold_list += data['data']
                    begin = 2 if last_page == 0 else last_page
                    for n in range(begin, total_page + 1):
                        last_page = n
                        data = await self.rest.fetch_hold_list(page=n, page_size=100)
                        temp_hold_list += data['data']
                        await asyncio.sleep(0.5)
                    break
                except:
                    self.log.warning(f"获取持仓报错:{traceback.format_exc()}")
                await asyncio.sleep(5)
            base_pos = {}
            for i in temp_hold_list:
                tag = i['tag']
                if 'E' in tag:
                    continue
                symbol = i['symbol']
                side = 1 if i['openDirection'] == 1 else -1
                amount = float(i['amount'])*side*self.contract_unit.get(symbol, 0)
                if symbol in base_pos:
                    base_pos[symbol] += amount
                else:
                    base_pos[symbol] = amount
            self.log.info(f"risk查询当前用户持仓:{base_pos}")
            wss_symbol_pos = {}
            for symbol, pos_dict in self.user_symbol_pos.items():
                wss_symbol_pos[symbol] = sum([v[0] + v[1] for v in pos_dict.values()])
            # 对冲symbols_pos和self.symbols_pos的差别
            for symbol, pos in base_pos.items():
                if symbol in wss_symbol_pos:
                    diff = pos - wss_symbol_pos[symbol]
                    bid_one, ask_one = await self.get_price(symbol)
                    amt = diff * (bid_one + ask_one) / 2
                    if abs(diff) > 10:
                        self.log.info(f"{symbol} rest持仓:{pos} wss持仓:{wss_symbol_pos[symbol]} diff:{round(amt, 2)}U")
                await asyncio.sleep(0.1)
            # 获取外盘仓位
            text = 'ok账户持仓:\n'
            hedge_pos = {}
            res = await self.okx_rest.fetch_position()  # 仓位带正负
            for i in res:
                hedge_pos[i['symbol']] = i['contracts']
                now_price = float(i['markPrice'])
                liq_price = float(i['info']['liqPx'])
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
                # 爆仓警报
                if i['contracts'] > 0:
                    if not liq_price:
                        continue
                    if now_price/liq_price < 1.3:
                        mess = f"{strategy_name} {i['symbol']}合约爆仓警报,当前价格:{now_price} 爆仓价:{liq_price} 立即减仓!!!"
                        self.log.warning(mess)
                        await self.send_tg(mess)
                elif i['contracts'] < 0:
                    if not liq_price:
                        continue
                    if liq_price/now_price < 1.3:
                        mess = f"{strategy_name} {i['symbol']}合约爆仓警报,当前价格:{now_price} 爆仓价:{liq_price} 立即减仓!!!"
                        self.log.warning(mess)
                        await self.send_tg(mess)
            self.log.info(text)

            # 根据rest查询的外盘持仓,验证本地记录是否正确 
            for symbol, pos in hedge_pos.items():
                last_pos = self.local_hedge_pos.get(symbol, [0, 0])[1]
                if pos != last_pos:
                    self.log.warning(f"风险警告: {symbol}外盘持仓:{pos} 与本地记录:{last_pos} 不一致")
                    # base_pos = self.local_hedge_pos.get(symbol, [0, 0])[0]
                    # await self.local_save_hedge_pos(symbol, base_pos, pos)
            
            # 外盘大于内盘持仓,报警
            for symbol, pos in hedge_pos.items():
                base = base_pos.get(symbol, 0)
                if abs(pos) > abs(base):
                    self.log.warning(f"风险警告: {symbol}对冲仓位:{pos} 大于 内盘持仓:{base}")
                    title = f"{strategy_name} 内外盘持仓不匹配"
                    warning_mess = f"{strategy_name} {symbol}合约对冲仓位:{pos} 大于 内盘持仓:{base},立即检查持仓问题!!!"
                    await self.send_warning(title, warning_mess)
                    await self.send_tg(warning_mess)
            # for symbol, pos in base_pos.items():
            #     if pos != 0 and symbol not in hedge_pos and symbol in self.hedge_symbols:
            #         self.log.warning(f"风险警告: {symbol}内盘持仓:{pos} 外盘无持仓")
            #         title = f"{strategy_name} 内外盘持仓不匹配"
            #         warning_mess = f"{strategy_name} {symbol}内盘持仓:{pos} 外盘无持仓,立即检查持仓问题!!!"
            #         await self.send_warning(title, warning_mess)
            
            # 验证wss推送持仓和rest查持仓的区别
            # 如果超过1小时，则重新查询fetch_all_hold_list函数
            # if time.time() - self.last_fetch_time > 3600:
            #     pos = await self.fetch_all_hold_list()
            #     symbols_pos = {}
            #     for i in pos:
            #         tag = i['tag']
            #         if 'E' in tag:
            #             continue
            #         symbol = i['symbol']
            #         side = i['openDirection']
            #         pos = float(i['tradeNum']) if side == 1 else -float(i['tradeNum'])
            #         # profit = float(i['profit_loss'])
            #         if symbol in symbols_pos:
            #             symbols_pos[symbol] += pos
            #         else:
            #             symbols_pos[symbol] = pos
            #     self.log.info(f"risk定时查询用户持仓:{symbols_pos}")
            #     self.last_fetch_time = time.time()
            #     wss_symbol_pos = {}
            #     for symbol, pos_dict in self.user_symbol_pos.items():
            #         symbols_pos[symbol] = sum([v[0] + v[1] for v in pos_dict.values()])
            #     # 对冲symbols_pos和self.symbols_pos的差别
            #     for symbol, pos in symbols_pos.items():
            #         if symbol in wss_symbol_pos:
            #             diff = pos - wss_symbol_pos[symbol]
            #             bid_one, ask_one = await self.get_price(symbol)
            #             amt = diff * (bid_one + ask_one) / 2
            #             if diff != 0:
            #                 self.log.info(f"{symbol} rest持仓:{pos} wss持仓:{wss_symbol_pos[symbol]} diff:{round(amt, 2)}U")
            #         await asyncio.sleep(0.1)
        except:
            self.log.error(f"风险验证报错 {traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' =================================== function =============================='''
    ''' ==========================================================================='''
    # 统计每笔订单成交量均值
    async def count_vol(self, symbol):
        price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz = await self.get_precision(symbol)
        res = await self.okx_rest.fetch_trade(symbol=symbol, limit=100)
        vol = 0
        for i in res:
            vol += float(i['amount'])
        each_vol = vol/len(res)*face_value
        self.each_vol_dict[symbol] = each_vol
        self.log.info(f"{symbol}近{len(res)}笔成交量均值:{each_vol:.2f}")
    
    # 拿最新盘口价格计算maker价格
    async def count_maker_price(self, symbol, side, price_precision):
        bid_one, ask_one = await self.get_price(symbol)
        bid_price = round(bid_one+1/10**price_precision, price_precision)
        ask_price = round(ask_one-1/10**price_precision, price_precision) 
        price = min(bid_price, ask_price) if side == 'buy' else max(bid_price, ask_price)
        self.log.info(f"最新{symbol}盘口价格:bid_one:{bid_one}, ask_one:{ask_one} 计算maker价格:{price}")
        return price
    
    # 获取精度
    async def get_precision(self, symbol):
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz = self.symbol_precision[
                symbol]
        except:
            price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz = None, None, None, None, None, None
        return price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz
    
    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''
    
    async def handle_deals(self):
        self.log.info(f"handle_deals函数:{self.hedge_clock}")
        if self.hedge_clock:    # 正在对冲中
            return
        self.hedge_clock = True
        self.t1 = time.time()*1000
        agg_symbols_pos = {}
        for symbol, pos_dict in self.user_symbol_pos.items():
            agg_symbols_pos[symbol] = sum([v[0] + v[1] for v in pos_dict.values()])
        for symbol, pos in agg_symbols_pos.items():
            if symbol not in self.symbols:
                continue
            hedge_amt_limit = self.symbol_limit.get(symbol, 0)        # 敞口金额阈值
            # 没有设置阈值则不对冲
            if hedge_amt_limit == 0:
                self.log.info(f"{symbol}没有设置阈值,不对冲.净敞口金额:{diff_amt}U 对冲仓位金额:{hedge_amt}U")
                continue
            last_pos = self.local_hedge_pos.get(symbol, [0, 0])[1]    # 本地记录的已对冲数量
            bid_one, ask_one = await self.get_price(symbol)
            mark_price = (bid_one+ask_one)/2

            # 若symbol持仓是0，或持仓小于对冲阈值。则对冲量是已对冲的数量
            if pos == 0 or abs(pos*mark_price) <= hedge_amt_limit:
                this_hedge_size = -last_pos
            else:
                hedge_size = pos - last_pos  # 内盘持仓 - 已对冲仓位
                # 计算本次对冲数量
                if pos > 0:
                    this_hedge_size = hedge_size-hedge_amt_limit/mark_price if hedge_size > 0 \
                                 else hedge_size+hedge_amt_limit/mark_price
                else:
                    this_hedge_size = hedge_size+hedge_amt_limit/mark_price if hedge_size < 0 \
                                 else hedge_size-hedge_amt_limit/mark_price
            pos_amt = int(pos * mark_price)     # 内盘持仓金额
            hedge_amt = int(last_pos * mark_price)   # 对冲仓位金额
            diff_amt = int(hedge_size * mark_price)   # 净敞口金额
            self.log.info(f"内盘持仓量:{pos} 金额:{pos_amt}U 已对冲:{last_pos} 金额:{hedge_amt}U 净敞口:{hedge_size} 金额:{diff_amt}U")
            
            # 按比例对冲
            # this_hedge_size = 0.1 if this_hedge_size > 0 else -0.1
            if abs(this_hedge_size*mark_price) > self.max_hedge_amt:
                this_hedge_size = self.max_hedge_amt/mark_price if this_hedge_size > 0 else -self.max_hedge_amt/mark_price
            self.log.info(f"本次对冲:{this_hedge_size} 已对冲总量:{last_pos}")
            if abs(last_pos) > 1 and last_pos * this_hedge_size > 0:
                self.log.info(f"{symbol}外盘持仓大于最大对冲数量,不继续对冲")
                continue
            # 本地记录内盘净持仓，外盘对冲仓位.只是记录
            await self.local_save_hedge_pos(symbol, pos, last_pos+this_hedge_size)
            self.log.info(f"{symbol}内盘净持仓:{pos}, 已对冲:{last_pos}, 本次对冲:{this_hedge_size}")
            # 判断外盘是否有对应交易对
            if symbol not in self.hedge_symbols:
                self.log.error(f"{symbol}外盘无对应交易对,无法对冲")
                return
            try:
                await self.hedge(symbol, this_hedge_size)
                self.hedge_clock = False
                self.log.info(f"对冲耗时:{time.time() * 1000 - self.t1}ms")
            except KeyboardInterrupt as e:   # 修改
                self.hedge_clock = False
                self.log.error(f"手动停止 {traceback.format_exc()}")
                raise e
            except:
                self.hedge_clock = False
                self.log.error((f'此条pendingTask处理失败! {traceback.format_exc()}'))
                title = f"{strategy_name}对冲报错"
                warning_mess = f"{strategy_name}{symbol}合约对冲策略hedge报错:hedge函数报错信息:{traceback.format_exc()}"
                await self.send_warning(title, warning_mess)
        self.hedge_clock = False
    
    # 本地化对冲仓位  测试成功
    async def local_save_hedge_pos(self, symbol, pos, hedge_pos):
        self.local_hedge_pos[symbol] = [pos, hedge_pos]
        self.local_hedge_pos = self._local_deals.save(
            {'hedge_pos': self.local_hedge_pos})['hedge_pos']
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos}")
        
    # 对冲
    async def hedge(self, symbol, vol):
        thisVol = vol
        if thisVol == 0.0:
            self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
            return
        if vol > 0:
            # 并行下单
            asyncio.gather(self.maker_hedge_order(symbol, thisVol, 'buy', 'buy对冲'))
        elif vol < 0:
            # 并行下单
            asyncio.gather(self.maker_hedge_order(symbol, thisVol, 'sell', 'sell对冲'))
        notice = f"{strategy_name}发生对冲"
        tb.warning(notice, 'notice')
    
    # maker对冲
    async def maker_hedge_order(self, symbol, vol, side, reason):
        self.log.info(f"{reason} maker_hedge_order对冲:{symbol} {side} {vol}")
        #return
        # 对冲时间的区间固定,每笔下单量动态计算,每笔下单间隔根据前两项计算
        # 价格取最新盘口价
        try:
            all_vol = abs(vol)
            vol = abs(vol)
            if time.time() - self.each_vol_check_ts > 120 or symbol not in self.each_vol_dict:
                await self.count_vol(symbol)
                self.each_vol_check_ts = time.time()
            each_vol = self.each_vol_dict[symbol]   # 每笔下单量
            vol_ratio = 1 if each_vol/vol > 1 else each_vol/vol  # 计算每笔下单量占总下单量的比例
            each_interval_time = vol_ratio*self.hedge_time   # 每笔下单间隔时间
            ave_vol_plan_num = int(vol/each_vol)   # 计划对冲次数
            ave_vol_plan_num = 1 if ave_vol_plan_num < 1 else ave_vol_plan_num
            plan_num = min(ave_vol_plan_num, self.max_plan_num)
            self.log.info(f"对冲时间:{self.hedge_time} 每笔下单量:{each_vol} 总下单量:{vol} 占比:{vol_ratio} 每笔下单间隔:根据成交量{each_interval_time} 计划对冲次数:{ave_vol_plan_num} 根据时间{plan_num}(取这个值)")
            
            # 算法交易核心
            # 计算下单价
            # 下单
            # 等3s + wss接收成交信息,完全成交则立即继续下单
            # 撤单
            # 检查成交情况
            # 重新计算下单量,时间间隔不变,每次提高下单量
            # 时间超过60s,直接下taker单
            price_precision, amount_precision, face_value, hedge_vol_limit, max_limit_amt, max_limit_sz = await self.get_precision(symbol)
            if symbol == 'ETH-USDT':
                min_vol = hedge_vol_limit
            else:
                min_vol = face_value*hedge_vol_limit
            start_time = time.time()
            num = 1
            while time.time() - start_time < 60:
                if num < plan_num:
                    each_vol = abs(round(vol/(plan_num-num), amount_precision))  # 每次重新计算下单量,若maker单未完全成交,意味着每次下单量会增加
                    self.log.info(f"下单量验证:each_vol:{each_vol} vol:{vol} plan_num:{plan_num} num:{num}")
                else:
                    each_vol = round(vol, amount_precision)
                num += 1
                price = await self.count_maker_price(symbol, side, price_precision)   # 每次重新计算对冲价格
                self.log.info(f"{reason} 总对冲量:{all_vol} 未对冲量:{vol} 最小对冲量:{min_vol} 第{num}次maker对冲,价格:{price} 数量:{each_vol} 方向:{side}")
                try:
                    t1 = time.time()*1000
                    res = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                           side=side, amount=abs(each_vol),
                                                           price=price, tdMode='cross')
                    deal_log = f"{datetime.datetime.now()} maker对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{each_vol} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{res}"
                    self.log.info(deal_log)
                    self.deal_log.write(deal_log)
                    order_id = res['id']
                except:
                    title = f"{strategy_name} 下单失败"
                    try:
                        warning_mess = f"{strategy_name} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单失败:{res} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    except:
                        warning_mess = f"{strategy_name} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                    self.log.warning(warning_mess)
                    await self.send_warning(title, warning_mess)
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=warning_mess)
                    await asyncio.sleep(each_interval_time)
                    continue
                # 等3s + wss接收成交信息,完全成交则立即继续下单
                # deal_ts = time.time()
                # wss_deal_filled = False
                # while (time.time() - deal_ts) < self.duration:
                #     self.log.info(f"等待成交,order_id:{order_id} wss_filled_list:{self.wss_filled_list}")
                #     if order_id in self.wss_filled_list:
                #         wss_deal_filled = True
                #         break
                #     await asyncio.sleep(0.1)
                await asyncio.sleep(1)
                # if not wss_deal_filled:
                if 1:
                    # 撤单
                    res = await self.okx_rest.cancel_order(symbol, order_id=order_id)
                    self.log.info(f"撤单结果:{res}")
                    # 检查成交情况
                    res = await self.okx_rest.fetch_order_detail(symbol, order_id=order_id)
                    self.log.info(f"撤单后查订单成交数量:{res}")
                    if res['status'] == 'closed':
                        deal_vol = res['amount']    # 成交的币的数量 filled表示张数
                        vol -= deal_vol     # 修改未成交量
                    elif res['status'] == 'canceled' and res['filled'] != 0:
                        deal_vol = res['filled']*face_value    # 成交的币的数量 filled表示张数
                        vol -= deal_vol     # 修改未成交量
                if vol <= min_vol:
                    self.log.info(f"全部对冲完毕,对冲次数:{num} 计划对冲次数:{plan_num} 剩余对冲量:{vol}")
                    break
            # 超过60s,未成交部分直接下taker单
            if vol > min_vol:   # 应该是这样写，但因为okx查询持仓返回的持仓比最小交易单位少一位，所以才这样
                vol = round(vol, amount_precision)
                bid_one, ask_one = await self.get_price(symbol)
                price = round(ask_one*(1+self.slip), price_precision) if side == 'buy' else \
                        round(bid_one*(1-self.slip), price_precision)
                self.log.info(f"{reason} 超过60s,未成交部分直接下taker单,对冲价格:{price} 数量:{vol} 方向:{side}")
                try:
                    t1 = time.time()*1000
                    deal_log = f"maker超时后taker对冲 交易对:{symbol} 价格:{price} 数量:{vol} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms"
                    self.log.info(deal_log)
                    res = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                           side=side, amount=abs(vol),
                                                           price=price, tdMode='cross')
                    deal_log = f"maker超时后taker对冲 交易对:{symbol} 价格:{price} 数量:{vol} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{res}"
                    self.deal_log.write(deal_log)
                except:
                    title = f"{strategy_name} 下单失败"
                    try:
                        warning_mess = f"{strategy_name} hedge_order函数 {symbol}对冲仓位全平 {side}价:{price} 量:{vol} 下单失败:{res} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    except:
                        warning_mess = f"{strategy_name} hedge_order函数 {symbol}对冲仓位全平 {side}价:{price} 量:{vol} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    await self.send_warning(title, warning_mess)
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=warning_mess)
                    await asyncio.sleep(each_interval_time)
            notice = f"{strategy_name}发生对冲"
            tb.warning(notice, 'notice')
        except:
            print(traceback.format_exc())


def main(path='pos_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()


