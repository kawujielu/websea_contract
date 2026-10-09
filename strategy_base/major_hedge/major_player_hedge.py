'''
    大户实时跟单策略
    版本信息:v1.0.0
    日期:2025-11-28
    作者:sky

    测试方案:
    1、快速开平仓
    2、未完全对冲时继续加仓,不可以超过最大对冲金额
'''

import sys
import time
import copy
import datetime
import importlib
import traceback
import asyncio
import numpy as np
import major_player_hedge_config
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest


strategy_name = "606019大户对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, id, config):
        super().__init__(scheduler=True, gcc=True)
        self.id = int(id)
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()

        self._load_config(config)   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
        self.wss_log = tb.Log(f'wss_log/{self.id}_wss_data.log')
        self.deal_log = tb.Log(f'deal_log/{self.id}_hedge_deals.log')
        
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))  # CHECK self.tag
                
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

    def _initParams(self):
        """初始化参数
        """
        self.symbols_markprice = {}         # 保存内盘标记价格
        self.contract_unit = {}             # 保存内盘合约单位
        self.symbols_bid_ask = {}           # 保存外盘wss推送来的一档价格
        self.deals_dict = {}                # 保存内盘成交数据
        self.hedge_symbol_precision = {}    # 保存交易对精度
        self.symbol_precision = {}          # 保存ok交易对精度+面值
        self.unhedge_vol_dict = {}          # 保存未对冲量
        self.hedge_clock = False            # 对冲锁
        self.check_hedge_clock = False      # 追单锁
        self.hedge_pos = {}                 # 保存对冲端仓位
        self.wss_filled_list = {}           # 保存okx的wss推送过来的成交订单id
        self.hedge_symbols = []             # 保存对冲端交易对列表
        # self.id = list(self.leads.keys())[0]  # 保存用户id
        self.uid = int(self.leads[self.id])      # 带单员UID列表
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
        self._local_deals = tb.LocalDict('local_follow_data.log')
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})    # 保存所有成交聚合数据
    
    async def on_first(self):
        self.log.add(f"log/{self.id}_follow_hedge.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_hedge_message))
        await asyncio.sleep(1)
        await self.redis_conn.sub_channel(f"market.tag.hold.A2")       # 标记组持仓推送
        await self.sub_bid_ask(self.symbol)

        await asyncio.sleep(0.1)
        await self.hedge_contract_info()    # 更新币对信息
        await self.risk()
        self.log.info(f"初始化完成")
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/1"))  # 每1s执行一次
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*/5"))  # 每5min执行一次
    
    # 订阅内盘标记价格 + okx一档价格
    async def sub_bid_ask(self, symbol):
        await self.redis_conn.sub_channel(f"contract.bids_asks.{symbol}.okex")
        self.log.info(f"subscribe {symbol}: 最优挂单")
    
    async def reload_config(self):
        try:
            importlib.reload(major_player_hedge_config)
            [setattr(self, k, v) for k, v in vars(major_player_hedge_config).items()]
        except:
            self.log.warning(f"reload_config报错{traceback.format_exc()}")

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
        self.log.info(f"交易对精度+面值:{self.symbol_precision}")
    
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
    async def on_hedge_message(self, channel: str, item: dict):
        # print('redis订阅信息:', channel, item)
        if 'bids_asks' in channel and item['messageType'] == 'message':
            symbol = channel.split('.')[2]
            self.symbols_bid_ask[symbol] = [item['bid'], item['ask']]
        # print(f"redis订阅一档价格:{self.symbols_bid_ask}")
        else:
            self.log.info(f"redis订阅信息: {channel}, {item}")
    
    # 获取对冲端一档最新价格    测试成功
    async def get_price(self, symbol):
        while True:
            try:
                bid_one, ask_one = self.symbols_bid_ask[symbol][0], self.symbols_bid_ask[symbol][1]
                self.log.info(f"{symbol}最新盘口价格:{bid_one} {ask_one}")
                return bid_one, ask_one
            except:
                self.log.error(f"获取{symbol}最新价格失败{traceback.format_exc()}")
                await self.sub_bid_ask(symbol)
            await asyncio.sleep(0.5)

    # 内盘wss成交推送
    async def on_adl(self, content):
        # self.log.info(f"ws成交推送数据:{content}")
        self.wss_log.write(f"on_adl ws成交推送数据:{content}")
        {'userId': 55737691, 'uid': 100022, 'tag': 'A2', 'market': 'ETH-USDT', 'orderType': 'limit', 'lastfilledSize': 3.81, 
         'lastfilledprice': '2839.68', 'lastfilledVolume': 10819.18, 'cumfilledSize': 3.81, 'cumfilledVol': '10819.18', 
         'status': 'FILLED', 'amount': 3.81, 'side': 'BUY', 'origQty': 3.81, 'price': '2839.68', 'direction': 'MANY', 
         'positionSide': 'LONG', 'entryPrice': '2839.68', 'liquidationPrice': '2706.10', 'markPrice': '2836.71', 
         'marginType': 'full', 'isAutoAddMargin': 'false', 'isolatedMargin': '58510.4943', 'leverage': 20, 
         'positionAmt': '+411.95', 'unRealizedProfit': '-1223.4915', 'updateTime': 1763658560858}
        notice = f"{strategy_name} 有新推送,检查判断条件是否正常"
        tb.warning(notice, 'notice')
        if content['tag'] == self.tag and content['userId'] == self.uid and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.deal_wss(dict(content))
        
    # 内盘成交处理
    async def deal_wss(self, content):
        symbol = content['market']
        # uid = content['userId']
        if 'positionAmt' in content:
            pos = float(content['positionAmt'])   # TODO 是币的数量？还是张数？
        else:
            pos = 0 # 没有positionAmt字段表示持仓清零
        if symbol in self.deals_dict:
            self.deals_dict[symbol][self.id] = pos
        else:
            self.deals_dict[symbol] = {self.id: pos}
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self):
        # self.log.info(f"风控线程")
        try:
            # 获取账户信息
            balance = await self.okx_rest.fetch_balance()
            text = f"USDT权益:{balance['USDT']['total']} 可用:{balance['USDT']['free']} 冻结:{balance['USDT']['used']}"
            self.log.info(f"ok账户:{text}")
            # 获取内盘仓位
            while 1:
                temp_hold_list = []
                try:
                    data = await self.rest.fetch_hold_list(page_size=20, user_id=self.id)
                    hold_list_page = data['pager']['total_page']
                    temp_hold_list += data['data']
                    for n in range(2, hold_list_page+1):
                        data = await self.rest.fetch_hold_list(page=n, page_size=20, user_id=self.id)
                        temp_hold_list += data['data']['data']
                        await asyncio.sleep(1)
                    break
                except:
                    self.log.warning(f"获取持仓报错:{traceback.format_exc()}")
                await asyncio.sleep(5)
            base_pos = {}
            for i in temp_hold_list:
                side = 1 if i['openDirection'] == 1 else -1
                if i['symbol'] in base_pos:
                    base_pos[i['symbol']] += float(i['amount'])*side
                else:
                    base_pos[i['symbol']] = float(i['amount'])*side
            self.log.info(f"当前用户持仓:{base_pos}")
            # 获取外盘仓位
            text = 'ok账户持仓:\n'
            hedge_pos = {}
            res = await self.okx_rest.fetch_position()  # 仓位带正负
            self.hedge_amt = 0
            for i in res:
                hedge_pos[i['symbol']] = i['contracts']
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
                self.hedge_amt += float(i['contracts'])*float(i['entryPrice'])
            self.log.info(text)
            self.hedge_pos = hedge_pos
            # 外盘大于内盘持仓,报警
            for symbol, pos in hedge_pos.items():
                base = base_pos.get(symbol, 0)
                # 临时处理报警信息，将对冲迷人等待的仓位剔除
                if symbol == 'BTC-USDT':
                    pos -= 0.2868
                elif symbol == 'ETH-USDT':
                    pos -= 2.86
                if abs(pos) > abs(base):
                    self.log.warning(f"风险警告: {symbol}对冲仓位:{pos} 大于 内盘持仓:{base}")
                    title = f"{strategy_name} 内外盘持仓不匹配"
                    warning_mess = f"{strategy_name} {symbol}合约对冲仓位:{pos} 大于 内盘持仓:{base},立即检查持仓问题!!!"
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=warning_mess)
                    await self.send_warning(title, warning_mess)
            for symbol, pos in base_pos.items():
                if pos != 0 and symbol not in hedge_pos and symbol in self.hedge_symbols:
                    self.log.warning(f"风险警告: {symbol}内盘持仓:{pos} 外盘无持仓")
                    title = f"{strategy_name} 内外盘持仓不匹配"
                    warning_mess = f"{strategy_name} {symbol}内盘持仓:{pos} 外盘无持仓,立即检查持仓问题!!!"
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=warning_mess)
                    await self.send_warning(title, warning_mess)
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
        # self.log.info(f"对冲线程,{self.hedge_clock},{self.deals_dict}")
        if self.hedge_clock:    # 正在对冲中
            return
        self.hedge_clock = True
        self.t1 = time.time()*1000
        matchs = copy.deepcopy(self.deals_dict)
        self.this_wss_data = matchs
        # self.log.info(f"要对冲的数据{matchs}")
        for symbol, match in matchs.items():
            # 按交易对并行处理对冲
            if len(match) != 0:
                try:
                    await self.agg_deals(symbol, match)
                except:
                    self.log.error(f"agg报错记录,为了防止多线程警告 {traceback.format_exc()}")
                    tb.warning(f"{strategy_name} {symbol}合约对冲策略agg报错:{traceback.format_exc()}", 'notice')
        self.hedge_clock = False
    
    # 聚合
    async def agg_deals(self, symbol, matchs):
        # 计算聚合成交量
        sum_pos = 0
        for uid, pos in matchs.items():
            sum_pos += pos
        # 多种情况:0——多;0——空;多——0;空——0;多——加多;多——减多;空——加空;空——减空;多——空;空——多
        last_pos = self.local_hedge_pos.get(self.id, {}).get(symbol, 0)
        hedge_size = sum_pos - last_pos
        
        # 下单
        if hedge_size != 0:
            # 本地化要对冲的数量,若对冲失败,也记录进来。只需要处理对冲失败的问题。不要因为对冲失败就不记录,让后面的仓位去累计对冲。价格可能不好,也不容易做盈亏统计。
            self.log.info(f"326调用local_save_hedge_pos:{uid} {symbol} {sum_pos}")
            await self.local_save_hedge_pos(uid, symbol, sum_pos)
            # 调整对冲量,大于1则多对冲,小于1则部分对冲
            hedge_size = hedge_size*getattr(self, 'follow_ratio', 1)
            self.log.info(f"聚合数据:{matchs}, {symbol}切片持仓:{sum_pos}, 上次持仓:{last_pos}, 对冲数量:{hedge_size}")
            # 判断外盘是否有对应交易对
            if symbol not in self.hedge_symbols:
                self.log.error(f"{symbol}外盘无对应交易对,无法对冲")
                return
            # 判断内外盘持仓是否合理
            ok_pos = self.hedge_pos.get(symbol, 0)
            if abs(ok_pos) > abs(sum_pos) * getattr(self, 'follow_ratio', 1):
                self.log.error(f"{symbol}外盘持仓{ok_pos}大于等于内盘持仓{sum_pos},检查持仓")
                # return
            # 判断对冲仓位是否超过最大对冲金额
            if abs(self.hedge_amt) >= self.max_hedge_amt and hedge_size*self.hedge_amt > 0:
                self.log.error(f"对冲金额{int(self.hedge_amt)}U 超过最大对冲金额{self.max_hedge_amt}U,不再增加对冲头寸")
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=f"{self.id}用户对冲金额达到{self.hedge_amt}U,不再开仓,只平仓")
                return
            try:
                await self.hedge(symbol, hedge_size)
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
    
    # 本地化对冲仓位  测试成功
    async def local_save_hedge_pos(self, uid, symbol, pos):
        if uid in self.local_hedge_pos:
            self.local_hedge_pos[uid][symbol] = pos
        else:
            self.local_hedge_pos[uid] = {symbol: pos}
        self.local_hedge_pos = self._local_deals.save(
            {'hedge_pos': self.local_hedge_pos})['hedge_pos']
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos}")
        
    # 对冲
    async def hedge(self, symbol, vol):
        bid_one, ask_one = await self.get_price(symbol)
        price = (bid_one+ask_one)/2
        # 设置最大对冲量
        if abs(vol)*price > self.hedge_amt_limit:
            side = 1 if vol > 0 else -1
            vol = self.hedge_amt_limit/price*side
        thisVol = vol
        if thisVol == 0.0:
            self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
            return
        # depth = await self.okx_rest.fetch_depth(symbol)
        # bid = depth['bids'][0][0]*(1-self.slip)    # CHECK self.slip
        # ask = depth['asks'][0][0]*(1+self.slip)
        if vol > 0:
            # 并行下单
            asyncio.gather(self.maker_hedge_order(self.id, symbol, thisVol, 'buy', 'buy对冲'))
        elif vol < 0:
            # 并行下单
            asyncio.gather(self.maker_hedge_order(self.id, symbol, thisVol, 'sell', 'sell对冲'))
        await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=f"{self.id}用户对冲{thisVol}个{symbol}")
        notice = f"{strategy_name}发生对冲"
        tb.warning(notice, 'notice')
    
    # maker对冲
    async def maker_hedge_order(self, id, symbol, vol, side, reason):
        self.log.info(f"{reason} maker_hedge_order对冲:{id} {symbol} {side} {vol}")
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
                        warning_mess = f"{strategy_name} 对冲下单 {id}用户 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单失败:{res} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    except:
                        warning_mess = f"{strategy_name} 对冲下单 {id}用户 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
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
                        # 本地化
                        save_vol = deal_vol if side == 'buy' else -deal_vol
                        self.log.info(f"479用local_save_hedge_pos:{id} {symbol} {save_vol}")
                        await self.local_save_hedge_pos(id, symbol, save_vol)
                    elif res['status'] == 'canceled' and res['filled'] != 0:
                        deal_vol = res['filled']*face_value    # 成交的币的数量 filled表示张数
                        vol -= deal_vol     # 修改未成交量
                        # 本地化
                        save_vol = deal_vol if side == 'buy' else -deal_vol
                        self.log.info(f"486用local_save_hedge_pos:{id} {symbol} {save_vol}")
                        await self.local_save_hedge_pos(id, symbol, save_vol)
                else:
                    vol -= each_vol     # 修改未成交量
                    # 本地化
                    save_vol = each_vol if side == 'buy' else -each_vol
                    self.log.info(f"492用local_save_hedge_pos:{id} {symbol} {save_vol}")
                    await self.local_save_hedge_pos(id, symbol, save_vol)
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
                    if res['result']:
                        # 本地化
                        save_vol = vol if side == 'buy' else -vol
                        self.log.info(f"516用local_save_hedge_pos:{id} {symbol} {save_vol}")
                        await self.local_save_hedge_pos(id, symbol, save_vol)
                except:
                    title = f"{strategy_name} 下单失败"
                    try:
                        warning_mess = f"{strategy_name} hedge_order函数 {id}用户{symbol}对冲仓位全平 {side}价:{price} 量:{vol} 下单失败:{res} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    except:
                        warning_mess = f"{strategy_name} hedge_order函数 {id}用户{symbol}对冲仓位全平 {side}价:{price} 量:{vol} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                    await self.send_warning(title, warning_mess)
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=warning_mess)
                    await asyncio.sleep(each_interval_time)
            notice = f"{strategy_name}发生对冲"
            tb.warning(notice, 'notice')
        except:
            print(traceback.format_exc())


def main(id, path='major_player_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(id, config).run()



if __name__ == '__main__':
    id = sys.argv[1]
    main(id)





