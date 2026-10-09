'''
    策略逻辑：
    根据标记用户或指定用户的持仓成本
    在成本之下做对冲

    2025-04-21 update:
    1、将rest查询全量用户持仓,修改为wss推送
    2、定时用rest检查本地维护的用户持仓是否正确

    2025-06-03 update:
    1、外盘仓位小于内盘仓位,报警只报一次?
    2、还是会出现外盘多对冲的情况       
    3、记录成交推送数据     优化tag参数类型,从list改为str

    2025-06-12 update:
    1、本地化对冲计划+已对冲仓位
    2、满足代码功能,可用一个对冲账户同时对冲多个用户,且不与其他账户持仓混淆

    2025-07-16 update大版本更新:
    将taker对冲改成maker对冲
    对冲1min后未成交部分taker对冲
    TODO 关键点是已对冲数量的本地化放在下单前?还是挂maker单后?  下单前
    下单前更新:万一下单失败,后续的减仓也会失败。
    下单后更新:用户秒级持仓,平仓单可能会报错导致无法平仓
    TODO later_order一个是str一个是int   直接全部对冲
    TODO 最关键的还是持仓方向问题？内盘双向持仓？    已处理双向持仓
    
    模块一:拆单
    模块二:maker对冲
'''


import sys
import time
import datetime
import math
import copy
import traceback
import asyncio
import importlib
import numpy as np
import test_config
sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from client.env_pro.rest.websea.contract import WebseaContract as old_ws_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest
from crypto_center.client.rest.websea.contract_quan import WebseaContract as ws_contract_rest

strategy_name = "grid_hedge_v4网格对冲535229用户策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, id, symbol, config):
        super().__init__(scheduler=True, gcc=True)
        self.id = int(id)
        self.symbol = symbol
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1

        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
        self._setLocalDict()       # 配置本地数据
        self.deal_log = tb.Log(f'deal_log/{self.id}_hedge_deals.log')
        self.wss_log = tb.Log(f"wss_log/{self.id}_wss.log")
        self.hedge_wss_log = tb.Log(f"wss_log/{self.id}_hedge_wss.log")
        
        # 订阅base数据
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl    # 标记用户回调函数
        self.loop.create_task(self.ws_wss.only_subscribe())
        time.sleep(0.5) # 为了可以正常订阅上成交推送
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rc_task = rc.RestClient()
        self.old_ws_rest = old_ws_rest(config['base_token'], config['base_secret'])
        self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])

    def _initParams(self):
        """初始化参数
        """
        self.symbol_size = {}       # 内盘合约单位
        self.symbol_precision = {}  # 交易对精度+面值
        self.base_pos = {}          # 内盘持仓
        self.hedge_pos = {}         # 已对冲信息
        self.last_base_pos = {}     # 上一次内盘持仓
        self.hedge_pos_risk = {}    # 对冲持仓
        self.symbols_bid_ask = {}   # 保存外盘wss推送来的一档价格
        self.hold_list = []         # 标签用户持仓数据
        self.diff_num = 0           # 仓位不一致的次数
        self.send_tg_ts = 0         # 发送tg时间戳
        self.last_base_pos_mess = ""    # 上次内盘持仓数据
        self.uid = self.leads[self.id]  # 根据id,获取用户uid
        self.save_wss_data = []     # 保存wss推送数据
        self.order_num = 0          # 对冲自定义ID编号
        self.this_wss_data = ""
        self.temp_update_data = []  # 保存本地更新的内容
        self.hedge_order_id = {}    # 保存所有maker订单
        self.wss_deals = {}         # 记录wss推送过来的成交信息
        self.last_risk_ts = 0       # 上次浮亏报警时间戳
        self.real_hedge_ratio = 0   # 已对冲金额占总对冲金额比例
        self.each_vol_check_ts = 0  # 最后一次检查每分钟平均成交量的时间戳
        self.each_vol_dict = {}     # 保存每分钟平均成交量
        
        # 测试数据
        self.first = True
    
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_grid_deals.log')
        self.local_base_deals = self._local_deals.load().get('deals', {})    # 保存所有成交聚合数据
        self.local_later_order = self._local_deals.load().get('later_order', {})  # 保存所有对冲挂单id
        # self.local_hedge_orderid = self._local_deals.load().get('later_order', {})  # 保存所有对冲挂单id
        # 初始化更新一次,cancel_close_orders函数验证减仓时更新一次
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})   # 保存所有对冲持仓
        self.local_unhedge_pos = self._local_deals.load().get('unhedge_pos', {})   # 保存所有未对冲持仓
    
    async def on_first(self):
        self.log.add(f"log/{self.id}_{self.symbol}_log.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        
        channel_pos = ["contract.balance.okex", "contract.order.okex"]
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_ok_deal))
        self.log.info("subscribe: okex成交")

        await self.get_symbol_unit()        # 内盘合约单位
        await self.hedge_contract_info()    # 更新币对信息
        await self.risk(True)               # 判断是否对冲了计划内的仓位
        
        # res = await self.ws_rest.fetch_follow_sum(trader_id=485142, token='c1cf4185b2bed317aeb6e6674491fbef')
        # print(f"跟单比例数据:{res}")

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/3"))  # 每3s执行一次
        self.schedule.add_job(self.check_deal, CronTrigger(second="*/3"))
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*/5"))  # 每5min执行一次
        
    async def sub_bid_ask(self, symbol):
        channel_asks_bids = f"contract.bids_asks.{symbol}"
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_asks_bids, callback=self.on_hedge_message))
        self.log.info(f"subscribe: {symbol}最优挂单")
    
    async def reload_config(self):
        try:
            importlib.reload(test_config)
            [setattr(self, k, v) for k, v in vars(test_config).items()]
        except:
            self.log.warning(f"reload_config报错{traceback.format_exc()}")

    async def count_vol(self, symbol):
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        res = await self.okx_rest.fetch_trade(symbol=symbol, limit=100)
        vol = 0
        for i in res:
            vol += float(i['amount'])
        each_vol = vol/len(res)*face_value
        self.each_vol_dict[symbol] = each_vol
        self.log.info(f"{symbol}近{len(res)}笔成交量均值:{each_vol:.2f}")
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    # 接收ok账户权益推送
    # async def on_ok_balance(self, channel: str, item: dict):
    #     if channel == "contract.balance.okex":
    #         self.total = item['balance']['USDT']['total']
    #         self.log.info(f"wss推送的ok账户权益:{self.total}")
    
    # 接收ok成交推送
    async def on_ok_deal(self, channel: str, item: dict):
        if channel == "contract.balance.okex":
            self.total = item['balance']['USDT']['total']
            self.hedge_wss_log.write(f"wss推送的ok账户权益:{self.total}")
        if channel == "contract.order.okex":
            self.hedge_wss_log.write(f"wss推送的deal数据:{self.total}")
            # contract.order.okex, 
            # {'exchange': 'okex', 'symbol': '', 'id': '', 'clientOrderId': '', 'price': 0, 
            # 'stopPrice': None, 'triggerPrice': None, 'amount': 0, 'side': '', 'type': '', 'status': '', 
            # 'leverage': 0, 'timeInForce': None, 'postOnly': None, 'reduceOnly': False, 'marginMode': '', 
            # 'average': 0, 'filled': 0, 'cost': None, 'remaining': 0, 'takeProfitPrice': 0, 
            # 'stopLossPrice': 0, 'fee': {'feeCurrency': '', 'cost': 0, 'rate': None}, 
            # 'tradeAmt': 0, 'timestamp': 0, 'messageType': 'message', 'info': {'arg': {'channel': 'orders', 
            # 'instType': 'SWAP', 'uid': '678147541502075525'}, 'data': [{'accFillSz': '0.1', 
            # 'algoClOrdId': '', 'algoId': '', 'amendResult': '', 'amendSource': '', 'attachAlgoClOrdId': '', 
            # 'attachAlgoOrds': [], 'avgPx': '0.7465', 'cTime': '1752652951323', 'cancelSource': '', 
            # 'category': 'normal', 'ccy': '', 'clOrdId': '', 'code': '0', 'execType': 'M', 
            # 'fee': '0.0001493', 'feeCcy': 'USDT', 'fillFee': '0.0001493', 'fillFeeCcy': 'USDT', 
            # 'fillFwdPx': '', 'fillIdxPx': '0.7466', 'fillMarkPx': '0.7467', 'fillMarkVol': '', 
            # 'fillNotionalUsd': '7.465000000000001', 'fillPnl': '0', 'fillPx': '0.7465', 'fillPxUsd': '', 
            # 'fillPxVol': '', 'fillSz': '0.1', 'fillTime': '1752653060423', 'instId': 'ADA-USDT-SWAP', 
            # 'instType': 'SWAP', 'isTpLimit': 'false', 'lastPx': '0.7465', 'lever': '3', 
            # 'linkedAlgoOrd': {'algoId': ''}, 'msg': '', 'notionalUsd': '7.465000000000001', 
            # 'ordId': '2689406224158154752', 'ordType': 'post_only', 'pnl': '0', 'posSide': 'net', 
            # 'px': '0.7465', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'quickMgnType': '', 'rebate': '0', 
            # 'rebateCcy': 'USDT', 'reduceOnly': 'false', 'reqId': '', 'side': 'buy', 'slOrdPx': '', 
            # 'slTriggerPx': '', 'slTriggerPxType': '', 'source': '', 'state': 'filled', 'stpId': '', 
            # 'stpMode': 'cancel_maker', 'sz': '0.1', 'tag': '', 'tdMode': 'cross', 'tgtCcy': '', 
            # 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'tradeId': '254459198', 
            # 'tradeQuoteCcy': '', 'uTime': '1752653060423'}]}}
            # TODO 若发现rest查到持仓比，推送来的成交量大，则漏推，报警，以查到的持仓为准。以上全部本地化
            # 推送的成交量不带方向
            state = item['info']['data'][0]['state']
            if state in ['partially_filled', 'filled']:
                self.log.info(f"on_ok_deal成交推送:{channel}, {item}")
                symbol = item['info']['data'][0]['instId'][:-5]
                price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
                if price_precision is None and amount_precision is None:
                    self.log.error(f"获取{symbol}精度失败,减仓操作取消")
                    return
                id = item['info']['data'][0]['ordId']
                fill_vol = float(item['info']['data'][0]['fillSz'])*face_value    # 张数 fillSz是本次成交量 accFillSz是累计成交量
                side = item['info']['data'][0]['side']
                fee = float(item['info']['data'][0]['fee'])
                reduceOnly = item['info']['data'][0]['reduceOnly']
                side_type = 1 if side == 'buy' else -1
                fill_vol *= side_type
                self.log.info(f"成交信息:{symbol} {id} {side} fee:{fee} 成交量:{fill_vol} reduceOnly:{reduceOnly}")
                
                if id in self.wss_deals:
                    self.wss_deals[id] += fill_vol  # TODO 是否需要带方向？暂定不带方向
                else:
                    self.wss_deals[id] = fill_vol
                    
                # 增加远端挂单成交后的处理
                temp_later_order = self.local_later_order
                if id in temp_later_order:
                    if self.wss_deals[id] >= temp_later_order[id]:
                        self.local_later_order[self.id][symbol].pop(id)
                    # 增加已对冲量
                    await self.local_save_hedge_pos(self.id, symbol, self.wss_deals[id])
                    
            
    # 接收订阅的外盘一档价格    测试成功
    async def on_hedge_message(self, channel: str, item: dict):
        # self.log.info('redis订阅信息:', channel, item)
        if 'bids_asks' in channel and item['messageType'] == 'message':
            symbol = channel.split('.')[2]
            self.symbols_bid_ask[symbol] = [item['bid'], item['ask']]
        # self.log.info(f"redis订阅一档价格:{self.symbols_bid_ask}")

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        self.wss_log.write(f"ws成交推送数据:{content}")
        symbol = self.symbol if self.symbol else True
        uid = self.uid
        con1 = True if symbol == True else content['market'] in symbol    # 交易对
        con2 = True if uid == True else uid == content['userId']   # 对手方uid
        self.log.info(f"条件判断:{self.tag}, {con1}, {symbol}, {content['market']}, {con2}, {content['userId']}, {uid}")
        if content['tag'] == self.tag and con1 and con2 and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.update_tag_user_pos(content)

    # 将wss推送数据转换成hold_list查询到的用户持仓数据格式
    async def update_tag_user_pos(self, content):
        # self.log.info(f"需要处理的数据:{content}")        
        symbol = content['market']
        # side = 1 if content['positionAmt'][0] == '+' else -1
        close_pos = False
        if 'positionAmt' not in content:
            # 是平仓单推送 TODO 根据之前的持仓,更新temp_pos的持仓
            close_pos = True
            amount = 0
        else:
            amount = float(content['positionAmt'])
            side = 1 if float(content['positionAmt']) > 0 else 2
            self.log.info(f"最新持仓+方向:{float(content['positionAmt'])}, {side}")
            is_full = 2 if content['marginType'] == 'full' else 1
        # None表示推送数据中没有相关内容
        if not close_pos:
            temp_pos = {'amount': abs(amount/self.symbol_size[symbol]), 'avgPrice': content['entryPrice'], 'freeze_amount': None, 
                        'in_position': None, 'isFull': is_full, 'is_full': is_full, 'mark_price': content['markPrice'],
                        'multiple': content['leverage'], 'openDirection': side, 'open_time': content['updateTime'], 
                        'parity': content['liquidationPrice'], 'profitLoss': content['unRealizedProfit'], 
                        'profit_loss': content['unRealizedProfit'], 'risk_ratio': None, 'symbol': symbol, 
                        'tag': content['tag'], 'time': content['updateTime'], 'user_id': str(self.id), 
                        'user_name': str(self.id), 'zhqy': None}
        else:
            close_symbol = symbol
            close_uid = str(self.id)
            temp_pos = {}   # TODO 这里跟前一版不同,关注风险
        update_tag = False
        delete_tag = False
        self.log.info(f"308数据验证:{self.hold_list}\n{temp_pos}")
        for i in self.hold_list:
            if not close_pos:
                if temp_pos['symbol'] == i['symbol'] and \
                    temp_pos['user_id'] == i['user_id'] and \
                    temp_pos['isFull'] == i['isFull'] and \
                    temp_pos['openDirection'] == i['openDirection']:    # 确定方向就没问题了
                    update_tag = True
                    index = self.hold_list.index(i)
                    break
            else:
                self.log.info(close_symbol, i['symbol'], close_uid, i['user_id'])
                if close_symbol == i['symbol'] and close_uid == i['user_id']:
                    delete_tag = True
                    index = self.hold_list.index(i)
                    break
        
        self.log.info(f"更新:{update_tag},删除:{delete_tag}")
        self.log.info(f"数据验证:{self.local_base_deals} {self.id} {type(self.id)}")
        if update_tag:
            self.hold_list[index] = temp_pos
            self.log.info(f"更新持仓:{temp_pos}")
            self.temp_update_data = [symbol, temp_pos, 'update']
            # self.temp_update_data.append([symbol, temp_pos, 'update'])
            # await self.update_deals(symbol, temp_pos, 'update')   # 这里更新会导致后续在走平仓判断逻辑时，应该平仓的不平仓
            # self.local_base_deals[self.id][symbol] = temp_pos['amount']
        elif delete_tag:
            self.hold_list.pop(index)
            self.log.info(f"删除持仓:{content}")
            self.temp_update_data = [symbol, temp_pos, 'delete']
            # self.temp_update_data.append([symbol, 0, 'delete'])
            # await self.update_deals(symbol, 0, 'delete')
            # self.local_base_deals[self.id][symbol] = 0
        else:
            self.hold_list.append(temp_pos)
            self.log.info(f"新增持仓:{temp_pos}")
            self.temp_update_data = [symbol, temp_pos, 'update']
            # self.temp_update_data.append([symbol, temp_pos, 'update'])
            # await self.update_deals(symbol, temp_pos, 'update')
            # self.local_base_deals[self.id][symbol] = temp_pos['amount']
        self.save_wss_data.append(content)  # 推送数据更新到self.hold_list后,保存到self.save_wss_data.这样不会保存没有更新的ws推送
    
    async def update_deals(self, symbol, pos_data, action):
        if action == 'update':
            # 由于可能双向持仓的原因,只能通过rest验证
            temp_pos = pos_data['amount'] if pos_data['openDirection'] == 1 else -pos_data['amount']
            hold_list = await self.get_hold_list()  # 获取当前指定用户持仓
            base_pos, _ = await self.get_hedge_dict(hold_list)
            try:
                update_pos = base_pos[symbol]
            except:
                update_pos = temp_pos
            self.log.info(f"本地化更新持仓:{symbol} 接口查到的:{update_pos} 传过来的:{temp_pos}")
        else:
            update_pos = 0
        if self.id in self.local_base_deals:
            self.local_base_deals[self.id][symbol] = update_pos
        else:
            self.local_base_deals[self.id] = {symbol: update_pos}
        self.local_base_deals = self._local_deals.save({'deals': self.local_base_deals})['deals']
        
    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                # 获取所有交易对
                res = await self.old_ws_rest.get_symbols(quan=True)
                for s in res:
                    self.symbol_size[s.symbol] = s.contract_size
                self.log.info(f"内盘合约单位:{self.symbol_size}")
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    # rest获取对冲端交易对详情
    async def hedge_contract_info(self):
        res = await self.okx_rest.fetch_precision()
        for s, v in res.items():
            price_precision = int(-np.log10(v['price']))    # 价格精度
            amount_precision = int(-np.log10(v['amount']))  # 数量精度
            face_value = v['faceValue']  # 合约面试,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']
            self.symbol_precision[s] = (price_precision, amount_precision, face_value, hedge_vol_limit)
        self.log.info(f"交易对精度+面值:{self.symbol_precision}")
    
    async def cancel_orders(self, symbol=None, ids=None):
        cancel_symbols = {}
        # 开盘撤单
        if symbol is None:
            res = await self.okx_rest.fetch_current_list(limit=100, state='live')
            for s in res:
                if s['symbol'] in cancel_symbols:
                    cancel_symbols[s['symbol']].append(s['id'])
                else:
                    cancel_symbols[s['symbol']] = [s['id']]
            for s, ids in cancel_symbols.items():
                res = await self.okx_rest.cancel_order_batch(s, order_id=ids)
                self.log.info(f"取消所有委托:{res}")
        # 用户平仓后,撤单委托
        else:
            res = await self.okx_rest.cancel_order_batch(symbol, order_id=ids)
            self.log.info(f"取消指定委托:{res}")
    
    async def update_pos_data(self, hold_list):
        while True:
            try:
                # 内盘持仓
                self.base_pos, _ = await self.get_hedge_dict(hold_list)   # 按筛选条件获取持仓数据

                # 本地化记录的已对冲订单
                self.hedge_pos = self.local_hedge_pos
                
                # 查询接口持仓
                temp_data = {}
                hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
                for v in hedge_pos:
                    if float(v['contracts']) != 0:
                        temp_data[v['symbol']] = float(v['contracts'])
                
                # 对比持仓
                if self.hedge_pos != temp_data:
                    self.log.warning(f"本地记录{self.hedge_pos}和对冲仓位{temp_data}对不上!!!立即检查!!!")
                    # tb.warning(f"{strategy_name} 本地记录{self.hedge_pos}和对冲仓位{temp_data}对不上!!!立即检查!!!",'risk')
                
                # 按照字典的value大小排序
                # self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
                # self.hedge_pos = dict(sorted(self.hedge_pos.items(), key=lambda item: item[1], reverse=True))
                self.log.info(f"\n初始内盘持仓:{self.base_pos}\n初始外盘持仓:{self.hedge_pos}")
                break
            except:
                self.log.warning(f"update_pos_data函数报错:{traceback.format_exc()}")
            await asyncio.sleep(3)
        
    async def send_msg(self, text):
        try:
            tb.warning(f"{strategy_name} 合约持仓异常:{text}",'risk')
            # tb.sendmail(f'{strategy_name} 合约持仓异常', text)
            self.log.info(text)
        except:
            self.log.warning(f"send_msg函数报错:{traceback.format_exc()}")
    
    # 获取精度
    async def get_precision(self, symbol):
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            price_precision, amount_precision, face_value, hedge_vol_limit = None, None, None, None
        return price_precision, amount_precision, face_value, hedge_vol_limit
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self, first_tag=False):
        self.log.info(f"风控线程")
        
        try:
            res = await self.okx_rest.fetch_balance()
            self.free = res['USDT']['free']      # 可用金额
                        
            used = res['USDT']['used']      # 冻结金额
            self.total = res['USDT']['total']    # 账户权益
            self.log.info(f"账户权益:{self.total} 可用:{self.free} 冻结:{used}")
            # 内盘持仓
            hold_list = await self.get_hold_list()  # 获取当前指定用户持仓
            self.log.info(f"最新持仓:{hold_list}")
            if first_tag:
                self.log.info("第一次执行")
                await self.update_pos_data(hold_list)
            self.hold_list = hold_list
            self.base_pos, _ = await self.get_hedge_dict(hold_list)   # 按筛选条件获取持仓数据
            
            hedge_pos_risk = {}
            # 获取本地已对冲量
            self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})
            for symbol, pos in self.local_hedge_pos.get(self.id, {}).items():
                if symbol in hedge_pos_risk:
                    hedge_pos_risk[symbol] += pos
                else:
                    hedge_pos_risk[symbol] = pos
            self.log.info(f"本地记录的对冲量:{hedge_pos_risk}")
            # 外盘持仓
            text = f"{strategy_name} 用户持仓:\n"
            sum_profit = 0
            hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
            for v in hedge_pos:
                if float(v['contracts']) != 0:
                    sum_profit += float(v['unrealizedPnl'])
                    text += f"{v['symbol']} 持仓:{v['contracts']} 浮动盈亏:{v['unrealizedPnl']} 成本:{v['entryPrice']} 当前价格:{v['markPrice']} 爆仓价:{v['info']['liqPx']}\n"
                    # 单币对浮亏超过1万u报警
            # 总体浮亏超过1万u报警
            if sum_profit < -10000 and time.time() - self.last_risk_ts > 60*10:
                self.last_risk_ts = time.time()
                mess = f"{strategy_name} 总浮动盈亏:{sum_profit} 关注持仓!!!\n{text}"
                await self.send_msg(mess)
                # await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=mess)
            
            # 按照字典的value大小排序
            self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
            hedge_pos_risk = dict(sorted(hedge_pos_risk.items(), key=lambda item: item[1], reverse=True))
            self.log.info(f"\n内盘持仓:{self.base_pos}\n外盘持仓:{hedge_pos_risk}")
            text += f"总浮动盈亏:{sum_profit}\n"
            self.log.info(text)
        except:
            self.log.warning(f"risk函数报错:{traceback.format_exc()}")
            
        # 计算净敞口
        # 内外盘仓位差报警,只有当所有maker对冲单全都下完之后,有仓位差别才算异常
        if self.hedge_order_id == {}:
            self.log.info(f"全部平仓,检查风控")
            if self.base_pos != self.last_base_pos:
                self.diff_num += 1
                if self.diff_num > 1:
                    await self.check_pos(hedge_pos_risk)
                    self.diff_num = 0
            else:
                self.diff_num = 0

            # 外盘持仓>内盘持仓,报警
            for s, p in hedge_pos_risk.items():
                if s in self.base_pos and abs(p) > abs(self.base_pos[s]):
                    text = f"{s}外盘仓位:{p} 内盘仓位:{self.base_pos[s]} 外盘仓位大于内盘仓位,立即查看!!!\n"
                    await self.send_msg(text)
        else:
            self.log.info(f"{self.hedge_order_id}非空,不检查风控")
        
        # 测试数据
        """
            开多  非真实下单部分通过测试,真实成交通过
            开空  非真实下单部分通过测试,真实成交通过
            加多  非真实下单部分通过测试,真实成交通过
            加空  非真实下单部分通过测试,真实成交通过
            减多(只平仓)    非真实下单部分通过测试,真实成交通过
            减多(全平+撤单) 非真实下单部分通过测试,真实成交通过
            减空(只平仓)    非真实下单部分通过测试,真实成交通过
            减空(全平+撤单) 非真实下单部分通过测试,真实成交通过
            平多            非真实下单部分通过测试,真实成交通过
            平空            非真实下单部分通过测试,真实成交通过
            开多加多减仓平仓  真实成交通过
            开空架空减仓平仓  真实成交通过
            分多次推送       通过
            追单时间判断是否正确   通过
            本地记录是否正确       暂时没有发现记录数据错误的情况
            双向持仓是否正确       推送成交没问题,判断内盘仓位做了优化,但不放心
            风控在对冲中不要报警   完成
            增加浮亏超过1w报警     完成
            
            验证外盘的wss推送是否正常   正常
            持仓方向记录是否正确        正常
        """
        # if first_tag:
        # if self.first:
        #     self.first = False
        #     adl_wss = [{'lastfilledVolume': 43.215, 'orderType': 'market', 'leverage': 100, 'lastfilledSize': 50, 'isolatedMargin': '9799.2138', 'liquidationPrice': '0.5', 'cumfilledSize': 50, 'uid': 100022, 'positionAmt': '-500', 'markPrice': '0.803', 'price': '', 'tag': 'Z2', 'direction': 'MANY', 'amount': 50, 'side': 'SELL', 'origQty': 50, 'positionSide': 'LONG', 'updateTime': 1752182307292, 'userId': 38838551, 'market': 'ADA-USDT', 'entryPrice': '0.803', 'cumfilledVol': '50', 'isAutoAddMargin': 'false', 'unRealizedProfit': '18088.3702', 'marginType': 'full', 'lastfilledprice': '0.803', 'status': 'FILLED'},
        #                {'lastfilledVolume': 51.858, 'orderType': 'market', 'leverage': 100, 'lastfilledSize': 60, 'isolatedMargin': '9799.2138', 'liquidationPrice': '0.5', 'cumfilledSize': 60, 'uid': 100022, 'positionAmt': '-3000', 'markPrice': '0.803', 'price': '', 'tag': 'Z2', 'direction': 'MANY', 'amount': 60, 'side': 'SELL', 'origQty': 50, 'positionSide': 'LONG', 'updateTime': 1752182307292, 'userId': 38838551, 'market': 'ADA-USDT', 'entryPrice': '0.803', 'cumfilledVol': '60', 'isAutoAddMargin': 'false', 'unRealizedProfit': '18088.3702', 'marginType': 'full', 'lastfilledprice': '0.803', 'status': 'FILLED'},
        #                {'lastfilledVolume': 51.858, 'orderType': 'market', 'leverage': 100, 'lastfilledSize': 90, 'isolatedMargin': '9799.2138', 'liquidationPrice': '0.5', 'cumfilledSize': 90, 'uid': 100022, 'positionAmt': '-800', 'markPrice': '0.803', 'price': '', 'tag': 'Z2', 'direction': 'MANY', 'amount': 90, 'side': 'BUY', 'origQty': 90, 'positionSide': 'LONG', 'updateTime': 1752182307292, 'userId': 38838551, 'market': 'ADA-USDT', 'entryPrice': '0.803', 'cumfilledVol': '90', 'isAutoAddMargin': 'false', 'unRealizedProfit': '18088.3702', 'marginType': 'full', 'lastfilledprice': '0.803', 'status': 'FILLED'},
        #                {'lastfilledVolume': 95.073, 'orderType': 'market', 'lastfilledSize': 20, 'amount': 800, 'side': 'BUY', 'origQty': 20, 'cumfilledSize': 20, 'userId': 38838551, 'market': 'ADA-USDT', 'uid': 100022, 'cumfilledVol': '80', 'price': '', 'lastfilledprice': '0.803', 'tag': 'Z2', 'status': 'FILLED', 'direction': 'MANY'}
        #             ]
        #     for i in adl_wss:
        #         await self.on_adl(i) # 测试开仓
        #         await asyncio.sleep(10)
        #     await self.on_ok_deal() # 测试成交
    
    async def check_pos(self, hedge_pos_risk):
        for k, v in self.base_pos.items():
            if k in hedge_pos_risk:
                self.log.info(f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v}")
                # 按对冲比例还原,若超过则报警
                hedge_pos_amount = abs(hedge_pos_risk[k])/self.hedge_ratio
                if hedge_pos_amount > abs(v):
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位大于内盘仓位,立即查看!!!\n"
                    await self.send_msg(text)
                elif hedge_pos_amount < abs(v):
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位小于内盘仓位,立即查看!!!\n"
                    await self.send_msg(text)
        for k, v in hedge_pos_risk.items():
            if k not in self.base_pos:
                text = (f"{k}外盘仓位:{v} 内盘仓位:0 立即平仓")
                await self.send_msg(text)
        self.hedge_pos_risk = hedge_pos_risk
        self.last_base_pos = self.base_pos
    
    ''' ==========================================================================='''
    ''' ================================= function ================================'''
    ''' ==========================================================================='''
    # 发送报警
    async def send_warning(self, title, warning_mess):
        try:
            self.log.warning(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            self.log.warning(f"send_warning发送报警信息失败:{traceback.format_exc()}")
            
    # 按照twap计算对冲次数
    async def twap(self, hedge_vol, each_vol):
        max_num = self.total_time/self.each_interval_time
        # 计算预期交易笔数 tradenumber = Q/ordersize
        vol = abs(hedge_vol)
        trade_num = vol//each_vol-1
        if trade_num > max_num:
            trade_num = max_num
            each_vol = vol/trade_num
        if vol%each_vol != 0:
            trade_num = vol//each_vol+1     # 交易笔数
            residue_vol = vol%each_vol
        else:
            trade_num = vol//each_vol
            residue_vol = 0
        return int(trade_num), each_vol, residue_vol
    
    # 计算下单价格+量
    async def get_price_vol(self, symbol, side, i, trade_num, each_vol, residue_vol, price_precision):
        vol = each_vol
        if i == trade_num:
            vol = residue_vol
        side_type = 1 if side == 'buy' else -1
        vol *= side_type
        
        bid_one, ask_one = await self.get_price(symbol)
        bid_price = round(bid_one+1/10**price_precision, price_precision)
        ask_price = round(ask_one-1/10**price_precision, price_precision) 
        price = min(bid_price, ask_price) if side == 'buy' else max(bid_price, ask_price)
        return price, vol

    # 过滤最小下单量
    async def check_min_vol(self, symbol, vol, face_value, hedge_vol_limit, mess):
        fave_vol_limit = face_value*hedge_vol_limit    # 最小交易币数
        unhedge_pos = self.local_unhedge_pos.get(self.id, {}).get(symbol, 0)
        temp_vol = vol+unhedge_pos
        if abs(temp_vol) < fave_vol_limit:
            self.log.info(f"{mess} {vol}加上未对冲量{temp_vol}不足最小交易币数:{fave_vol_limit}")
            await self.update_unhedge_pos(symbol, temp_vol)
            return True, temp_vol
        else:
            await self.update_unhedge_pos(symbol, 0)
            self.log.info(f"{mess} {vol}加上未对冲量{temp_vol}满足最小交易币数:{fave_vol_limit}")
        return False, temp_vol

    # 更新未对冲仓位
    async def update_unhedge_pos(self, symbol, vol):
        if self.id in self.local_unhedge_pos:
            self.local_unhedge_pos[self.id][symbol] = vol
        else:
            self.local_unhedge_pos[self.id] = {symbol: vol}
        self.local_unhedge_pos = self._local_deals.save({'unhedge_pos': self.local_unhedge_pos})['unhedge_pos']        

    # 根据外盘最小下单量优化
    async def count_deal_vol(self, side, vol,face_value, hedge_vol_limit):
        side_type = 1 if side == 'buy' else -1
        unit = int(-math.log10(face_value*hedge_vol_limit))
        vol = round(abs(vol), unit)
        vol *= side_type
        return vol
        # each_vol = round(abs(each_vol), unit)
        # each_vol *= side_type
        # residue_vol = round(abs(residue_vol), unit)
        # residue_vol *= side_type
        # if trade_num == 1:
        #     reduce_pos = residue_vol
        # else:
        #     reduce_pos = (trade_num-1)*each_vol+residue_vol    # 本次对冲量
        # return reduce_pos
    
    # 执行下单
    async def make_order(self, **kwarge):
        symbol = kwarge['symbol']
        order_type = kwarge['order_type']
        side = kwarge['side']
        vol = kwarge['vol']
        price = kwarge['price']
        type_mess = kwarge['type_mess']     # 下单原因
        reduce_only = kwarge.get('reduce_only', False)  # 如果对冲账户有多个用户的同一交易对持仓,则不能用这个参数,否则会持续报错
        save_local = kwarge.get('save_local', False)
        update_ts = kwarge.get('update_ts', False)
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        vol = await self.count_deal_vol(side, vol, face_value, hedge_vol_limit)
        self.log.info(f"make_order参数:{symbol}, {order_type}, {price}, {vol}, {side}, {type_mess}, 只减仓:{reduce_only}, 本地化:{save_local}, 继续maker下单:{update_ts}")
        
        # 更新本地化外盘对冲仓位
        if '操作' in type_mess:
            await self.local_save_hedge_pos(self.id, symbol, vol)
        try:
            t1 = time.time()*1000
            # result = {'id': 123, 'timestamp': int(time.time()*1000)}
            result = await self.okx_rest.create_order(symbol=symbol, order_type=order_type,
                                                side=side, amount=abs(vol), #reduceOnly=reduce_only,
                                                price=price, tdMode='cross')
            # {'symbol': 'ETH-USDT', 'result': True, 'id': '2695260760319123456', 'clientOrderId': '', 'timestamp': 1752827430097, 'cost_time': 0.056985564064234495, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2695260760319123456', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1752827430097'}], 'inTime': '1752827430097119', 'msg': '', 'outTime': '1752827430099047'}}
            # # 无仓位可平会报错
            # {'result': False, 'cost_time': 0.0566100999712944, 'info': {'code': '1', 'data': 
            #     [{'clOrdId': '', 'ordId': '', 'sCode': '51169', 'sMsg': "Order failed because you don't have any positions in this direction for this contract to reduce or close. ", 
            #       'tag': '', 'ts': '1752827358095'}], 'inTime': '1752827358095210', 
            #       'msg': 'All operations failed', 'outTime': '1752827358096434'}}
            deal_mess = f"{datetime.datetime.now()} {type_mess} 对冲下单 {symbol} 对冲价格:{price} 对冲数量:{abs(vol)} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
            self.log.info(f"下单回报:{deal_mess}")
            self.deal_log.write(deal_mess)
            if result['result']:
                if save_local:
                    # TODO 保存时vol是否加abs()
                    if update_ts:
                        self.hedge_order_id[update_ts] = [result['id'], symbol, vol, side, reduce_only, result['timestamp']]
                    else:
                        self.hedge_order_id[result['timestamp']] = [result['id'], symbol, vol, side, reduce_only, result['timestamp']]
            else:
                if save_local:
                    ts = int(time.time()*1000)
                    if update_ts:
                        # maker追单失败走这里
                        self.hedge_order_id[update_ts] = [None, symbol, vol, side, reduce_only, ts]
                    else:
                        # 第一次对冲就失败走这里
                        self.hedge_order_id[ts] = [None, symbol, vol, side, reduce_only, ts]
        except:
            title = f"{strategy_name} 下单失败"
            warning_mess = f"{strategy_name} {type_mess} 对冲下单 {symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
            await self.send_warning(title, warning_mess)
            self.log.error(f"{warning_mess}")
        return result

    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''
    # 检查maker单未成交量,并重新挂maker单
    async def check_deal(self):
        hedge_orders = copy.deepcopy(self.hedge_order_id)
        self.log.info(f"检查当前maker单:{hedge_orders}")
        pop_list = []
        for t, d in hedge_orders.items():
            # 先判断若超过1min则taker
            self.log.info(f"hedge_orders本地化数据:{d}")
            now_ts = int(time.time()*1000)
            deal_vol = self.wss_deals.get(d[0], 0)  # 统计已成交量
            symbol = d[1]                           # 交易对
            vol = d[2] - deal_vol                   # 计算未成交量
            side = d[3]                             # 下单方向
            reduce_only = d[4]                      # 只减仓
            self.log.info(f"{symbol} 订单号:{d[0]} 已成交量:{deal_vol} 未成交量:{vol} 方向:{side} 只减仓:{reduce_only}")
            price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
            if price_precision is None and amount_precision is None:
                self.log.error(f"获取{symbol}精度失败,减仓操作取消")
                return
            tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, 'taker对冲量')
            if tag:
                pop_list.append(t)
                continue
            bid_one, ask_one = await self.get_price(symbol=symbol)
            if now_ts - t > 60000:
                if d[0] is not None:
                    res = await self.okx_rest.cancel_order(symbol, order_id=d[0])  # 先撤单
                    self.log.info(f"maker未成交撤单1:{res}")
                    # 下单报错,处理可能因时间差已经成交的数量。因为self.wss_deals.get(d[0])是总成交量,所以直接减去是不对的
                    # if not res['result'] and res['info']['data'][0]['sCode'] == '51400':
                    #     vol -= self.wss_deals.get(d[0], 0)
                    if not res['result']:
                        res = await self.okx_rest.fetch_order_detail(symbol, order_id=d[0])
                        self.log.info(f"撤单后查订单成交数量:{res}")
                        if res['status'] == 'closed':
                            deal_vol = res['amount']
                            deal_side = res['side']
                            if deal_side == 'buy':
                                vol -= deal_vol
                            else:
                                vol += deal_vol
                price = round(ask_one*(1+self.slip), price_precision) if side == 'buy' else \
                        round(bid_one*(1-self.slip), price_precision)
                tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, 'taker对冲量')
                if tag:
                    continue
                # 直接taker
                res = await self.make_order(symbol=symbol, order_type='limit', side=side, vol=vol, price=price, 
                                      type_mess='maker单超过1min未成交,taker对冲下单', reduce_only=reduce_only)
                # 追单成功才删除本地化,失败则继续保留
                if res['result']:
                    pop_list.append(t)
            else:    # 未超过1min,但超过3s,则撤单后继续挂maker单
                # 已完全成交,从字典中删除
                # self.log.info(f"当前时间戳:{now_ts} 订单创建时间:{d[-1]} hedge_vol_limit:{hedge_vol_limit}")
                if now_ts - t > 3000:
                    if d[0] is not None:
                        res = await self.okx_rest.cancel_order(symbol, order_id=d[0])  # 先撤单
                        self.log.info(f"maker未成交撤单2:{res}")
                        # 下单报错,处理可能因时间差已经成交的数量
                        # if not res['result'] and res['info']['data'][0]['sCode'] == '51400':
                        #     vol -= self.wss_deals.get(d[0], 0)
                        if not res['result']:
                            res = await self.okx_rest.fetch_order_detail(symbol, order_id=d[0])
                            self.log.info(f"撤单后查订单成交数量:{res}")
                            if res['status'] == 'closed':
                                deal_vol = res['amount']
                                deal_side = res['side']
                                if deal_side == 'buy':
                                    vol -= deal_vol
                                else:
                                    vol += deal_vol
                    bid_price = round(bid_one+1/10**price_precision, price_precision)
                    ask_price = round(ask_one-1/10**price_precision, price_precision) 
                    price = min(bid_price, ask_price) if side == 'buy' else \
                            max(bid_price, ask_price)
                    tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, 'maker对冲量')
                    if tag:
                        pop_list.append(t)
                        continue
                    # 下maker单
                    await self.make_order(symbol=symbol, order_type='post_only', side=side, vol=vol, 
                                          price=price, type_mess='maker单未成交,继续maker下单',
                                          reduce_only=reduce_only, save_local=True, update_ts=t)
        # 删除不会再对冲的数据
        if pop_list:
            for t in pop_list:
                del self.hedge_order_id[t]
    
    async def get_hold_list(self):
        while 1:
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.old_ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id=self.id)
                hold_list_page = data['data']['pager']['total_page']
                temp_hold_list += data['data']['data']
                for n in range(2, hold_list_page+1):
                    data = await self.old_ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20, user_id=self.id)
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(1)
                break
            except:
                self.log.warning(f"获取持仓列表失败:{traceback.format_exc()}")
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        self.log.info(f"当前用户持仓:{hold_list}")
        return hold_list
    
    # 按筛选条件获取持仓数据
    async def get_hedge_dict(self, hold_list):
        temp_hedge_dict = {}        # 内盘标记用户持仓
        temp_open_parity_price = {} # 保存交易对开仓均价和爆仓价
        for i in hold_list:
            if i == {}:
                continue
            id = int(i['user_id'])
            tag = i['tag']
            symbol = i['symbol']
            if id == self.id and tag == self.tag:
                # self.log.info(f"用户持仓情况:{i}")
                symbol = i['symbol']
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                side_type = 1 if i['openDirection'] == 1 else -1
                open_price = float(i['avgPrice'])   # 开仓均价
                amount = float(i['amount'])*self.symbol_size[symbol]*side_type  # 持仓数量,币的数量
                self.log.info(f"570行数据验证:{float(i['amount'])}, {self.symbol_size[symbol]}, {side_type}, {amount}")
                try:
                    parity = float(i['parity'])         # 强平价
                except:
                    parity = 0
                # 用来记录开仓均价和爆仓价
                if symbol in temp_open_parity_price:
                    if side in temp_open_parity_price[symbol]:
                        sum_amount = temp_open_parity_price[symbol][side][0]
                        ave_price = temp_open_parity_price[symbol][side][1]
                        open_price = (open_price*amount+sum_amount*ave_price)/(amount+sum_amount)
                        parity = max(parity, temp_open_parity_price[symbol][side][2]) if side == 'buy' \
                            else min(parity, temp_open_parity_price[symbol][side][2])
                        temp_open_parity_price[symbol][side][0] = sum_amount+amount
                        temp_open_parity_price[symbol][side][1] = open_price
                        temp_open_parity_price[symbol][side][2] = parity
                    else:
                        # [持仓数量, 开仓均价, 爆仓价]
                        temp_open_parity_price[symbol][side] = [amount, open_price, parity]
                else:
                    temp_open_parity_price[symbol] = {side: [amount, open_price, parity]}
                # 统计当前内盘标记用户持仓
                if symbol not in temp_hedge_dict:
                    temp_hedge_dict[symbol] = amount
                else:
                    temp_hedge_dict[symbol] += amount
        mess = (f"内盘标记用户当前持仓:\n")
        for s, v in temp_hedge_dict.items():
            mess += (f"{s}持仓:{v}\n")
        if mess != self.last_base_pos_mess:
            self.last_base_pos_mess = mess
            self.log.info(mess)
        return temp_hedge_dict, temp_open_parity_price
    
    # 判断持仓是否有变化,以及需要对冲和撤单的数据
    async def get_hedge_info(self, temp_hedge_dict):
        self.log.info(f"temp_hedge_dict持仓:{temp_hedge_dict}")
        hedge_info = {'open':{}, 'close':{}}
        # 开仓、加减仓情况
        # 说明: 因为内盘没有真实持仓,所以temp_hedge_dict没有uni的-10持仓;
        # 如果temp_hedge_dict有数据,self.base_pos再后面也会根据temp_hedge_dict更新,从而不会出现问题
        for s, v in temp_hedge_dict.items():
            last_pos = self.base_pos.get(s, 0)
            self.log.info(f"last_pos持仓:{last_pos}")
            if v == last_pos:
                # self.log.info(f"{s}持仓已对冲过")
                pass
            elif last_pos == 0:
                self.log.info(f"{s}新增持仓:{v}")
                hedge_info['open'][s] = v
            elif v > last_pos and last_pos > 0:
                self.log.info(f"{s}多单持仓增加:{v-last_pos}")
                hedge_info['open'][s] = (v-last_pos)
            elif v < last_pos and last_pos > 0:
                self.log.info(f"{s}多单持仓减少:{last_pos-v}")
                hedge_info['close'][s] = -(last_pos-v)
            elif v > last_pos and last_pos < 0:
                self.log.info(f"{s}空单持仓减少:{v-last_pos}")
                hedge_info['close'][s] = (v-last_pos)
            elif v < last_pos and last_pos < 0:
                self.log.info(f"{s}空单持仓增加:{last_pos-v}")
                hedge_info['open'][s] = -(last_pos-v)
        # 平仓情况
        self.log.info(f"过往持仓self.base_pos:{self.base_pos}")
        for s, v in self.base_pos.items():
            now_pos = temp_hedge_dict.get(s, 0)
            if now_pos == 0:
                self.log.info(f"{s}持仓已清空")
                hedge_info['close'][s] = -v
        mess = f"需要对冲的持仓:\n"
        for k, v in hedge_info['open'].items():
            mess += (f"开仓信息:{k}对冲:{v}\n")
        for k, v in hedge_info['close'].items():
            mess += (f"平仓信息:{k}对冲:{v}\n")
        self.log.info(mess)
        self.base_pos = temp_hedge_dict     # 判断完加减仓后,再更新内盘持仓
        return hedge_info
    
    # 获取交易对外盘最新价格
    async def get_price(self, symbol):
        while True:
            try:
                bid_one, ask_one = self.symbols_bid_ask[symbol][0], self.symbols_bid_ask[symbol][1]
                return bid_one, ask_one
            except:
                self.log.error(f"获取{symbol}最新价格失败{traceback.format_exc()}")
                await self.sub_bid_ask(symbol)
            await asyncio.sleep(0.5)
    
    # 平仓对冲
    async def cancel_close_orders(self, symbol, pos, this_handle_wss_data):
        # self.log.info(f"cancel_close_orders传入参数:{pos}")
        """
            本地记录总对冲量(即使正在挂maker单没有成交)
            减仓操作优先平已对冲持仓
            都平了还在减仓,则把远端挂单撤掉
        """
        # 先判断本次减仓占已开仓总量的比例
        base_pos = self.local_base_deals.get(self.id, {}).get(symbol, 0)
        if base_pos == 0:
            mess = f"{symbol}在本地记录已无持仓数据,但触发减仓{pos},查看持仓和记录情况"
            self.log.warning(mess)
            return
        reduce_ratio = round(abs(pos/base_pos), 3)   # 减仓占比
        
        # pos = pos*self.hedge_ratio
        hedge_pos = self.local_hedge_pos.get(self.id, {}).get(symbol, 0)
        reduce_pos = hedge_pos*reduce_ratio
        side = 'buy' if pos > 0 else 'sell'
        reduce_pos = abs(reduce_pos) if side == 'buy' else abs(reduce_pos)*-1
        self.log.info(f"{symbol}内盘用户持仓量{base_pos} 减仓量{pos} 方向{side} 减仓占比:{reduce_ratio*100}% 已对冲数量{hedge_pos} 本次减仓量{reduce_pos}")
        # TODO 验证pos和hedge_pos的是否带方向
        # if abs(pos) < abs(hedge_pos):
        if reduce_ratio < 1:
            # maker减仓
            # 更新交易对精度
            price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
            if price_precision is None and amount_precision is None:
                self.log.error(f"获取{symbol}精度失败,人工查看")
                await self.send_msg(f"获取{symbol}精度失败,人工查看")
                return
            
            # TWAP拆单
            if time.time() - self.each_vol_check_ts > 120 or symbol not in self.each_vol_dict:
                self.each_vol = await self.count_vol(symbol)
                self.each_vol_check_ts = time.time()
            each_vol = self.each_vol_dict[symbol]
            trade_num, each_vol, residue_vol = await self.twap(reduce_pos, each_vol)
            
            # reduce_pos = await self.count_deal_vol(side, face_value, hedge_vol_limit, trade_num, each_vol, residue_vol)
            
            # 更新本地记录的内盘用户持仓
            await self.update_deals(self.temp_update_data[0],self.temp_update_data[1],self.temp_update_data[2])
            # 更新本地化外盘对冲仓位
            # await self.local_save_hedge_pos(self.id, symbol, reduce_pos)   # TODO pos还是要带方向才行
            # self.log.info(f"909存hedge_pos数据验证:{self.local_hedge_pos} {reduce_pos}")
            # 对冲
            for i in range(1, trade_num+1):
                # 计算下单价格+量
                price, vol = await self.get_price_vol(symbol, side, i, trade_num, each_vol, residue_vol, price_precision)
                tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, '减仓量')
                if tag:
                    continue
                # 下maker单
                await self.make_order(symbol=symbol, order_type='post_only', side=side, vol=vol, 
                                      price=price, type_mess=f'数据来源:{this_handle_wss_data} 减仓操作',
                                      reduce_only=True, save_local=True)
                await asyncio.sleep(self.each_interval_time)
            reduce_ratio = abs(pos*price/self.max_follow_amt)
            self.real_hedge_ratio -= reduce_ratio
            self.log.info(f"减仓总和占比:{self.real_hedge_ratio}")
        else:
            # 平仓数量大于已对冲数量,则全部平仓外,远端订单也全部撤掉
            if self.id not in self.local_later_order or symbol not in self.local_later_order[self.id]:
                self.log.info(f"本地没有{self.id}的{symbol}持仓数据")    # 没有远端挂单也正常
                # await self.send_msg(f"本地没有{self.id}的{symbol}持仓数据")
            else:
                cancel_orders = list(self.local_later_order[self.id][symbol].keys())
                await self.cancel_orders(symbol, cancel_orders)
                # 更新远端挂单本地化数据
                self.local_later_order = self._local_deals.save({'later_order': {}})['later_order']
            
            # 对冲仓位全部平仓
            price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
            if price_precision is None and amount_precision is None:
                self.log.error(f"获取{symbol}精度失败,人工查看")
                await self.send_msg(f"获取{symbol}精度失败,人工查看")
                return
            
            # 处理减仓方向问题
            reduce_pos = abs(reduce_pos) if side == 'buy' else reduce_pos*-1
            self.log.info(f"{symbol}减仓量{reduce_pos} 方向{side}")
            
            # TWAP拆单
            if time.time() - self.each_vol_check_ts > 120 or symbol not in self.each_vol_dict:
                self.each_vol = await self.count_vol(symbol)
                self.each_vol_check_ts = time.time()
            each_vol = self.each_vol_dict[symbol]
            trade_num, each_vol, residue_vol = await self.twap(reduce_pos, each_vol)
            
            # reduce_pos = await self.count_deal_vol(side, face_value, hedge_vol_limit, trade_num, each_vol, residue_vol)
            
            # 更新本地记录的内盘用户持仓
            await self.update_deals(self.temp_update_data[0],self.temp_update_data[1],self.temp_update_data[2])
            # 更新本地化外盘对冲仓位
            # self.local_hedge_pos[self.id][symbol] = 0
            # self.local_hedge_pos = self._local_deals.save({'hedge_pos': self.local_hedge_pos})['hedge_pos']
            # 对冲
            for i in range(1, trade_num+1):
                # 计算下单价格+量
                price, vol = await self.get_price_vol(symbol, side, i, trade_num, each_vol, residue_vol, price_precision)
                tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, '平仓量')
                if tag:
                    continue
                # 下maker单
                await self.make_order(symbol=symbol, order_type='post_only', side=side, vol=vol, 
                                      price=price, type_mess=f'数据来源:{this_handle_wss_data} 平仓操作',
                                      reduce_only=True, save_local=True)
            self.log.info(f"平仓总和占比:{self.real_hedge_ratio}")
            self.real_hedge_ratio = 0
                
    async def main(self):
        self.log.info("执行main主函数")
        # 获取当前所有用户持仓
        hold_list = self.hold_list   # 根据ws推送的内盘标记用户成交数据去对冲
        this_handle_wss_data = self.save_wss_data   # 将本次需要处理的wss数据保存为局部变量,不要直接动用全局变量,防止在处理的过程中,on_adl在继续更新全局变量
        self.save_wss_data = []      # 清空
        # 按筛选条件获取持仓数据
        temp_hedge_dict, temp_open_parity_price = await self.get_hedge_dict(hold_list)
        # 判断持仓是否有变化,以及需要对冲和撤单的数据
        hedge_info = await self.get_hedge_info(temp_hedge_dict)
        self.log.info(f"需要操作的仓位:{hedge_info}")
        # 聚合wss推送数据
        sum_vol = 0
        sum_amt = 0
        
        self.log.info(f"1080行数据验证:{this_handle_wss_data}")
        try:
            if this_handle_wss_data:
                for i in this_handle_wss_data:
                    symbol = i['market']
                    sum_vol += i['amount']
                    sum_amt += i['lastfilledVolume']
                    side = i['side']
                    createTime = i.get('updateTime', int(time.time()*1000))
                    pos = i.get('positionAmt', 0)
                price = round(sum_amt/sum_vol, 4)
                this_handle_wss_data = {'amrket':symbol, 'price': price, 'amount':round(sum_vol, 4), 
                                        'volume': round(sum_amt, 4), 'side':side, 
                                        'createTime':createTime, 'pos':pos}
        except:
            self.log.warning(f"1080行数据报错:{traceback.format_exc()}")
        # {'orderType': 'market', 'side': 'SELL', 'amount': 0.67, 'traderId': 61857352, 
        #  'userId': '18648059', 'market': 'ETH-USDT', 'volume': '2124.462', 'createTime': 1752673368, 
        #  'price': '3170.84', 'id': '1752673368496241', 'status': 'FILLED'}
        
        # 平仓对冲
        for k, v in hedge_info.get('close', {}).items():
            try:
                await self.cancel_close_orders(k, v, this_handle_wss_data)
            except:
                self.log.warning(f"cancel_close_orders函数报错:{traceback.format_exc()}")
            
        # 开仓对冲
        for k, v in hedge_info.get('open', {}).items():
            symbol = k
            side = 'buy' if v > 0 else 'sell'
            
            self.log.info(f"对冲前的当前持仓:{temp_open_parity_price}")
            open_price = temp_open_parity_price[symbol][side][1]
            parity = temp_open_parity_price[symbol][side][2]
            self.log.info(f"{k}对冲方向:{side} 开仓均价:{open_price} 强平价:{parity}")

            # 计算远端价格挂在哪里
            liquidity_ratio = 1-parity/open_price if side == 'buy' else parity/open_price-1
            hedge_price_interval = self.price_ratio if liquidity_ratio >= self.price_ratio \
                                    else round(liquidity_ratio, 3)
            hedge_price_percent = hedge_price_interval*self.hedge_price_ratio    # 第二次对冲价格距离现在价格的百分比
            later_hedge_price = open_price*(1-hedge_price_percent) if side == 'buy' else \
                                open_price*(1+hedge_price_percent)
            self.log.info(f"爆仓价:{parity} 远端对冲价格:{later_hedge_price}")
            
            # 计算对冲价格+对冲量
            now_hedge_price = open_price    # TODO 到底是什么价格？带方向的内盘成交均价,理论上结果是不带方向的
            now_hedge_vol = v * (1-self.later_ratio)
            later_hedge_vol = v * self.later_ratio
            # 最终对冲量来自于:对冲比例0.67,maker对冲比例0.7  hedge_vol = deal_vol * 0.67 * 0.7
            self.log.info(f"实时对冲价格:{now_hedge_price} 量:{now_hedge_vol} 远端对冲价格:{later_hedge_price} 量:{later_hedge_vol}")
            
            # 更新本地记录的内盘用户持仓
            await self.update_deals(self.temp_update_data[0],self.temp_update_data[1],self.temp_update_data[2])
            # 去对冲
            # await self.later_hedge(symbol, later_hedge_price, later_hedge_vol, side, this_handle_wss_data)
            await self.now_hedge(symbol, now_hedge_price, now_hedge_vol, side, this_handle_wss_data)
    
    async def later_hedge(self, symbol, later_hedge_price, later_hedge_vol, side, this_handle_wss_data):
        # 更新交易对精度
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        if price_precision is None and amount_precision is None:
            self.log.error(f"获取{symbol}精度失败,人工查看")
            await self.send_msg(f"获取{symbol}精度失败,人工查看")
            return
        
        price = round(later_hedge_price, price_precision)
        if 'e' in str(price):
            price = f'{price:.10f}'
        vol = round(later_hedge_vol, amount_precision)
        tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, '远端挂单量')
        if tag:
            return
        # 挂远单
        result = await self.make_order(symbol=symbol, order_type='limit', side=side, vol=vol, 
                                       price=price, type_mess=f'数据来源:{this_handle_wss_data} 远端挂单操作')
        if self.id in self.local_later_order:
            if symbol in self.local_later_order[self.id]:
                self.local_later_order[self.id][symbol][result['id']] = vol
            else:
                self.local_later_order[self.id][symbol] = {str(result['id']): vol}
        else:
            if result['result']:
                self.local_later_order[self.id] = {symbol: {str(result['id']): vol}}
            else:
                self.log.warning(f"远端挂单失败:{result}")
        self.local_later_order = self._local_deals.save({'later_order': self.local_later_order})['later_order']
        
    # 对冲
    async def now_hedge(self, symbol, hedge_price, hedge_vol, side, this_handle_wss_data, tag='开仓'):
        # 更新交易对精度
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        if price_precision is None and amount_precision is None:
            self.log.error(f"获取{symbol}精度失败,人工查看")
            await self.send_msg(f"获取{symbol}精度失败,人工查看")
            return

        # 处理对冲量
        hedge_pos_risk = {}
        if self.id in self.local_hedge_pos: # TODO 哪里做的本地化？我的理解是在fetch订单后更新
            for loc_symbol, pos in self.local_hedge_pos[self.id].items():
                # 因为可能双向持仓,才这样写
                if loc_symbol in hedge_pos_risk:
                    hedge_pos_risk[loc_symbol] += pos
                else:
                    hedge_pos_risk[loc_symbol] = pos
        real_hedge_vol = hedge_pos_risk[symbol] if symbol in hedge_pos_risk else 0    # 这个持仓是根据uid对应的本地维护持仓+查询未完成订单的成交结果加总后的值
        self.log.info(f"1085数据验证:{self.local_hedge_pos} ===== {real_hedge_vol}")
        
        side_type = 1 if side == 'buy' else -1
        base_ratio = abs(hedge_vol*hedge_price/self.max_follow_amt)
        self.log.info(f"1089数据验证:hedge_vol:{hedge_vol} hedge_price:{hedge_price} 最大对冲:{self.max_follow_amt} 基准成交占比:{base_ratio} 总和占比:{self.real_hedge_ratio}")
        if (1-self.real_hedge_ratio) > base_ratio:
            self.real_hedge_ratio += base_ratio
        else:
            if self.real_hedge_ratio < 1:
                base_ratio = 1-self.real_hedge_ratio
                self.real_hedge_ratio = 1
            else:
                self.real_hedge_ratio = 1
                base_ratio = 0
        self.log.info(f"1093数据验证:{hedge_vol} 成交占比:{base_ratio} 总和占比:{self.real_hedge_ratio}")
        temp_vol = hedge_vol*self.hedge_ratio     # 按照对冲比例调整对冲量
        # 获取对冲资金和计划最大可用对冲资金做比较
        use_total = self.free   #*self.use_balance_ratio   # 总可用对冲资金
        # use_balance = min(getattr(self, 'hedge_balance_limit', use_total), use_total) # 可用对冲资金
        use_balance = use_total*self.level       # 可用资金*杠杆倍数
        hedge_amt = abs(real_hedge_vol*hedge_price)         # 已对冲金额
        this_use_usdt = use_balance*base_ratio
        this_max_hedge_amt = use_balance - hedge_amt
        use_usdt = min(this_use_usdt, this_max_hedge_amt, self.max_follow_amt)
        self.log.info(f"总对冲资金:{use_balance} 已对冲金额:{hedge_amt} 本次按比例需要对冲金额:{this_use_usdt} 抛去已对冲金额后可用最大对冲金额:{this_max_hedge_amt} 最终本次对冲金额:{use_usdt}")
        temp_vol2 = use_usdt/hedge_price   # 本次可对冲量
        vol = min(abs(temp_vol), abs(temp_vol2)) if abs(temp_vol2) > 0 else 0
        vol = vol*side_type
        self.log.info(f"{symbol} 对冲量:初始按比例对冲量:{hedge_vol} 按资金调整后:{temp_vol2} 最终对冲量:{vol}") 
        
        # TWAP拆单
        if time.time() - self.each_vol_check_ts > 120 or symbol not in self.each_vol_dict:
            self.each_vol = await self.count_vol(symbol)
            self.each_vol_check_ts = time.time()
        each_vol = self.each_vol_dict[symbol]
        trade_num, each_vol, residue_vol = await self.twap(vol, each_vol)
        
        # vol = await self.count_deal_vol(side, face_value, hedge_vol_limit, trade_num, each_vol, residue_vol)
        
        # 更新本地化外盘对冲仓位
        # await self.local_save_hedge_pos(self.id, symbol, vol)
        # self.log.info(f"1113存hedge_pos数据验证:{self.local_hedge_pos} {vol}")
        # 对冲
        self.log.info(f"maker总对冲量:{vol} 挂单次数:{trade_num} 单次对冲量:{each_vol} 剩余量:{residue_vol}")
        for i in range(1, trade_num+1):
            # 计算下单价格+量
            price, vol = await self.get_price_vol(symbol, side, i, trade_num, each_vol, residue_vol, price_precision)
            tag, vol = await self.check_min_vol(symbol, vol, face_value, hedge_vol_limit, '开仓量')
            if tag:
                continue
            # 下maker单
            await self.make_order(symbol=symbol, order_type='post_only', side=side, vol=vol, 
                                  price=price, type_mess=f'数据来源:{this_handle_wss_data} 开仓操作',
                                  save_local=True)
            await asyncio.sleep(self.each_interval_time)
            
    # 将已对冲仓位本地化
    async def local_save_hedge_pos(self, uid, symbol, vol):
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos} {uid} {symbol} {vol}")
        if uid in self.local_hedge_pos:
            if symbol in self.local_hedge_pos[uid]:
                self.local_hedge_pos[uid][symbol] += vol
            else:
                self.local_hedge_pos[uid][symbol] = vol
        else:
            self.local_hedge_pos[uid] = {symbol: vol}
        
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        unit = int(-math.log10(face_value*hedge_vol_limit))
        vol = round(self.local_hedge_pos[uid][symbol], unit)
        self.local_hedge_pos[uid][symbol] = vol
        
        self.local_hedge_pos = self._local_deals.save({'hedge_pos': self.local_hedge_pos})['hedge_pos']
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos}")
        

def main(id, symbol, path='grid_hedge_252552_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(id, symbol, config).run()



if __name__ == '__main__':
    id = sys.argv[1]
    try:
        symbol = sys.argv[2]
    except:
        symbol = None
    main(id, symbol)


