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

    TODO 检查self.hedge_pos_ratio参数使用位置
    
    hold_list查出来的持仓是张数
    on_adl推送来的positionAmt和amount是币的数量
    
    测试用例:
    1、开多  OK
    2、开空  OK
    3、开多+加多  OK
    4、开空+加空  OK
    5、开多+平仓  OK
    6、开多+减仓  OK
    7、开空+平仓  OK
    8、开空+减仓  OK
    9、本地已有多单,加多
    10、本地已有空单,加空  OK
    11、本地已有多单,平仓
    12、本地已有多单,减仓
    13、本地已有空单,平仓  
    14、本地已有空单,减仓  OK
    
    TODO 验证self.local_hedge_pos更新和查询订单状态之间的更新关系
    # TODO 查询到订单状态是canceled之后如何更新本地数据的?
    TODO WSS接外盘权益推送数据
    TODO 增加固定止损,根据用户真实杠杆和行情波动设置
'''

import sys
import time
import datetime
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
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest

strategy_name = "grid_hedge_v3网格对冲策略"


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
        
        # 订阅base数据
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl    # 标记用户回调函数
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rc_task = rc.RestClient()
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
        
    
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_grid_deals.log')
        self.local_base_deals = self._local_deals.load().get('deals', {})    # 保存所有成交聚合数据
        self.local_hedge_orderid = self._local_deals.load().get('hedge_orderid', {})  # 保存所有对冲挂单id
        # 初始化更新一次,cancel_close_orders函数验证减仓时更新一次
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})   # 保存所有对冲持仓
    
    async def on_first(self):
        self.log.add(f"log/{self.id}_{self.symbol}_log.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        
        channel_pos = f"contract.balance.okex"
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_ok_balance))
        self.log.info("subscribe: okex权益")

        await self.get_symbol_unit()        # 内盘合约单位
        await self.hedge_contract_info()    # 更新币对信息
        await self.cancel_orders()          # 取消所有委托
        await self.risk(True)               # 判断是否对冲了计划内的仓位

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/3"))  # 每3s执行一次
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
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    # 接收ok账户权益推送
    async def on_ok_balance(self, channel: str, item: dict):
        # self.log.info(f"ok账户权益推送:{channel}  {item}")
        # {'exchange': 'okex', 'balance': {'USDT': 
        #     {'currency': 'USDT', 'free': None, 'used': None, 'total': 98122.13542560421}}, 
        # 'timestamp': 1750781390086, 'messageType': 'message', 'info': {'arg': {'channel': 
        #     'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': 
        #         [{'cashBal': '98122.1354256042159649', 'ccy': 'USDT', 'uTime': '1750781390086'}], 
        #         'eventType': 'filled', 'pTime': '1750781390086', 'posData': 
        #             [{'avgPx': '2436.22', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 
        #             'instType': 'SWAP', 'mgnMode': 'cross', 'nonSettleAvgPx': '', 'pos': '-821.31', 
        #             'posCcy': '', 'posId': '2550208086072352768', 'posSide': 'net', 'quoteBal': '', 
        #             'settledPnl': '', 'tradeId': '2193281083', 'uTime': '1750781390086'}], 'trades': 
        #                 [{'instId': 'ETH-USDT-SWAP', 'tradeId': '2193281083'}]}]}}
        if channel == "contract.balance.okex":
            self.total = item['balance']['USDT']['total']
            self.log.info(f"wss推送的ok账户权益:{self.total}")
        
    # 接收订阅的外盘一档价格    测试成功
    async def on_hedge_message(self, channel: str, item: dict):
        # self.log.info('redis订阅信息:', channel, item)
        if 'bids_asks' in channel and item['messageType'] == 'message':
            symbol = channel.split('.')[2]
            self.symbols_bid_ask[symbol] = [item['bid'], item['ask']]
        # self.log.info(f"redis订阅一档价格:{self.symbols_bid_ask}")

    # 内盘wss成交推送
    async def on_adl(self, content):
        # self.log.info(f"ws成交推送数据:{content}")
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
        # 推送数据
        {'lastfilledVolume': 2.97, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 0.01, 
         'isolatedMargin': '0.0606', 'liquidationPrice': '435085.92', 'cumfilledSize': 0.01, 'uid': 100022, 
         'positionAmt': '-0.01', 'markPrice': '296.80', 'price': '296.96', 'tag': 'Z2', 'direction': 'SPACE', 
         'amount': 0.01, 'side': 'SELL', 'origQty': 0.01, 'positionSide': 'SHORT', 
         'updateTime': 1740552793258, 'userId': 76332266, 'market': 'UNI-USDT', 'entryPrice': '297.01', 
         'cumfilledVol': '2.97', 'isAutoAddMargin': 'false', 'unRealizedProfit': '0.0021', 
         'marginType': 'full', 'lastfilledprice': '297.01', 'status': 'FILLED'}
        {'lastfilledVolume': 11568.33, 'orderType': 'limit', 'lastfilledSize': 38.91, 'amount': 38.91, 
         'side': 'SELL', 'origQty': 60.74, 'cumfilledSize': 60.74, 'userId': 31611693, 'market': 'UNI-USDT', 
         'uid': 100022, 'cumfilledVol': '18058.82', 'price': '297.25', 'lastfilledprice': '297.31', 
         'tag': 'Z2', 'status': 'FILLED', 'direction': 'MANY'}
        # 保存的数据格式
        {'amount': '4', 'avgPrice': '2526.42', 'freeze_amount': '0', 'in_position': '1.22', 'isFull': 1, 
         'is_full': 1, 'mark_price': '2526.27', 'multiple': 100, 'openDirection': 1, 'open_time': 1747726735, 
         'parity': '1707.34', 'profitLoss': '-0.0060', 'profit_loss': '-0.0060', 'risk_ratio': '1.22', 
         'symbol': 'ETH-USDT', 'tag': '', 'time': 1747726735, 'user_id': '485111', 'user_name': '485111', 
         'zhqy': '33.0301'}
        
        symbol = content['market']
        # side = 1 if content['positionAmt'][0] == '+' else -1
        close_pos = False
        if 'positionAmt' not in content:
            # 是平仓单推送 TODO 根据之前的持仓,更新temp_pos的持仓
            close_pos = True
            amount = 0
        else:
            amount = float(content['positionAmt'])
            side = 1 if float(content['positionAmt']) > 0 else -1
            self.log.info('最新持仓+方向:', float(content['positionAmt']), side)
            is_full = 2 if content['marginType'] == 'full' else 1
        # None表示推送数据中没有相关内容
        if not close_pos:
            temp_pos = {'amount': abs(amount/self.symbol_size[symbol]), 'avgPrice': content['entryPrice'], 'freeze_amount': None, 
                        'in_position': None, 'isFull': is_full, 'is_full': is_full, 'mark_price': content['markPrice'],
                        'multiple': content['leverage'], 'openDirection': side, 'open_time': content['updateTime'], 
                        'parity': content['liquidationPrice'], 'profitLoss': content['unRealizedProfit'], 
                        'profit_loss': content['unRealizedProfit'], 'risk_ratio': None, 'symbol': content['market'], 
                        'tag': content['tag'], 'time': content['updateTime'], 'user_id': str(self.id), 
                        'user_name': str(self.id), 'zhqy': None}
        else:
            close_symbol = content['market']
            close_uid = str(self.id)
        update_tag = False
        delete_tag = False
        for i in self.hold_list:
            if not close_pos:
                if temp_pos['symbol'] == i['symbol'] and \
                    temp_pos['user_id'] == i['user_id'] and \
                    temp_pos['isFull'] == i['isFull']:
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
            self.temp_update_data.append([symbol, temp_pos, 'update'])
            # await self.update_deals(symbol, temp_pos, 'update')
            # self.local_base_deals[self.id][symbol] = temp_pos['amount']
        elif delete_tag:
            self.hold_list.pop(index)
            self.log.info(f"删除持仓:{content}")
            self.temp_update_data.append([symbol, 0, 'delete'])
            # await self.update_deals(symbol, 0, 'delete')
            # self.local_base_deals[self.id][symbol] = 0
        else:
            self.hold_list.append(temp_pos)
            self.log.info(f"新增持仓:{temp_pos}")
            self.temp_update_data.append([symbol, temp_pos, 'update'])
            # await self.update_deals(symbol, temp_pos, 'update')
            # self.local_base_deals[self.id][symbol] = temp_pos['amount']
        self.save_wss_data.append(content)  # 推送数据更新到self.hold_list后,保存到self.save_wss_data.这样不会保存没有更新的ws推送
    
    async def update_deals(self, symbol, pos_data, action):
        if action == 'update':
            update_pos = pos_data['amount']
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
                res = await self.ws_rest.get_symbols(quan=True)
                for s in res:
                    self.symbol_size[s.symbol] = s.contract_size
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
                # 查询从本地获取的委托订单+本地记录的已对冲量
                if self.id not in self.local_hedge_orderid and self.id not in self.local_hedge_pos:
                    self.base_pos = {}
                    self.hedge_pos = {}
                    break
                # 已对冲订单
                self.hedge_pos = self.local_hedge_pos
                # 委托订单
                orders = self.local_hedge_orderid[self.id]
                for symbol, orders in orders.items():
                    filled_vol = 0  # 整体已成交量
                    for i in orders:
                        res = await self.okx_rest.fetch_order_detail(symbol=symbol, order_id=i)
                        self.log.info(f"初始化查询{symbol}当前委托:{res}")
                        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
                        if price_precision is None and amount_precision is None:
                            self.log.error(f"获取{symbol}精度失败,减仓操作取消")
                            continue
                        fill_vol = res['filled']*face_value if res['side'] == 'buy' else \
                                res['filled']*face_value*-1  # 成交量*币面值
                        filled_vol += fill_vol      # 全部委托的已成交量
                        await asyncio.sleep(0.2)
                    uid = self.id
                    if uid in self.hedge_pos:
                        if symbol in self.hedge_pos[uid]:
                            self.hedge_pos[uid][symbol] += filled_vol
                        else:
                            self.hedge_pos[uid][symbol] = filled_vol
                    else:
                        self.hedge_pos[uid] = {symbol: filled_vol}  # 已对冲量
                # 本地化已对冲数据
                self.local_hedge_pos = self.hedge_pos
                self.local_hedge_pos = self._local_deals.save({'hedge_pos': self.local_hedge_pos})['hedge_pos']
                # 按照字典的value大小排序
                self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
                self.hedge_pos = dict(sorted(self.hedge_pos[self.id].items(), key=lambda item: item[1], reverse=True))
                self.log.info(f"\n初始内盘持仓:{self.base_pos}\n初始外盘持仓:{self.hedge_pos}")
                break
            except:
                self.log.warning(f"update_pos_data函数报错:{traceback.format_exc()}")
            await asyncio.sleep(3)
        
    def send_msg(self, text):
        try:
            tb.warning(f"{strategy_name} 合约持仓异常:{text}",'risk')
            tb.sendmail(f'{strategy_name} 合约持仓异常', text)
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
        # self.log.info(f"风控线程")
        
        try:
            res = await self.okx_rest.fetch_balance()
            self.free = res['USDT']['free']      # 可用金额
            used = res['USDT']['used']      # 冻结金额
            self.total = res['USDT']['total']    # 账户权益
            self.log.info(f"账户权益:{self.total} 可用:{self.free} 冻结:{used}")
            # mess = {'lastfilledVolume': 41928.37, 'orderType': 'market', 'leverage': 100, 'lastfilledSize': 17.33, 'isolatedMargin': '4994.1048', 'liquidationPrice': '1746.78', 'cumfilledSize': 41.33, 'uid': 100022, 'positionAmt': '+206.42', 'markPrice': '2419.73', 'price': '', 'tag': 'Z2', 'direction': 'MANY', 'amount': 17.33, 'side': 'BUY', 'origQty': 41.33, 'positionSide': 'LONG', 'updateTime': 1750845160241, 'userId': 38838551, 'market': 'ETH-USDT', 'entryPrice': '2422.08', 'cumfilledVol': '99994.21', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-485.087', 'marginType': 'full', 'lastfilledprice': '2419.41', 'status': 'FILLED'}
            # await self.on_adl(mess)
            # return
            # 内盘持仓
            hold_list = await self.get_hold_list()  # 获取当前指定用户持仓
            if first_tag:
                self.log.info("第一次执行")
                await self.update_pos_data(hold_list)
            self.hold_list = hold_list
            self.base_pos, _ = await self.get_hedge_dict(hold_list)   # 按筛选条件获取持仓数据
            # 外盘委托
            hedge_pos_risk = {}
            if self.id in self.local_hedge_orderid:
                orders = self.local_hedge_orderid[self.id]
                # 获取本地已对冲量
                self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})
                for symbol, pos in self.local_hedge_pos[self.id].items():
                    if symbol in hedge_pos_risk:
                        hedge_pos_risk[symbol] += pos
                    else:
                        hedge_pos_risk[symbol] = pos
                self.log.info(f"本地记录的对冲量:{hedge_pos_risk}")
                # 查询当前委托订单是否成交
                for symbol, orders in orders.items():
                    pop_orders = []
                    for i in orders:
                        res = await self.okx_rest.fetch_order_detail(symbol=symbol, order_id=i)
                        self.log.info(f"risk查询{symbol}当前委托:{res}")
                        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
                        if price_precision is None and amount_precision is None:
                            self.log.error(f"获取{symbol}精度失败,减仓操作取消")
                            continue
                        fill_vol = res['filled']*face_value if res['side'] == 'buy' else \
                                res['filled']*face_value*-1  # 成交量*币面值
                        if symbol in hedge_pos_risk:
                            hedge_pos_risk[symbol] += fill_vol
                        else:
                            hedge_pos_risk[symbol] = fill_vol
                        # 要从本地删除的数据
                        if res['status'] == 'canceled':
                            pop_orders.append(i)
                        await asyncio.sleep(0.2)
                    # 更新订单,并本地化
                    new_orders = [i for i in orders if i not in pop_orders]
                    self.local_hedge_orderid[self.id][symbol] = new_orders
                    self.local_hedge_orderid = self._local_deals.save({'hedge_orderid': self.local_hedge_orderid})['hedge_orderid']
                    self.log.info(f"471本地化对冲订单:{self.local_hedge_orderid}")
            # 外盘持仓
            text = f"{strategy_name} 用户持仓:\n"
            sum_profit = 0
            hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
            for v in hedge_pos:
                if float(v['contracts']) != 0:
                    sum_profit += float(v['unrealizedPnl'])
                    text += f"{v['symbol']} 持仓:{v['contracts']} 浮动盈亏:{v['unrealizedPnl']} 成本:{v['entryPrice']} 当前价格:{v['markPrice']}\n"
            
            
            # 按照字典的value大小排序
            self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
            hedge_pos_risk = dict(sorted(hedge_pos_risk.items(), key=lambda item: item[1], reverse=True))
            self.log.info(f"\n内盘持仓:{self.base_pos}\n外盘持仓:{hedge_pos_risk}")
            text += f"总浮动盈亏:{sum_profit}\n"
            self.log.info(text)
            if time.time()-self.send_tg_ts > 1800:
                # await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=text)
                self.send_tg_ts = int(time.time())
        except:
            self.log.warning(f"risk函数报错:{traceback.format_exc()}")
            
        # 计算净敞口
        # 内外盘仓位差报警
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
                self.send_msg(text)
    
    async def check_pos(self, hedge_pos_risk):
        for k, v in self.base_pos.items():
            if k in hedge_pos_risk:
                self.log.info(f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v}")
                # 按对冲比例还原,若超过则报警
                hedge_pos_amount = abs(hedge_pos_risk[k])/self.hedge_pos_ratio
                if hedge_pos_amount/self.hedge_ratio > abs(v):
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位大于内盘仓位,立即查看!!!\n"
                    self.send_msg(text)
                elif hedge_pos_amount/self.hedge_ratio < abs(v):
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位小于内盘仓位,立即查看!!!\n"
                    self.send_msg(text)
        for k, v in hedge_pos_risk.items():
            if k not in self.base_pos:
                text = (f"{k}外盘仓位:{v} 内盘仓位:0 立即平仓")
                self.send_msg(text)
        self.hedge_pos_risk = hedge_pos_risk
        self.last_base_pos = self.base_pos

    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''

    async def get_hold_list(self):
        while 1:
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id=self.id)
                hold_list_page = data['data']['pager']['total_page']
                temp_hold_list += data['data']['data']
                for n in range(2, hold_list_page+1):
                    data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20, user_id=self.id)
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(1)
                break
            except:
                pass
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
            id = int(i['user_id'])
            tag = i['tag']
            symbol = i['symbol']
            if id == self.id and tag == self.tag:
                # self.log.info(f"用户持仓情况:{i}")
                symbol = i['symbol']
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                side_type = 1 if i['openDirection'] == 1 else -1
                open_price = float(i['avgPrice'])   # 开仓均价
                # TODO 验证这里*side_type去掉后，是否正常？ UPDATE可能跟mike策略有关
                amount = float(i['amount'])*self.symbol_size[symbol]*side_type  # 持仓数量
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
        # TODO 需要实盘验证!!!!!!!
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
    async def get_price(self, symbol, side):
        while True:
            try:
                price = self.symbols_bid_ask[symbol][1] if side == 'buy' else self.symbols_bid_ask[symbol][0]
                return price
            except:
                self.log.error(f"获取{symbol}最新价格失败{traceback.format_exc()}")
                await self.sub_bid_ask(symbol)
            await asyncio.sleep(0.5)
    
    async def cancel_close_orders(self, symbol, pos, this_handle_wss_data):
        self.log.info(f"cancel_close_orders传入参数:{pos}")
        orders = self.local_hedge_orderid[self.id][symbol]
        current_symbols = []    # 统计未成交+部分成交挂单
        filled_vol = 0          # 整体已成交量
        pop_orders = []         # 已完全成交委托,需要在本地化中删除
        price_precision, amount_precision, face_value, hedge_vol_limit = await self.get_precision(symbol)
        if price_precision is None and amount_precision is None:
            self.log.error(f"获取{symbol}精度失败,减仓操作取消")
            return
        for i in orders:
            res = await self.okx_rest.fetch_order_detail(symbol=symbol, order_id=i)
            self.log.info(f"查询当前委托:{res}")
            # 成交
            {'symbol': 'ETH-USDT', 'id': '2601689192009031681', 'clientOrderId': '', 'price': 2541.82, 
             'stopPrice': None, 'triggerPrice': None, 'amount': 0.01, 'amountIsNum': False, 'side': 'sell', 
             'type': 'limit', 'status': 'closed', 'leverage': 10.0, 'timeInForce': None, 'postOnly': None, 
             'reduceOnly': False, 'marginMode': 'crossed', 'average': 2546.76, 'filled': 0.1, 'cost': None, 
             'remaining': 0.0, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': 
             {'feeCurrency': 'USDT', 'cost': -0.00764028}, 'timestamp': 1750038780053, 
             'cost_time': 0.05978026601951569}
            # 未成交
            {'symbol': 'ETH-USDT', 'id': '2602051029414174720', 'clientOrderId': '', 'price': 100.0, 
             'stopPrice': None, 'triggerPrice': None, 'amount': 0.01, 'amountIsNum': False, 'side': 'buy', 
             'type': 'limit', 'status': 'canceled', 'leverage': 10.0, 'timeInForce': None, 'postOnly': None, 
             'reduceOnly': False, 'marginMode': 'crossed', 'average': None, 'filled': 0.0, 'cost': None, 
             'remaining': 0.1, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': 
             {'feeCurrency': 'USDT', 'cost': 0.0}, 'timestamp': 1750049570091, 'cost_time': 0.05620937200728804}
            
            deal_vol = res['amount'] if res['side'] == 'buy' else \
                       res['amount']*-1  # 币的数量
            fill_vol = res['filled']*face_value if res['side'] == 'buy' else \
                       res['filled']*face_value*-1  # 成交量*币面值
            filled_vol += fill_vol      # 全部委托的已成交量
            # 统计完全未成交挂单
            if fill_vol == 0:
                current_symbols.append([res['price'], deal_vol, i])
            # 统计部分成交挂单
            elif abs(fill_vol) < deal_vol:
                current_symbols.append([res['price'], deal_vol-fill_vol, i])
            # 完全成交
            else:
                pop_orders.append(i)
            
        side = 'buy' if pos > 0 else 'sell'
        hedge_plan_vol = 0  # 统计撤单委托量
        add_order = []  # 若多减仓,则增加一笔计划委托
        if side == 'buy':
            # 按价格降序排列
            local_hedge_orderid = sorted(current_symbols, key=lambda x: x[0])
        else:
            # 按价格升序排列
            local_hedge_orderid = sorted(current_symbols, key=lambda x: x[0], reverse=True)
        self.log.info(f"cancel_close_orders当前委托排序后:{local_hedge_orderid}")
        
        for i in local_hedge_orderid:
            hedge_plan_vol += i[1]/self.hedge_ratio
            pop_orders.append(i[2])
            self.log.info(f"hedge_plan_vol:{hedge_plan_vol} pos:{pos}")
            if abs(hedge_plan_vol) > abs(pos):
                # 若多减仓,则增加一笔计划挂单
                price = i[0]
                vol = hedge_plan_vol+pos
                add_side = 'buy' if side == 'sell' else 'sell'
                add_order = [price, vol, add_side]  # TODO UPDATE其他策略修改
                break
        # 撤单
        if pop_orders:
            self.log.info(f"{symbol}撤销委托:{pop_orders}")
            res = await self.okx_rest.cancel_order_batch(symbol=symbol, order_id=pop_orders)
            self.log.info(f"{symbol}撤单结果:{res}")
        
        # 删除已对冲的订单
        if len(pop_orders) > 0:
            new_orders = [i for i in orders if i not in pop_orders]
            self.local_hedge_orderid[self.id][symbol] = new_orders
            await self.local_save('hedge_orderid')
        # 更新已对冲量的本地化数据
        await self.local_save_hedge_pos(self.id, symbol, filled_vol)
        
        # 挂单
        if add_order:
            self.log.info(f"{symbol}增加计划委托:{add_order}")
            price = add_order[0]
            vol = add_order[1]*self.hedge_ratio
            side = add_order[2]
            # TODO 增加可用金额验证
            use_total = self.free   #*self.use_balance_ratio   # 总可用对冲资金
            use_balance = min(getattr(self, 'hedge_balance_limit', use_total), use_total) # 可用对冲资金
            temp_vol = use_balance/price   # 本次可对冲量
            vol = min(vol, temp_vol)
            # 测试数据
            # vol = 100/price
            try:
                t1 = time.time()*1000
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                       side=side, amount=abs(vol), reduceOnly=True,
                                                       price=price, tdMode='cross')
                deal_mess = f"{datetime.datetime.now()} 数据来源:{this_handle_wss_data} 754行cancel_close_orders函数 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
                self.deal_log.write(deal_mess)
            except:
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} 754行cancel_close_orders函数 {symbol}对冲价格:{price} 对冲数量:{vol} 方向:{side} 增加计划委托失败:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                self.log.error(f"{warning_mess}, 数据来源:{this_handle_wss_data}")
            if 'id' in result:
                await self.local_save_hedge_orders(symbol, [result['id']])  # 新增挂单本地化
            else:
                self.log.error(f"{symbol}对冲价格:{price} 对冲数量:{vol} 方向:{side} 增加委托失败:{result}")
            vol = vol if side == 'buy' else -vol
            # 更新本地记录的内盘用户持仓
            for i in self.temp_update_data:
                await self.update_deals(i[0],i[1],i[2])
            await self.local_save_hedge_pos(self.id, symbol, vol)
            notice = f"{strategy_name}发生对冲"
            tb.warning(notice, 'notice')

        # 若撤单数量不足减仓数量,继续从已对冲仓位平仓
        diff_pos = abs(pos) - abs(hedge_plan_vol)   # diff_pos是按用户成交量来衡量的,pos是用户的减仓量,若用户减仓量 > 已撤单量,还需要继续撤已对冲数量
        diff_pos *= self.hedge_ratio     # 按照对冲比例减仓
        if diff_pos > 0:
            self.log.info(f"触发减仓操作:{symbol} 减仓量{diff_pos} 用户减仓量{pos} 已对冲量{hedge_plan_vol}")

            # 验证记录是否正确,有小概率本地统计会多
            self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})
            local_hedge_pos = self.local_hedge_pos[self.id][symbol]
            if diff_pos > abs(local_hedge_pos):
                mess = f"{self.id}用户{symbol} 本地记录持仓:{local_hedge_pos} 需要减仓:{diff_pos} 账对不齐,立即检查!"
                self.log.warning(mess)
                try:
                    tb.warning(mess,'risk')
                except:
                    pass
            
            # 若本身就对冲不完整，则按照已对冲比例减仓 TODO 这里要重点测试  self.id格式
            self.log.info(f"数据验证:{self.local_base_deals} {self.id} {type(self.id)}")
            local_pos = self.local_base_deals[self.id][symbol]*self.symbol_size[symbol]
            ratio = abs(pos/local_pos) if local_pos != 0 else 1
            diff_pos = local_hedge_pos if ratio >= 1 else local_hedge_pos*ratio   # 本地已对冲数量*成交比例
            
            # 减仓
            depth_price = await self.get_price(symbol, side)
            price = depth_price*(1-self.slip) if side == 'sell' else \
                    depth_price*(1+self.slip)
            price = round(price, price_precision)
            vol = round(abs(diff_pos), amount_precision)
            # 测试数据
            # vol = 100/price
            try:
                t1 = time.time()*1000
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                       side=side, amount=vol, reduceOnly=True,
                                                       price=price, tdMode='cross')
                deal_mess = f"{datetime.datetime.now()} 数据来源:{this_handle_wss_data} 806行cancel_close_orders函数 交易对:{symbol} 对冲价格:{price} 对冲数量:{vol}  方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
                self.deal_log.write(deal_mess)
            except:
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} 806行cancel_close_orders函数 {symbol}对冲价格:{price} 对冲数量:{vol} 方向:{side} 下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                self.log.error(f"{warning_mess}, 数据来源:{this_handle_wss_data}")
            
            # 更新本地记录的内盘用户持仓
            for i in self.temp_update_data:
                await self.update_deals(i[0],i[1],i[2])
                
            vol = vol if side == 'buy' else -vol
            await self.local_save_hedge_pos(self.id, symbol, vol)
            notice = f"{strategy_name}发生对冲"
            tb.warning(notice, 'notice')
        
                
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
            vol = v*self.hedge_pos_ratio
            # if abs(vol)*temp_open_parity_price[symbol][side][1] < self.hedge_amt_limit:
            #     self.log.info(f"{k}对冲数量:{vol} 小于对冲限制:{self.hedge_amt_limit} 暂时不对冲")
            #     continue
            
            self.log.info('对冲前的当前持仓:', temp_open_parity_price)
            open_price = temp_open_parity_price[symbol][side][1]
            parity = temp_open_parity_price[symbol][side][2]
            self.log.info(f"{k}对冲方向:{side} 开仓均价:{open_price} 强平价:{parity}")

            # 价格波动多少百分比会爆仓
            liquidity_ratio = 1-parity/open_price if side == 'buy' else parity/open_price-1
            hedge_price_interval = self.price_ratio if liquidity_ratio >= self.price_ratio \
                                    else round(liquidity_ratio, 3)
            hedge_step_percent = hedge_price_interval*self.hedge_price_ratio/self.hedge_step  # 每档对冲价格比例
            self.log.info(f"价格波动多少百分比会爆仓:{liquidity_ratio} 最大对冲价格比例:{hedge_price_interval} 每档对冲价格比例:{hedge_step_percent}")    

            # 根据斐波那契额数列对冲
            def generate_fibonacci(n):
                fib = [0, 1]
                for i in range(2, n):
                    fib.append(fib[-1] + fib[-2])
                return fib[:n]
            fbnq = generate_fibonacci(self.hedge_step+1)[1:]   # 生成step位斐波那契数列
            fenshu = []
            fbnq_sum = sum(fbnq)
            for i in fbnq:
                fenshu.append(round(i/fbnq_sum, 4))
            self.log.info(f"斐波那契额数列:{fbnq}  权重:{fenshu}")
            
            hedge_price_list = []
            hedge_vol_list = []
            for n in range(len(fenshu)):
                hedge_price = open_price*(1-hedge_step_percent*n) if side == 'buy' \
                        else open_price*(1+hedge_step_percent*n)
                hedge_vol = vol*fenshu[n]
                hedge_price_list.append(hedge_price)
                hedge_vol_list.append(hedge_vol)
            self.log.info(f"对冲价格:{hedge_price_list} 对冲量:{hedge_vol_list}")

            # 去对冲
            await self.hedge(symbol, hedge_price_list, hedge_vol_list, side, this_handle_wss_data)
                
        
    # 对冲
    async def hedge(self, symbol, price_list, vol_list, side, this_handle_wss_data, tag='开仓'):
        # 更新交易对精度
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            mess = f"{strategy_name} {symbol}在ok没有获取到交易对信息,无法对冲,立即人工介入"
            self.log.warning(mess)
            # tb.warning(mess,'risk')
            # tb.sendmail('ok对冲失败,无法获取交易对信息', mess)
            await self.send_msg(mess)
            return
        # 处理可能多对冲的情况
        hedge_vol = sum([round(v, amount_precision) for v in vol_list])
        # real_hedge_vol = self.hedge_pos_risk.get(symbol, 0)
        
        hedge_pos_risk = {}
        if self.id in self.local_hedge_pos: # TODO 哪里做的本地化？我的理解是在fetch订单后更新
            for symbol, pos in self.local_hedge_pos[self.id].items():
                if symbol in hedge_pos_risk:
                    hedge_pos_risk[symbol] += pos
                else:
                    hedge_pos_risk[symbol] = pos
        # 查询当前委托订单是否成交
        if self.id in self.local_hedge_orderid:
            for symbol, orders in self.local_hedge_orderid[self.id].items():
                pop_orders = []
                for i in orders:
                    res = await self.okx_rest.fetch_order_detail(symbol=symbol, order_id=i)
                    # TODO 发现订单全部成交,要更新本地化存储的对冲order_id
                    self.log.info(f"hedge查询{symbol}当前委托:{res}")
                    fill_vol = res['filled']*face_value if res['side'] == 'buy' else \
                            res['filled']*face_value*-1  # 成交量*币面值
                    if symbol in hedge_pos_risk:
                        hedge_pos_risk[symbol] += fill_vol
                    else:
                        hedge_pos_risk[symbol] = fill_vol
                    # 要从本地删除的数据
                    if res['status'] == 'canceled':
                        pop_orders.append(i)
                    await asyncio.sleep(0.2)
                # 更新订单,并本地化
                new_orders = [i for i in orders if i not in pop_orders]
                self.local_hedge_orderid[self.id][symbol] = new_orders
                self.local_hedge_orderid = self._local_deals.save({'hedge_orderid': self.local_hedge_orderid})['hedge_orderid']
                self.log.info(f"944本地化对冲订单:{self.local_hedge_orderid}")
        real_hedge_vol = hedge_pos_risk[symbol] if symbol in hedge_pos_risk else 0    # 这个持仓是根据uid对应的本地维护持仓+查询未完成订单的成交结果加总后的值
                
        sum_vol = hedge_vol + real_hedge_vol    # 已对冲仓位+本次对冲仓位
        base_vol = self.base_pos.get(symbol, 0) # 内盘持仓
        diff_vol = abs(sum_vol) - abs(base_vol) # 内外盘持仓差
        self.log.info(f"判断仓位差 diff_vol:{diff_vol} sum_vol:{sum_vol} base_vol:{base_vol} vol_list:{vol_list}")
        # TODO UPDATE可能需要改另一个对冲策略的地方
        if diff_vol > 0:
            if vol_list[-1] > diff_vol:
                vol_list[-1] -= diff_vol
            else:
               self.log.info(f"已对冲仓位:{real_hedge_vol} 本次对冲仓位:{hedge_vol} 外盘委托+对冲仓位:{sum_vol} 内盘持仓:{base_vol} 本次对冲仓位过大,已跳过,立即查看原因!!!")
               return
        
        hedge_id_list = []
        hedgeMess = f"交易对:{symbol} 方向:{side} 类型:{tag}\n"
        for i in range(len(price_list)):
            price = round(price_list[i], price_precision)
            if 'e' in str(price):
                price = f'{price:.10f}'
            # vol = round(vol_list[i], amount_precision) # 因为四舍五入有可能多对冲
            vol = vol_list[i]
            
            # 处理对冲量
            temp_vol = vol*self.hedge_ratio     # 按照对冲比例调整对冲量
            # 获取对冲资金和计划最大可用对冲资金做比较
            use_total = self.free   #*self.use_balance_ratio   # 总可用对冲资金
            # use_balance = min(getattr(self, 'hedge_balance_limit', use_total), use_total) # 可用对冲资金
            use_balance = use_total*self.level      # 临时兼容
            hedge_amt = real_hedge_vol*price        # 已对冲金额
            temp_vol2 = (use_balance - hedge_amt)/price   # 本次可对冲量
            self.log.info(f"{symbol} 对冲量:初始{vol} 按照对冲比例调整后:{temp_vol} 按资金调整后:{temp_vol2}")
            
            vol = min(abs(temp_vol), abs(temp_vol2)) if abs(temp_vol2) > 0 else 0
            # 根据max_hedge_amt参数调整下单量
            
            
            # TODO UPDATE可能跟mike策略有关 TODO 这里写法肯定不对
            if abs(vol) < hedge_vol_limit or abs(vol) < face_value*hedge_vol_limit:   # 小于一张合约面值也会下单失败
                self.log.info(f"{symbol}对冲数量:{vol} 小于对冲限制:{hedge_vol_limit} 或者小于合约面值:{face_value*hedge_vol_limit} 跳过对冲")
                continue
            hedgeMess += f"对冲价量:{price}  {vol}\n"
            # 测试数据
            # vol = 100/price
            try:
                t1 = time.time()*1000
                # self.log.info(this_handle_wss_data, type(this_handle_wss_data))
                # uid = this_handle_wss_data[0]['userId']
                # begin_ts = str(this_handle_wss_data[0]['updateTime'])
                # end_ts = str(this_handle_wss_data[-1]['updateTime'])
                # self.order_num += 1
                # client_id = f"{self.id}A{begin_ts}B{end_ts[-5:]}C{self.order_num}"
                # self.log.info(f"自定义ID:{client_id} {len(client_id)}")
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                    side=side, amount=abs(vol), 
                                                    price=price, tdMode='cross')
                hedge_id_list.append(result['id'])
                deal_mess = f"{datetime.datetime.now()} 数据来源:{this_handle_wss_data} hedge函数 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{abs(vol)} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
                self.deal_log.write(deal_mess)
            except:
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} hedge函数 {symbol}对冲价格:{price} 对冲数量:{vol} 方向:{side} 增加计划委托失败:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                self.log.error(f"{warning_mess} 数据来源:{this_handle_wss_data}")

        # 更新本地记录的内盘用户持仓
        for i in self.temp_update_data:
            await self.update_deals(i[0],i[1],i[2])
            
        await self.local_save_hedge_orders(symbol, hedge_id_list)
        # {'symbol': 'ADA-USDT', 'result': True, 'id': '2269739899423547392', 'clientOrderId': '', 'timestamp': 1740145920218, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2269739899423547392', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1740145920218'}], 'inTime': '1740145920217640', 'msg': '', 'outTime': '1740145920219807'}}
        # self.log.info(f"{hedgeMess}对冲延时:{round(time.time()*1000-t1, 2)}ms\n本次对冲order_id:{hedge_id_list}")
        vol = vol if side == 'buy' else vol*-1
        await self.local_save_hedge_pos(self.id, symbol, vol)
        notice = f"{strategy_name}发生对冲"
        tb.warning(notice, 'notice')

    # 将已对冲仓位本地化
    async def local_save_hedge_pos(self, uid, symbol, vol):
        if uid in self.local_hedge_pos:
            if symbol in self.local_hedge_pos[uid]:
                self.local_hedge_pos[uid][symbol] += vol
            else:
                self.local_hedge_pos[uid][symbol] = vol
        else:
            self.local_hedge_pos[uid] = {symbol: vol}
        self.local_hedge_pos = self._local_deals.save({'hedge_pos': self.local_hedge_pos})['hedge_pos']
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos}")

    # 将已挂单订单本地化
    async def local_save_hedge_orders(self, symbol, hedge_id_list):
        uid = self.id
        if uid in self.local_hedge_orderid:
            if symbol in self.local_hedge_orderid[uid]:
                self.local_hedge_orderid[uid][symbol].extend(hedge_id_list) # 追加
            else:
                self.local_hedge_orderid[uid][symbol] = hedge_id_list
        else:
            self.local_hedge_orderid[uid] = {symbol: hedge_id_list}
        # 本地化对冲计划
        await self.local_save('hedge_orderid')
    
    # 本地化
    async def local_save(self, tag):
        self.local_hedge_orderid = self._local_deals.save({'hedge_orderid': self.local_hedge_orderid})['hedge_orderid']
        self.log.info(f"1043本地化对冲订单:{self.local_hedge_orderid}")

def main(id, symbol, path='test_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(id, symbol, config).run()



if __name__ == '__main__':
    id = sys.argv[1]
    try:
        symbol = sys.argv[2]
    except:
        symbol = None
    main(id, symbol)

