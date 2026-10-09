'''
    策略逻辑：
    订阅内盘指定标签组用户的成交数据
    推送数据记录在字典中
    每1s读取数据做聚合去OK合约对冲
    实时完全对冲
'''

import sys
import time
import datetime
import requests
import traceback
import asyncio
import json
import numpy as np
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

# sys.path.append('/home/ubuntu/crypto_center/client/rest/okex')
# import contract as okx_rest

from crypto_center.client.rest.okex import contract as okx_rest


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()

        self._load_config(config)  # 读取配置
        self._initParams()    # 初始化参数
        self._setLocalDict()  # 配置本地数据
        
        # self.ws_wss = ws_contract_wss()
        # self.ws_wss.on_adl = self.on_adl
        # self.loop.create_task(self.ws_wss.only_subscribe())
        # self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest1 = ws_contract_rest(config['base_token'], config['base_secret'])
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
        self.thisVol = 0                    # 本次下单量
        self.hedge_clock = False            # 对冲锁
        self.risk_clock = False             # 风控锁
        self.exposeRiskAmt = getattr(self, 'exposeRiskAmt', 5000*1.1)   # 净敞口报警

    def _setLocalDict(self):
        """配置本地数据
        """
        pass
    
    async def on_first(self):
        await self.hedge_contract_info()       # 更新币对信息
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/1"))  # 每1s执行一次 
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    
    async def on_ticker_order(self, content):
        # print(f"外盘一档数据:{content}")
        symbol = content['symbol']
        bid = content['bid_price']
        ask = content['ask_price']
        bid_vol = content['bid_qty']
        ask_vol = content['ask_qty']
        self.symbols_bid_ask[symbol] = [[bid, bid_vol], [ask, ask_vol]]
        # print(f"外盘一档价格:{self.symbols_bid_ask}")

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        # 开仓挂单
        {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'side': 'BUY', 'origQty': 1, 'cumfilledSize': '', 'userId': 55970796, 'market': 'ADA-USDT', 'uid': '', 'cumfilledVol': '', 'price': '0.80258', 'lastfilledprice': '', 'tag': 'J', 'status': 'NEW', 'direction': 'MANY'},
        # 开仓全部成交
        {'lastfilledVolume': 0.8, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0401', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '+1', 'markPrice': '0.80232', 'price': '0.80258', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117169102, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80257', 'cumfilledVol': '0.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0002', 'marginType': 'full', 'lastfilledprice': '0.80257', 'status': 'FILLED'},
        # 开多加仓委托
        {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0400', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '+1', 'markPrice': '0.80116', 'price': '0.80160', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117769912, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80257', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0014', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 开多加仓全部成交
        {'lastfilledVolume': 0.8, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0801', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '+2', 'markPrice': '0.80116', 'price': '0.80160', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117769932, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80201', 'cumfilledVol': '0.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0017', 'marginType': 'full', 'lastfilledprice': '0.80145', 'status': 'FILLED'},
        # 平多挂单
        {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0798', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '+2', 'markPrice': '0.79829', 'price': '', 'tag': 'J', 'direction': 'MANY', 'side': 'SELL', 'origQty': 2, 'positionSide': 'LONG', 'updateTime': 1740119303294, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79844', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0003', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 平多全部成交
        {'lastfilledVolume': 1.59, 'orderType': 'market', 'lastfilledSize': 2, 'side': 'SELL', 'origQty': 2, 'cumfilledSize': 2, 'userId': 55970796, 'market': 'ADA-USDT', 'uid': 100022, 'cumfilledVol': '1.59', 'price': '', 'lastfilledprice': '0.79793', 'tag': 'J', 'status': 'FILLED', 'direction': 'MANY'},

        # 开空挂单
        {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'side': 'SELL', 'origQty': 1, 'cumfilledSize': '', 'userId': 55970796, 'market': 'ADA-USDT', 'uid': '', 'cumfilledVol': '', 'price': '0.79798', 'lastfilledprice': '', 'tag': 'J', 'status': 'NEW', 'direction': 'SPACE'},
        # 开空全部成交
        {'lastfilledVolume': 0.79, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0399', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '-1', 'markPrice': '0.79845', 'price': '0.79798', 'tag': 'J', 'direction': 'SPACE', 'side': 'SELL', 'origQty': 1, 'positionSide': 'SHORT', 'updateTime': 1740119045121, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79822', 'cumfilledVol': '0.79', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0002', 'marginType': 'full', 'lastfilledprice': '0.79822', 'status': 'FILLED'},
        # 平空挂单
        {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0399', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '-1', 'markPrice': '0.79854', 'price': '', 'tag': 'J', 'direction': 'SPACE', 'side': 'BUY', 'origQty': 1, 'positionSide': 'SHORT', 'updateTime': 1740119094534, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79822', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0003', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 平空全部成交
        {'lastfilledVolume': 0.79, 'orderType': 'market', 'lastfilledSize': 1, 'side': 'BUY', 'origQty': 1, 'cumfilledSize': 1, 'userId': 55970796, 'market': 'ADA-USDT', 'uid': 100022, 'cumfilledVol': '0.79', 'price': '', 'lastfilledprice': '0.79884', 'tag': 'J', 'status': 'FILLED', 'direction': 'SPACE'},

        # 逐仓开空全部成交
        {'lastfilledVolume': 2.39, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 3, 'isolatedMargin': '0.1197', 'liquidationPrice': '0.83417', 'cumfilledSize': 3, 'uid': 100022, 'positionAmt': '-3', 'markPrice': '0.79863', 'price': '0.79815', 'tag': 'J', 'direction': 'SPACE', 'side': 'SELL', 'origQty': 3, 'positionSide': 'SHORT', 'updateTime': 1740119888632, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79845', 'cumfilledVol': '2.39', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0005', 'marginType': 'isolated', 'lastfilledprice': '0.79845', 'status': 'FILLED'},

        if content['tag'] == self.tag and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.deal_wss(dict(content))
        
    # 内盘成交处理
    async def deal_wss(self, content):
        symbol = content['market']
        if symbol in self.deals_dict:
            self.deals_dict[symbol].append(content)
        else:
            self.deals_dict[symbol] = [content]

    # rest获取对冲端交易对详情
    async def hedge_contract_info(self):
        res = await self.okx_rest.fetch_precision()
        for s, v in res.items():
            price_precision = int(-np.log10(v['price']))    # 价格精度
            amount_precision = int(-np.log10(v['amount']))  # 数量精度
            face_value = v['faceValue']  # 合约面试,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']
            self.symbol_precision[s] = (price_precision, amount_precision, face_value, hedge_vol_limit)
        print(f"交易对精度+面值:{self.symbol_precision}")
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self):
        if self.risk_clock:
            return
        self.risk_clock = True
        self.log.info(f"风控线程")
        temp_data = [{'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '214.9624', 'liquidationPrice': '212.21', 'cumfilledSize': 36.31, 'uid': 100022, 'positionAmt': '+36.31', 'markPrice': '295.95', 'price': '296.06', 'tag': 'K', 'direction': 'MANY', 'amount': 36.31, 'side': 'BUY', 'origQty': 126.68, 'positionSide': 'LONG', 'updateTime': 1740550529365, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '295.98', 'cumfilledVol': '10747.03', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-1.0893', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},{'lastfilledVolume': 26748.61, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 90.37, 'isolatedMargin': '749.9709', 'liquidationPrice': '273.58', 'cumfilledSize': 126.68, 'uid': 100022, 'positionAmt': '+126.68', 'markPrice': '295.95', 'price': '296.06', 'tag': 'K', 'direction': 'MANY', 'amount': 90.37, 'side': 'BUY', 'origQty': 126.68, 'positionSide': 'LONG', 'updateTime': 1740550529406, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '295.98', 'cumfilledVol': '37495.65', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-3.8004', 'marginType': 'full', 'lastfilledprice': '295.99', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '606.4222', 'liquidationPrice': '267.04', 'cumfilledSize': 25.02, 'uid': 100022, 'positionAmt': '+101.66', 'markPrice': '298.30', 'price': '298.16', 'tag': 'K', 'direction': 'MANY', 'amount': 25.02, 'side': 'SELL', 'origQty': 126.68, 'positionSide': 'LONG', 'updateTime': 1740550726114, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '295.98', 'cumfilledVol': '7461.71', 'isAutoAddMargin': 'false', 'unRealizedProfit': '235.8512', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '586.3195', 'liquidationPrice': '265.91', 'cumfilledSize': 28.39, 'uid': 100022, 'positionAmt': '+98.29', 'markPrice': '298.30', 'price': '298.16', 'tag': 'K', 'direction': 'MANY', 'amount': 3.37, 'side': 'SELL', 'origQty': 126.68, 'positionSide': 'LONG', 'updateTime': 1740550726159, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '295.98', 'cumfilledVol': '8466.71', 'isAutoAddMargin': 'false', 'unRealizedProfit': '228.0328', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '336.3776', 'liquidationPrice': '240.48', 'cumfilledSize': 70.29, 'uid': 100022, 'positionAmt': '+56.39', 'markPrice': '298.30', 'price': '298.16', 'tag': 'K', 'direction': 'MANY', 'amount': 41.9, 'side': 'SELL', 'origQty': 126.68, 'positionSide': 'LONG', 'updateTime': 1740550726195, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '295.98', 'cumfilledVol': '20962.13', 'isAutoAddMargin': 'false', 'unRealizedProfit': '130.8248', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 16816.06, 'orderType': 'limit', 'lastfilledSize': 56.39, 'amount': 56.39, 'side': 'SELL', 'origQty': 126.68, 'cumfilledSize': 126.68, 'userId': 34320263, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '37778.19', 'price': '298.16', 'lastfilledprice': '298.21', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '23.9801', 'liquidationPrice': '26.53', 'cumfilledSize': 84.75, 'uid': 100022, 'positionAmt': '+3.94', 'markPrice': '298.36', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 84.75, 'side': 'SELL', 'origQty': 88.69, 'positionSide': 'LONG', 'updateTime': 1740550730000, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '296.06', 'cumfilledVol': '25274.14', 'isAutoAddMargin': 'false', 'unRealizedProfit': '9.062', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 1174.94, 'orderType': 'market', 'lastfilledSize': 3.94, 'amount': 3.94, 'side': 'SELL', 'origQty': 88.69, 'cumfilledSize': 88.69, 'userId': 89439747, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '26449.09', 'price': '', 'lastfilledprice': '298.21', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': '', 'isolatedMargin': '918.0032', 'liquidationPrice': '268.41', 'cumfilledSize': 122.29, 'uid': 100022, 'positionAmt': '+113.9', 'markPrice': '298.33', 'price': '298.09', 'tag': 'K', 'direction': 'MANY', 'amount': 122.29, 'side': 'SELL', 'origQty': 236.19, 'positionSide': 'LONG', 'updateTime': 1740550730321, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '295.85', 'cumfilledVol': '36468.10', 'isAutoAddMargin': 'false', 'unRealizedProfit': '282.472', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 33962.7, 'orderType': 'limit', 'lastfilledSize': 113.9, 'amount': 113.9, 'side': 'SELL', 'origQty': 236.19, 'cumfilledSize': 236.19, 'userId': 53597862, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '70430.80', 'price': '298.09', 'lastfilledprice': '298.18', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1516.2189', 'liquidationPrice': '286.53', 'cumfilledSize': 61.0, 'uid': 100022, 'positionAmt': '+254.22', 'markPrice': '298.33', 'price': '298.20', 'tag': 'K', 'direction': 'MANY', 'amount': 61.0, 'side': 'SELL', 'origQty': 173.37, 'positionSide': 'LONG', 'updateTime': 1740550730411, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '295.99', 'cumfilledVol': '18194.47', 'isAutoAddMargin': 'false', 'unRealizedProfit': '594.8748', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1120.3153', 'liquidationPrice': '281.39', 'cumfilledSize': 127.38, 'uid': 100022, 'positionAmt': '+187.84', 'markPrice': '298.33', 'price': '298.20', 'tag': 'K', 'direction': 'MANY', 'amount': 66.38, 'side': 'SELL', 'origQty': 173.37, 'positionSide': 'LONG', 'updateTime': 1740550730465, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '295.99', 'cumfilledVol': '37992.96', 'isAutoAddMargin': 'false', 'unRealizedProfit': '439.5456', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 13716.51, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 45.99, 'isolatedMargin': '846.0217', 'liquidationPrice': '274.17', 'cumfilledSize': 173.37, 'uid': 100022, 'positionAmt': '+141.85', 'markPrice': '298.33', 'price': '298.20', 'tag': 'K', 'direction': 'MANY', 'amount': 45.99, 'side': 'SELL', 'origQty': 173.37, 'positionSide': 'LONG', 'updateTime': 1740550730516, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '295.99', 'cumfilledVol': '51709.48', 'isAutoAddMargin': 'false', 'unRealizedProfit': '331.929', 'marginType': 'full', 'lastfilledprice': '298.25', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1460.9371', 'liquidationPrice': '281.64', 'cumfilledSize': 55.02, 'uid': 100022, 'positionAmt': '+244.91', 'markPrice': '298.32', 'price': '298.06', 'tag': 'K', 'direction': 'MANY', 'amount': 55.02, 'side': 'SELL', 'origQty': 164.96, 'positionSide': 'LONG', 'updateTime': 1740550732725, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '16403.11', 'isAutoAddMargin': 'false', 'unRealizedProfit': '565.7421', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1031.3234', 'liquidationPrice': '273.59', 'cumfilledSize': 127.04, 'uid': 100022, 'positionAmt': '+172.89', 'markPrice': '298.32', 'price': '298.06', 'tag': 'K', 'direction': 'MANY', 'amount': 72.02, 'side': 'SELL', 'origQty': 164.96, 'positionSide': 'LONG', 'updateTime': 1740550732762, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '37873.71', 'isAutoAddMargin': 'false', 'unRealizedProfit': '399.3759', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 11304.33, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 37.92, 'isolatedMargin': '805.1230', 'liquidationPrice': '265.11', 'cumfilledSize': 164.96, 'uid': 100022, 'positionAmt': '+134.97', 'markPrice': '298.32', 'price': '298.06', 'tag': 'K', 'direction': 'MANY', 'amount': 37.92, 'side': 'SELL', 'origQty': 164.96, 'positionSide': 'LONG', 'updateTime': 1740550732816, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '49178.04', 'isAutoAddMargin': 'false', 'unRealizedProfit': '311.7807', 'marginType': 'full', 'lastfilledprice': '298.11', 'status': 'FILLED'},
                {'lastfilledVolume': 71455.2, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 240.0, 'isolatedMargin': '649.9024', 'liquidationPrice': '258.20', 'cumfilledSize': 240.0, 'uid': 100022, 'positionAmt': '+106.77', 'markPrice': '298.32', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 240.0, 'side': 'SELL', 'origQty': 240.0, 'positionSide': 'LONG', 'updateTime': 1740550733270, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '296.65', 'cumfilledVol': '71455.20', 'isAutoAddMargin': 'false', 'unRealizedProfit': '178.3059', 'marginType': 'full', 'lastfilledprice': '297.73', 'status': 'FILLED'},
                {'lastfilledVolume': 9318.32, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 31.3, 'isolatedMargin': '459.3812', 'liquidationPrice': '241.02', 'cumfilledSize': 31.3, 'uid': 100022, 'positionAmt': '+75.47', 'markPrice': '298.32', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 31.3, 'side': 'SELL', 'origQty': 106.77, 'positionSide': 'LONG', 'updateTime': 1740550733326, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '296.65', 'cumfilledVol': '9318.32', 'isAutoAddMargin': 'false', 'unRealizedProfit': '126.0349', 'marginType': 'full', 'lastfilledprice': '297.71', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'lastfilledSize': '', 'amount': 75.47, 'side': 'SELL', 'origQty': 106.77, 'cumfilledSize': 106.77, 'userId': 76332266, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '31788.00', 'price': '', 'lastfilledprice': '', 'tag': 'K', 'status': 'PARTIALLY_FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1501.7045', 'liquidationPrice': '286.96', 'cumfilledSize': 39.0, 'uid': 100022, 'positionAmt': '+252.15', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 39.0, 'side': 'SELL', 'origQty': 145.57, 'positionSide': 'LONG', 'updateTime': 1740550734168, 'userId': 77640041, 'market': 'BCH-USDT', 'entryPrice': '296.02', 'cumfilledVol': '11574.03', 'isAutoAddMargin': 'false', 'unRealizedProfit': '597.5955', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '899.8911', 'liquidationPrice': '277.68', 'cumfilledSize': 140.05, 'uid': 100022, 'positionAmt': '+151.1', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 101.05, 'side': 'SELL', 'origQty': 145.57, 'positionSide': 'LONG', 'updateTime': 1740550734215, 'userId': 77640041, 'market': 'BCH-USDT', 'entryPrice': '296.02', 'cumfilledVol': '41561.62', 'isAutoAddMargin': 'false', 'unRealizedProfit': '358.107', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 1638.06, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 5.52, 'isolatedMargin': '867.0162', 'liquidationPrice': '276.88', 'cumfilledSize': 145.57, 'uid': 100022, 'positionAmt': '+145.58', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 5.52, 'side': 'SELL', 'origQty': 145.57, 'positionSide': 'LONG', 'updateTime': 1740550734269, 'userId': 77640041, 'market': 'BCH-USDT', 'entryPrice': '296.02', 'cumfilledVol': '43199.68', 'isAutoAddMargin': 'false', 'unRealizedProfit': '345.0246', 'marginType': 'full', 'lastfilledprice': '296.75', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1885.9598', 'liquidationPrice': '289.10', 'cumfilledSize': 20.33, 'uid': 100022, 'positionAmt': '+316.67', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 20.33, 'side': 'SELL', 'origQty': 155.02, 'positionSide': 'LONG', 'updateTime': 1740550734545, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '6037.60', 'isAutoAddMargin': 'false', 'unRealizedProfit': '753.6746', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1475.1425', 'liquidationPrice': '286.13', 'cumfilledSize': 89.31, 'uid': 100022, 'positionAmt': '+247.69', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 68.98, 'side': 'SELL', 'origQty': 155.02, 'positionSide': 'LONG', 'updateTime': 1740550734598, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '26522.59', 'isAutoAddMargin': 'false', 'unRealizedProfit': '589.5022', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 19513.24, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 65.71, 'isolatedMargin': '1083.8000', 'liquidationPrice': '281.20', 'cumfilledSize': 155.02, 'uid': 100022, 'positionAmt': '+181.98', 'markPrice': '298.39', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 65.71, 'side': 'SELL', 'origQty': 155.02, 'positionSide': 'LONG', 'updateTime': 1740550734652, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '46035.83', 'isAutoAddMargin': 'false', 'unRealizedProfit': '433.1124', 'marginType': 'full', 'lastfilledprice': '296.96', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '470.1350', 'liquidationPrice': '254.43', 'cumfilledSize': 62.91, 'uid': 100022, 'positionAmt': '+78.94', 'markPrice': '298.39', 'price': '296.94', 'tag': 'K', 'direction': 'MANY', 'amount': 62.91, 'side': 'SELL', 'origQty': 141.85, 'positionSide': 'LONG', 'updateTime': 1740550734854, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '295.99', 'cumfilledVol': '18686.78', 'isAutoAddMargin': 'false', 'unRealizedProfit': '189.456', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 6050.5, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 20.37, 'isolatedMargin': '347.9643', 'liquidationPrice': '238.96', 'cumfilledSize': 83.28, 'uid': 100022, 'positionAmt': '+58.57', 'markPrice': '298.24', 'price': '296.94', 'tag': 'K', 'direction': 'MANY', 'amount': 20.37, 'side': 'SELL', 'origQty': 141.85, 'positionSide': 'LONG', 'updateTime': 1740550734910, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '295.99', 'cumfilledVol': '24737.28', 'isAutoAddMargin': 'false', 'unRealizedProfit': '131.7825', 'marginType': 'full', 'lastfilledprice': '297.03', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'amount': 58.57, 'side': 'SELL', 'origQty': 141.85, 'cumfilledSize': 141.85, 'userId': 68143850, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '42135.50', 'price': '296.94', 'lastfilledprice': '', 'tag': 'K', 'status': 'PARTIALLY_FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '712.9942', 'liquidationPrice': '272.24', 'cumfilledSize': 25.62, 'uid': 100022, 'positionAmt': '+119.96', 'markPrice': '297.27', 'price': '296.66', 'tag': 'K', 'direction': 'MANY', 'amount': 25.62, 'side': 'SELL', 'origQty': 145.58, 'positionSide': 'LONG', 'updateTime': 1740550737156, 'userId': 77640041, 'market': 'BCH-USDT', 'entryPrice': '296.02', 'cumfilledVol': '7602.22', 'isAutoAddMargin': 'false', 'unRealizedProfit': '149.95', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '183.1223', 'liquidationPrice': '195.50', 'cumfilledSize': 114.77, 'uid': 100022, 'positionAmt': '+30.81', 'markPrice': '297.27', 'price': '296.66', 'tag': 'K', 'direction': 'MANY', 'amount': 89.15, 'side': 'SELL', 'origQty': 145.58, 'positionSide': 'LONG', 'updateTime': 1740550737181, 'userId': 77640041, 'market': 'BCH-USDT', 'entryPrice': '296.02', 'cumfilledVol': '34054.81', 'isAutoAddMargin': 'false', 'unRealizedProfit': '38.5125', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 9141.63, 'orderType': 'limit', 'lastfilledSize': 30.81, 'amount': 30.81, 'side': 'SELL', 'origQty': 145.58, 'cumfilledSize': 145.58, 'userId': 77640041, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '43196.44', 'price': '296.66', 'lastfilledprice': '296.71', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '704.7333', 'liquidationPrice': '270.59', 'cumfilledSize': 63.17, 'uid': 100022, 'positionAmt': '+118.81', 'markPrice': '297.24', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 63.17, 'side': 'SELL', 'origQty': 98.26, 'positionSide': 'LONG', 'updateTime': 1740550738323, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '18743.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '146.1363', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 10411.9, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 35.09, 'isolatedMargin': '496.5935', 'liquidationPrice': '258.81', 'cumfilledSize': 98.26, 'uid': 100022, 'positionAmt': '+83.72', 'markPrice': '297.24', 'price': '296.72', 'tag': 'K', 'direction': 'MANY', 'amount': 35.09, 'side': 'SELL', 'origQty': 98.26, 'positionSide': 'LONG', 'updateTime': 1740550738362, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '29155.70', 'isAutoAddMargin': 'false', 'unRealizedProfit': '102.9756', 'marginType': 'full', 'lastfilledprice': '296.72', 'status': 'FILLED'},
                {'lastfilledVolume': 71224.8, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 240.0, 'isolatedMargin': '586.2326', 'liquidationPrice': '259.12', 'cumfilledSize': 240.0, 'uid': 100022, 'positionAmt': '+96.8', 'markPrice': '297.13', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 240.0, 'side': 'SELL', 'origQty': 240.0, 'positionSide': 'LONG', 'updateTime': 1740550744512, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '296.23', 'cumfilledVol': '71224.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '87.12', 'marginType': 'full', 'lastfilledprice': '296.77', 'status': 'FILLED'},
                {'lastfilledVolume': 28727.33, 'orderType': 'market', 'lastfilledSize': 96.8, 'amount': 96.8, 'side': 'SELL', 'origQty': 96.8, 'cumfilledSize': 96.8, 'userId': 47383786, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '28727.33', 'price': '', 'lastfilledprice': '296.77', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '452.7811', 'liquidationPrice': '239.24', 'cumfilledSize': 58.68, 'uid': 100022, 'positionAmt': '+76.29', 'markPrice': '296.92', 'price': '296.67', 'tag': 'K', 'direction': 'MANY', 'amount': 58.68, 'side': 'SELL', 'origQty': 74.23, 'positionSide': 'LONG', 'updateTime': 1740550749926, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '17411.52', 'isAutoAddMargin': 'false', 'unRealizedProfit': '69.4239', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 4613.68, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 15.55, 'isolatedMargin': '360.4919', 'liquidationPrice': '224.05', 'cumfilledSize': 74.23, 'uid': 100022, 'positionAmt': '+60.74', 'markPrice': '296.92', 'price': '296.67', 'tag': 'K', 'direction': 'MANY', 'amount': 15.55, 'side': 'SELL', 'origQty': 74.23, 'positionSide': 'LONG', 'updateTime': 1740550749964, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '22025.21', 'isAutoAddMargin': 'false', 'unRealizedProfit': '55.2734', 'marginType': 'full', 'lastfilledprice': '296.7', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '352.0166', 'liquidationPrice': '350.17', 'cumfilledSize': 59.36, 'uid': 100022, 'positionAmt': '-59.36', 'markPrice': '296.83', 'price': '296.48', 'tag': 'K', 'direction': 'SPACE', 'amount': 59.36, 'side': 'SELL', 'origQty': 136.58, 'positionSide': 'SHORT', 'updateTime': 1740550760671, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.55', 'cumfilledVol': '17603.20', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-16.6208', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '639.6906', 'liquidationPrice': '325.04', 'cumfilledSize': 107.87, 'uid': 100022, 'positionAmt': '-107.87', 'markPrice': '296.83', 'price': '296.48', 'tag': 'K', 'direction': 'SPACE', 'amount': 48.51, 'side': 'SELL', 'origQty': 136.58, 'positionSide': 'SHORT', 'updateTime': 1740550760715, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.54', 'cumfilledVol': '31988.36', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-31.2823', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 8513.37, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 28.71, 'isolatedMargin': '809.9467', 'liquidationPrice': '318.58', 'cumfilledSize': 136.58, 'uid': 100022, 'positionAmt': '-136.58', 'markPrice': '296.83', 'price': '296.48', 'tag': 'K', 'direction': 'SPACE', 'amount': 28.71, 'side': 'SELL', 'origQty': 136.58, 'positionSide': 'SHORT', 'updateTime': 1740550760760, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.54', 'cumfilledVol': '40501.73', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-39.6082', 'marginType': 'full', 'lastfilledprice': '296.53', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1435.6409', 'liquidationPrice': '287.76', 'cumfilledSize': 31.39, 'uid': 100022, 'positionAmt': '+237.24', 'markPrice': '296.81', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 31.39, 'side': 'SELL', 'origQty': 240.0, 'positionSide': 'LONG', 'updateTime': 1740550762691, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '296.12', 'cumfilledVol': '9307.44', 'isAutoAddMargin': 'false', 'unRealizedProfit': '163.6956', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1190.7396', 'liquidationPrice': '285.39', 'cumfilledSize': 71.86, 'uid': 100022, 'positionAmt': '+196.77', 'markPrice': '296.81', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 40.47, 'side': 'SELL', 'origQty': 240.0, 'positionSide': 'LONG', 'updateTime': 1740550762741, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '296.12', 'cumfilledVol': '21306.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '135.7713', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 49851.82, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 168.14, 'isolatedMargin': '173.2524', 'liquidationPrice': '202.67', 'cumfilledSize': 240.0, 'uid': 100022, 'positionAmt': '+28.63', 'markPrice': '296.81', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 168.14, 'side': 'SELL', 'origQty': 240.0, 'positionSide': 'LONG', 'updateTime': 1740550762790, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '296.12', 'cumfilledVol': '71158.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '19.7547', 'marginType': 'full', 'lastfilledprice': '296.49', 'status': 'FILLED'},
                {'lastfilledVolume': 7651.63, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 25.81, 'isolatedMargin': '17.0650', 'liquidationPrice': '--', 'cumfilledSize': 25.81, 'uid': 100022, 'positionAmt': '+2.82', 'markPrice': '296.81', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 25.81, 'side': 'SELL', 'origQty': 28.63, 'positionSide': 'LONG', 'updateTime': 1740550762848, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '296.12', 'cumfilledVol': '7651.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '1.9458', 'marginType': 'full', 'lastfilledprice': '296.46', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'lastfilledSize': '', 'amount': 2.82, 'side': 'SELL', 'origQty': 28.63, 'cumfilledSize': 28.63, 'userId': 69728812, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '8487.73', 'price': '', 'lastfilledprice': '', 'tag': 'K', 'status': 'PARTIALLY_FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '231.3199', 'liquidationPrice': '181.54', 'cumfilledSize': 21.83, 'uid': 100022, 'positionAmt': '+38.91', 'markPrice': '297.49', 'price': '297.25', 'tag': 'K', 'direction': 'MANY', 'amount': 21.83, 'side': 'SELL', 'origQty': 60.74, 'positionSide': 'LONG', 'updateTime': 1740550824255, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '6490.49', 'isAutoAddMargin': 'false', 'unRealizedProfit': '57.5868', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 11568.33, 'orderType': 'limit', 'lastfilledSize': 38.91, 'amount': 38.91, 'side': 'SELL', 'origQty': 60.74, 'cumfilledSize': 60.74, 'userId': 31611693, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '18058.82', 'price': '297.25', 'lastfilledprice': '297.31', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '280.4916', 'liquidationPrice': '227.46', 'cumfilledSize': 36.55, 'uid': 100022, 'positionAmt': '+47.17', 'markPrice': '297.50', 'price': '', 'tag': 'K', 'direction': 'MANY', 'amount': 36.55, 'side': 'SELL', 'origQty': 83.72, 'positionSide': 'LONG', 'updateTime': 1740550825067, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '296.01', 'cumfilledVol': '10868.50', 'isAutoAddMargin': 'false', 'unRealizedProfit': '70.2833', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 14025.99, 'orderType': 'market', 'lastfilledSize': 47.17, 'amount': 47.17, 'side': 'SELL', 'origQty': 83.72, 'cumfilledSize': 83.72, 'userId': 23157236, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '24894.50', 'price': '', 'lastfilledprice': '297.35', 'tag': 'K', 'status': 'FILLED', 'direction': 'MANY'},
                {'lastfilledVolume': 24896.65, 'orderType': 'market', 'leverage': 50, 'lastfilledSize': 83.72, 'isolatedMargin': '497.8326', 'liquidationPrice': '335.02', 'cumfilledSize': 83.72, 'uid': 100022, 'positionAmt': '-83.72', 'markPrice': '297.50', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 83.72, 'side': 'SELL', 'origQty': 83.72, 'positionSide': 'SHORT', 'updateTime': 1740550825430, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '297.38', 'cumfilledVol': '24896.65', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-10.0464', 'marginType': 'full', 'lastfilledprice': '297.38', 'status': 'FILLED'},
                {'lastfilledVolume': 47997.13, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 161.4, 'isolatedMargin': '959.7489', 'liquidationPrice': '316.60', 'cumfilledSize': 161.4, 'uid': 100022, 'positionAmt': '-161.4', 'markPrice': '297.50', 'price': '297.25', 'tag': 'K', 'direction': 'SPACE', 'amount': 161.4, 'side': 'SELL', 'origQty': 161.4, 'positionSide': 'SHORT', 'updateTime': 1740550825581, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.38', 'cumfilledVol': '47997.13', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-19.368', 'marginType': 'full', 'lastfilledprice': '297.38', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '436.0121', 'liquidationPrice': '355.78', 'cumfilledSize': 71.85, 'uid': 100022, 'positionAmt': '-71.85', 'markPrice': '297.59', 'price': '297.25', 'tag': 'K', 'direction': 'SPACE', 'amount': 71.85, 'side': 'SELL', 'origQty': 148.35, 'positionSide': 'SHORT', 'updateTime': 1740550827737, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.32', 'cumfilledVol': '21362.44', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-19.3995', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 22744.21, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 76.5, 'isolatedMargin': '900.2422', 'liquidationPrice': '324.46', 'cumfilledSize': 148.35, 'uid': 100022, 'positionAmt': '-148.35', 'markPrice': '297.59', 'price': '297.25', 'tag': 'K', 'direction': 'SPACE', 'amount': 76.5, 'side': 'SELL', 'origQty': 148.35, 'positionSide': 'SHORT', 'updateTime': 1740550827777, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.31', 'cumfilledVol': '44106.65', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-41.538', 'marginType': 'full', 'lastfilledprice': '297.31', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '335.1221', 'liquidationPrice': '343.88', 'cumfilledSize': 55.23, 'uid': 100022, 'positionAmt': '-55.23', 'markPrice': '297.57', 'price': '297.42', 'tag': 'K', 'direction': 'SPACE', 'amount': 55.23, 'side': 'SELL', 'origQty': 186.25, 'positionSide': 'SHORT', 'updateTime': 1740550829063, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.42', 'cumfilledVol': '16426.50', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-8.2845', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 38967.96, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 131.02, 'isolatedMargin': '1130.1193', 'liquidationPrice': '308.75', 'cumfilledSize': 186.25, 'uid': 100022, 'positionAmt': '-186.25', 'markPrice': '297.57', 'price': '297.42', 'tag': 'K', 'direction': 'SPACE', 'amount': 131.02, 'side': 'SELL', 'origQty': 186.25, 'positionSide': 'SHORT', 'updateTime': 1740550829317, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.42', 'cumfilledVol': '55394.47', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-27.9375', 'marginType': 'full', 'lastfilledprice': '297.42', 'status': 'FILLED'},
                {'lastfilledVolume': 490.28, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 1.65, 'isolatedMargin': '10.0151', 'liquidationPrice': '940.86', 'cumfilledSize': 1.65, 'uid': 100022, 'positionAmt': '-1.65', 'markPrice': '297.74', 'price': '297.11', 'tag': 'K', 'direction': 'SPACE', 'amount': 1.65, 'side': 'SELL', 'origQty': 66.79, 'positionSide': 'SHORT', 'updateTime': 1740550831688, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '297.14', 'cumfilledVol': '490.28', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.99', 'marginType': 'full', 'lastfilledprice': '297.14', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '405.4016', 'liquidationPrice': '310.81', 'cumfilledSize': 66.79, 'uid': 100022, 'positionAmt': '-66.79', 'markPrice': '297.74', 'price': '297.11', 'tag': 'K', 'direction': 'SPACE', 'amount': 65.14, 'side': 'SELL', 'origQty': 66.79, 'positionSide': 'SHORT', 'updateTime': 1740550831743, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '297.15', 'cumfilledVol': '19847.28', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-39.4061', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 8048.16, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 27.09, 'isolatedMargin': '569.5450', 'liquidationPrice': '306.20', 'cumfilledSize': 27.09, 'uid': 100022, 'positionAmt': '-93.88', 'markPrice': '297.55', 'price': '297.04', 'tag': 'K', 'direction': 'SPACE', 'amount': 27.09, 'side': 'SELL', 'origQty': 27.09, 'positionSide': 'SHORT', 'updateTime': 1740550833708, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '297.13', 'cumfilledVol': '8048.16', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-39.4296', 'marginType': 'full', 'lastfilledprice': '297.09', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '389.7226', 'liquidationPrice': '363.63', 'cumfilledSize': 65.59, 'uid': 100022, 'positionAmt': '-65.59', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 65.59, 'side': 'SELL', 'origQty': 146.39, 'positionSide': 'SHORT', 'updateTime': 1740550835528, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.13', 'cumfilledVol': '19488.75', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-20.9888', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '620.9181', 'liquidationPrice': '338.02', 'cumfilledSize': 104.5, 'uid': 100022, 'positionAmt': '-104.5', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 38.91, 'side': 'SELL', 'origQty': 146.39, 'positionSide': 'SHORT', 'updateTime': 1740550835567, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.12', 'cumfilledVol': '31049.69', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-34.485', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 12445.93, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 41.89, 'isolatedMargin': '869.8201', 'liquidationPrice': '325.67', 'cumfilledSize': 146.39, 'uid': 100022, 'positionAmt': '-146.39', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 41.89, 'side': 'SELL', 'origQty': 146.39, 'positionSide': 'SHORT', 'updateTime': 1740550835624, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.12', 'cumfilledVol': '43495.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-48.3087', 'marginType': 'full', 'lastfilledprice': '297.11', 'status': 'FILLED'},
                {'lastfilledVolume': 3078.16, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 10.36, 'isolatedMargin': '632.0985', 'liquidationPrice': '305.08', 'cumfilledSize': 10.36, 'uid': 100022, 'positionAmt': '-104.24', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 10.36, 'side': 'SELL', 'origQty': 10.36, 'positionSide': 'SHORT', 'updateTime': 1740550836196, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '297.13', 'cumfilledVol': '3078.16', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-33.3568', 'marginType': 'full', 'lastfilledprice': '297.12', 'status': 'FILLED'},
                {'lastfilledVolume': 27987.76, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 94.2, 'isolatedMargin': '1470.7935', 'liquidationPrice': '312.04', 'cumfilledSize': 94.2, 'uid': 100022, 'positionAmt': '-242.55', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 94.2, 'side': 'SELL', 'origQty': 148.45, 'positionSide': 'SHORT', 'updateTime': 1740550836239, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.23', 'cumfilledVol': '27987.76', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-53.361', 'marginType': 'full', 'lastfilledprice': '297.11', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1799.7588', 'liquidationPrice': '308.74', 'cumfilledSize': 148.45, 'uid': 100022, 'positionAmt': '-296.8', 'markPrice': '297.45', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 54.25, 'side': 'SELL', 'origQty': 148.45, 'positionSide': 'SHORT', 'updateTime': 1740550836290, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.21', 'cumfilledVol': '44106.52', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-71.232', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1167.5033', 'liquidationPrice': '310.41', 'cumfilledSize': 59.89, 'uid': 100022, 'positionAmt': '-196.47', 'markPrice': '297.34', 'price': '297.02', 'tag': 'K', 'direction': 'SPACE', 'amount': 59.89, 'side': 'SELL', 'origQty': 184.13, 'positionSide': 'SHORT', 'updateTime': 1740550838866, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.70', 'cumfilledVol': '17791.52', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-125.7408', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1442.1016', 'liquidationPrice': '307.27', 'cumfilledSize': 106.1, 'uid': 100022, 'positionAmt': '-242.68', 'markPrice': '297.34', 'price': '297.02', 'tag': 'K', 'direction': 'SPACE', 'amount': 46.21, 'side': 'SELL', 'origQty': 184.13, 'positionSide': 'SHORT', 'updateTime': 1740550838917, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.77', 'cumfilledVol': '31518.66', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-138.3276', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 23178.81, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 78.03, 'isolatedMargin': '1905.7871', 'liquidationPrice': '304.02', 'cumfilledSize': 184.13, 'uid': 100022, 'positionAmt': '-320.71', 'markPrice': '297.34', 'price': '297.02', 'tag': 'K', 'direction': 'SPACE', 'amount': 78.03, 'side': 'SELL', 'origQty': 184.13, 'positionSide': 'SHORT', 'updateTime': 1740550838968, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.83', 'cumfilledVol': '54697.47', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-163.5621', 'marginType': 'full', 'lastfilledprice': '297.05', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1218.2514', 'liquidationPrice': '311.05', 'cumfilledSize': 43.61, 'uid': 100022, 'positionAmt': '-205.01', 'markPrice': '297.34', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 43.61, 'side': 'SELL', 'origQty': 96.14, 'positionSide': 'SHORT', 'updateTime': 1740550840340, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.32', 'cumfilledVol': '12957.40', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-4.1002', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 15607.18, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 52.53, 'isolatedMargin': '1530.4056', 'liquidationPrice': '307.57', 'cumfilledSize': 96.14, 'uid': 100022, 'positionAmt': '-257.54', 'markPrice': '297.34', 'price': '297.06', 'tag': 'K', 'direction': 'SPACE', 'amount': 52.53, 'side': 'SELL', 'origQty': 96.14, 'positionSide': 'SHORT', 'updateTime': 1740550840394, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.28', 'cumfilledVol': '28564.59', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-15.4524', 'marginType': 'full', 'lastfilledprice': '297.11', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1353.9758', 'liquidationPrice': '313.68', 'cumfilledSize': 81.46, 'uid': 100022, 'positionAmt': '-227.85', 'markPrice': '297.34', 'price': '297.01', 'tag': 'K', 'direction': 'SPACE', 'amount': 81.46, 'side': 'SELL', 'origQty': 146.98, 'positionSide': 'SHORT', 'updateTime': 1740550841739, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.09', 'cumfilledVol': '24198.50', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-56.9625', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 19462.71, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 65.52, 'isolatedMargin': '1743.3218', 'liquidationPrice': '309.29', 'cumfilledSize': 146.98, 'uid': 100022, 'positionAmt': '-293.37', 'markPrice': '297.34', 'price': '297.01', 'tag': 'K', 'direction': 'SPACE', 'amount': 65.52, 'side': 'SELL', 'origQty': 146.98, 'positionSide': 'SHORT', 'updateTime': 1740550841779, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.08', 'cumfilledVol': '43661.22', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-76.2762', 'marginType': 'full', 'lastfilledprice': '297.05', 'status': 'FILLED'},
                {'lastfilledVolume': 14061.26, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 47.33, 'isolatedMargin': '1416.0668', 'liquidationPrice': '305.75', 'cumfilledSize': 47.33, 'uid': 100022, 'positionAmt': '-233.58', 'markPrice': '297.32', 'price': '297.09', 'tag': 'K', 'direction': 'SPACE', 'amount': 47.33, 'side': 'SELL', 'origQty': 47.33, 'positionSide': 'SHORT', 'updateTime': 1740550841928, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.35', 'cumfilledVol': '14061.26', 'isAutoAddMargin': 'false', 'unRealizedProfit': '7.0074', 'marginType': 'full', 'lastfilledprice': '297.09', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1682.4800', 'liquidationPrice': '306.35', 'cumfilledSize': 25.62, 'uid': 100022, 'positionAmt': '-283.16', 'markPrice': '297.30', 'price': '297.10', 'tag': 'K', 'direction': 'SPACE', 'amount': 25.62, 'side': 'SELL', 'origQty': 39.57, 'positionSide': 'SHORT', 'updateTime': 1740550845273, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.27', 'cumfilledVol': '7613.49', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-8.4948', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 4145.38, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 13.95, 'isolatedMargin': '1765.3681', 'liquidationPrice': '305.76', 'cumfilledSize': 39.57, 'uid': 100022, 'positionAmt': '-297.11', 'markPrice': '297.30', 'price': '297.10', 'tag': 'K', 'direction': 'SPACE', 'amount': 13.95, 'side': 'SELL', 'origQty': 39.57, 'positionSide': 'SHORT', 'updateTime': 1740550845337, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.26', 'cumfilledVol': '11758.87', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-11.8844', 'marginType': 'full', 'lastfilledprice': '297.16', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '63.2377', 'liquidationPrice': '658.21', 'cumfilledSize': 10.43, 'uid': 100022, 'positionAmt': '-10.43', 'markPrice': '297.30', 'price': '297.17', 'tag': 'K', 'direction': 'SPACE', 'amount': 10.43, 'side': 'SELL', 'origQty': 201.88, 'positionSide': 'SHORT', 'updateTime': 1740550845855, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.17', 'cumfilledVol': '3099.48', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-1.3559', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '397.1911', 'liquidationPrice': '352.55', 'cumfilledSize': 65.51, 'uid': 100022, 'positionAmt': '-65.51', 'markPrice': '297.30', 'price': '297.17', 'tag': 'K', 'direction': 'SPACE', 'amount': 55.08, 'side': 'SELL', 'origQty': 201.88, 'positionSide': 'SHORT', 'updateTime': 1740550845904, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.17', 'cumfilledVol': '19467.60', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-8.5163', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '767.9721', 'liquidationPrice': '324.77', 'cumfilledSize': 126.63, 'uid': 100022, 'positionAmt': '-126.63', 'markPrice': '297.35', 'price': '297.17', 'tag': 'K', 'direction': 'SPACE', 'amount': 61.12, 'side': 'SELL', 'origQty': 201.88, 'positionSide': 'SHORT', 'updateTime': 1740550847312, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.17', 'cumfilledVol': '37630.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-22.7934', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 22362.04, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 75.25, 'isolatedMargin': '1224.3404', 'liquidationPrice': '312.74', 'cumfilledSize': 201.88, 'uid': 100022, 'positionAmt': '-201.88', 'markPrice': '297.35', 'price': '297.17', 'tag': 'K', 'direction': 'SPACE', 'amount': 75.25, 'side': 'SELL', 'origQty': 201.88, 'positionSide': 'SHORT', 'updateTime': 1740550847814, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.17', 'cumfilledVol': '59992.67', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-36.3384', 'marginType': 'full', 'lastfilledprice': '297.17', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1499.0103', 'liquidationPrice': '309.31', 'cumfilledSize': 45.29, 'uid': 100022, 'positionAmt': '-247.17', 'markPrice': '297.46', 'price': '297.12', 'tag': 'K', 'direction': 'SPACE', 'amount': 45.29, 'side': 'SELL', 'origQty': 84.8, 'positionSide': 'SHORT', 'updateTime': 1740550850913, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.16', 'cumfilledVol': '13456.56', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-74.151', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': '', 'isolatedMargin': '431.8319', 'liquidationPrice': '360.91', 'cumfilledSize': 53.79, 'uid': 100022, 'positionAmt': '-53.79', 'markPrice': '297.45', 'price': '296.95', 'tag': 'K', 'direction': 'SPACE', 'amount': 53.79, 'side': 'SELL', 'origQty': 161.5, 'positionSide': 'SHORT', 'updateTime': 1740550854109, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '297.00', 'cumfilledVol': '15975.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-24.2055', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': '', 'isolatedMargin': '1010.4979', 'liquidationPrice': '323.02', 'cumfilledSize': 125.87, 'uid': 100022, 'positionAmt': '-125.87', 'markPrice': '297.45', 'price': '296.95', 'tag': 'K', 'direction': 'SPACE', 'amount': 72.08, 'side': 'SELL', 'origQty': 161.5, 'positionSide': 'SHORT', 'updateTime': 1740550854129, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '296.99', 'cumfilledVol': '37382.66', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-57.9002', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 10581.39, 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': 35.63, 'isolatedMargin': '1296.5394', 'liquidationPrice': '316.78', 'cumfilledSize': 161.5, 'uid': 100022, 'positionAmt': '-161.5', 'markPrice': '297.45', 'price': '296.95', 'tag': 'K', 'direction': 'SPACE', 'amount': 35.63, 'side': 'SELL', 'origQty': 161.5, 'positionSide': 'SHORT', 'updateTime': 1740550854178, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '296.99', 'cumfilledVol': '47964.06', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-74.29', 'marginType': 'full', 'lastfilledprice': '296.98', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1496.0342', 'liquidationPrice': '309.31', 'cumfilledSize': 45.29, 'uid': '', 'positionAmt': '-247.17', 'markPrice': '296.59', 'price': '297.12', 'tag': 'K', 'direction': 'SPACE', 'side': 'SELL', 'origQty': 84.8, 'positionSide': 'SHORT', 'updateTime': 1740550937376, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.16', 'cumfilledVol': '13456.56', 'isAutoAddMargin': 'false', 'unRealizedProfit': '140.8869', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED_CANCELED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '1271.0817', 'liquidationPrice': '315.05', 'cumfilledSize': 78.74, 'uid': 100022, 'positionAmt': '-214.63', 'markPrice': '295.83', 'price': '296.25', 'tag': 'K', 'direction': 'SPACE', 'amount': 78.74, 'side': 'BUY', 'origQty': 146.68, 'positionSide': 'SHORT', 'updateTime': 1740551010829, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.08', 'cumfilledVol': '23314.91', 'isAutoAddMargin': 'false', 'unRealizedProfit': '268.2875', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 20119.75, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 67.94, 'isolatedMargin': '868.7275', 'liquidationPrice': '326.06', 'cumfilledSize': 146.68, 'uid': 100022, 'positionAmt': '-146.69', 'markPrice': '295.83', 'price': '296.25', 'tag': 'K', 'direction': 'SPACE', 'amount': 67.94, 'side': 'BUY', 'origQty': 146.68, 'positionSide': 'SHORT', 'updateTime': 1740551010849, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.08', 'cumfilledVol': '43434.66', 'isAutoAddMargin': 'false', 'unRealizedProfit': '183.3625', 'marginType': 'full', 'lastfilledprice': '296.14', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '362.9716', 'liquidationPrice': '349.95', 'cumfilledSize': 22.43, 'uid': 100022, 'positionAmt': '-61.29', 'markPrice': '295.83', 'price': '296.19', 'tag': 'K', 'direction': 'SPACE', 'amount': 22.43, 'side': 'BUY', 'origQty': 83.72, 'positionSide': 'SHORT', 'updateTime': 1740551010954, 'userId': 23157236, 'market': 'BCH-USDT', 'entryPrice': '297.38', 'cumfilledVol': '6641.74', 'isAutoAddMargin': 'false', 'unRealizedProfit': '94.9995', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 18149.19, 'orderType': 'limit', 'lastfilledSize': 61.29, 'amount': 61.29, 'side': 'BUY', 'origQty': 83.72, 'cumfilledSize': 83.72, 'userId': 23157236, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '24790.94', 'price': '296.19', 'lastfilledprice': '296.12', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': 29911.08, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 101.01, 'isolatedMargin': '1161.3434', 'liquidationPrice': '312.16', 'cumfilledSize': 101.01, 'uid': 100022, 'positionAmt': '-196.1', 'markPrice': '295.84', 'price': '296.21', 'tag': 'K', 'direction': 'SPACE', 'amount': 101.01, 'side': 'BUY', 'origQty': 101.01, 'positionSide': 'SHORT', 'updateTime': 1740551011038, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.26', 'cumfilledVol': '29911.08', 'isAutoAddMargin': 'false', 'unRealizedProfit': '278.462', 'marginType': 'full', 'lastfilledprice': '296.12', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '952.8227', 'liquidationPrice': '315.55', 'cumfilledSize': 159.82, 'uid': 100022, 'positionAmt': '-160.89', 'markPrice': '295.84', 'price': '296.21', 'tag': 'K', 'direction': 'SPACE', 'amount': 159.82, 'side': 'BUY', 'origQty': 160.35, 'positionSide': 'SHORT', 'updateTime': 1740551011293, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.83', 'cumfilledVol': '47325.89', 'isAutoAddMargin': 'false', 'unRealizedProfit': '159.2811', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 156.95, 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': 0.53, 'isolatedMargin': '949.6839', 'liquidationPrice': '315.62', 'cumfilledSize': 160.35, 'uid': 100022, 'positionAmt': '-160.36', 'markPrice': '295.84', 'price': '296.21', 'tag': 'K', 'direction': 'SPACE', 'amount': 0.53, 'side': 'BUY', 'origQty': 160.35, 'positionSide': 'SHORT', 'updateTime': 1740551011342, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.83', 'cumfilledVol': '47482.85', 'isAutoAddMargin': 'false', 'unRealizedProfit': '158.7564', 'marginType': 'full', 'lastfilledprice': '296.14', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': '', 'isolatedMargin': '1010.7754', 'liquidationPrice': '323.06', 'cumfilledSize': 35.2, 'uid': 100022, 'positionAmt': '-126.3', 'markPrice': '295.84', 'price': '296.19', 'tag': 'K', 'direction': 'SPACE', 'amount': 35.2, 'side': 'BUY', 'origQty': 161.5, 'positionSide': 'SHORT', 'updateTime': 1740551011357, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '296.99', 'cumfilledVol': '10423.42', 'isAutoAddMargin': 'false', 'unRealizedProfit': '145.245', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 30493.53, 'orderType': 'limit', 'leverage': 37, 'lastfilledSize': 102.97, 'isolatedMargin': '186.7093', 'liquidationPrice': '451.11', 'cumfilledSize': 138.17, 'uid': 100022, 'positionAmt': '-23.33', 'markPrice': '295.84', 'price': '296.19', 'tag': 'K', 'direction': 'SPACE', 'amount': 102.97, 'side': 'BUY', 'origQty': 161.5, 'positionSide': 'SHORT', 'updateTime': 1740551011410, 'userId': 53597862, 'market': 'BCH-USDT', 'entryPrice': '296.99', 'cumfilledVol': '40916.95', 'isAutoAddMargin': 'false', 'unRealizedProfit': '26.8295', 'marginType': 'full', 'lastfilledprice': '296.14', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'amount': 23.33, 'side': 'BUY', 'origQty': 161.5, 'cumfilledSize': 161.5, 'userId': 53597862, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '47825.20', 'price': '296.19', 'lastfilledprice': '', 'tag': 'K', 'status': 'PARTIALLY_FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '282.7548', 'liquidationPrice': '318.38', 'cumfilledSize': 57.45, 'uid': 100022, 'positionAmt': '-46.79', 'markPrice': '295.84', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 57.45, 'side': 'BUY', 'origQty': 104.24, 'positionSide': 'SHORT', 'updateTime': 1740551011582, 'userId': 89439747, 'market': 'BCH-USDT', 'entryPrice': '297.13', 'cumfilledVol': '17013.24', 'isAutoAddMargin': 'false', 'unRealizedProfit': '60.3591', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 13857.79, 'orderType': 'market', 'lastfilledSize': 46.79, 'amount': 46.79, 'side': 'BUY', 'origQty': 104.24, 'cumfilledSize': 104.24, 'userId': 89439747, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '30871.03', 'price': '', 'lastfilledprice': '296.17', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '752.0738', 'liquidationPrice': '330.95', 'cumfilledSize': 20.01, 'uid': 100022, 'positionAmt': '-126.68', 'markPrice': '295.82', 'price': '296.92', 'tag': 'K', 'direction': 'SPACE', 'amount': 20.01, 'side': 'BUY', 'origQty': 146.69, 'positionSide': 'SHORT', 'updateTime': 1740551014177, 'userId': 31611693, 'market': 'BCH-USDT', 'entryPrice': '297.08', 'cumfilledVol': '5939.76', 'isAutoAddMargin': 'false', 'unRealizedProfit': '159.6168', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '189.3839', 'liquidationPrice': '407.23', 'cumfilledSize': 164.2, 'uid': 100022, 'positionAmt': '-31.9', 'markPrice': '295.82', 'price': '296.96', 'tag': 'K', 'direction': 'SPACE', 'amount': 164.2, 'side': 'BUY', 'origQty': 196.1, 'positionSide': 'SHORT', 'updateTime': 1740551014213, 'userId': 68143850, 'market': 'BCH-USDT', 'entryPrice': '297.26', 'cumfilledVol': '48742.77', 'isAutoAddMargin': 'false', 'unRealizedProfit': '45.936', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 37604.95, 'orderType': 'limit', 'lastfilledSize': 126.68, 'amount': 126.68, 'side': 'BUY', 'origQty': 146.69, 'cumfilledSize': 146.69, 'userId': 31611693, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '43544.72', 'price': '296.92', 'lastfilledprice': '296.85', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': 9470.15, 'orderType': 'limit', 'lastfilledSize': 31.9, 'amount': 31.9, 'side': 'BUY', 'origQty': 196.1, 'cumfilledSize': 196.1, 'userId': 68143850, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '58212.92', 'price': '296.96', 'lastfilledprice': '296.87', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '769.5873', 'liquidationPrice': '320.51', 'cumfilledSize': 30.73, 'uid': 100022, 'positionAmt': '-129.63', 'markPrice': '295.82', 'price': '296.97', 'tag': 'K', 'direction': 'SPACE', 'amount': 30.73, 'side': 'BUY', 'origQty': 160.36, 'positionSide': 'SHORT', 'updateTime': 1740551014377, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.83', 'cumfilledVol': '9124.04', 'isAutoAddMargin': 'false', 'unRealizedProfit': '130.9263', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 50, 'lastfilledSize': '', 'isolatedMargin': '329.6705', 'liquidationPrice': '354.53', 'cumfilledSize': 104.83, 'uid': 100022, 'positionAmt': '-55.53', 'markPrice': '295.82', 'price': '296.97', 'tag': 'K', 'direction': 'SPACE', 'amount': 74.1, 'side': 'BUY', 'origQty': 160.36, 'positionSide': 'SHORT', 'updateTime': 1740551014425, 'userId': 34320263, 'market': 'BCH-USDT', 'entryPrice': '296.83', 'cumfilledVol': '31125.81', 'isAutoAddMargin': 'false', 'unRealizedProfit': '56.0853', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 16487.96, 'orderType': 'limit', 'lastfilledSize': 55.53, 'amount': 55.53, 'side': 'BUY', 'origQty': 160.36, 'cumfilledSize': 160.36, 'userId': 34320263, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '47613.78', 'price': '296.97', 'lastfilledprice': '296.92', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1068.5634', 'liquidationPrice': '318.70', 'cumfilledSize': 120.41, 'uid': 100022, 'positionAmt': '-176.39', 'markPrice': '295.82', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 120.41, 'side': 'BUY', 'origQty': 240.0, 'positionSide': 'SHORT', 'updateTime': 1740551014605, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.21', 'cumfilledVol': '35752.13', 'isAutoAddMargin': 'false', 'unRealizedProfit': '245.1821', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 35511.05, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 119.59, 'isolatedMargin': '344.0920', 'liquidationPrice': '371.46', 'cumfilledSize': 240.0, 'uid': 100022, 'positionAmt': '-56.8', 'markPrice': '295.82', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 119.59, 'side': 'BUY', 'origQty': 240.0, 'positionSide': 'SHORT', 'updateTime': 1740551014657, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.21', 'cumfilledVol': '71263.19', 'isAutoAddMargin': 'false', 'unRealizedProfit': '78.952', 'marginType': 'full', 'lastfilledprice': '296.94', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '242.8030', 'liquidationPrice': '403.33', 'cumfilledSize': 16.72, 'uid': 100022, 'positionAmt': '-40.08', 'markPrice': '295.82', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 16.72, 'side': 'BUY', 'origQty': 56.8, 'positionSide': 'SHORT', 'updateTime': 1740551014711, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.21', 'cumfilledVol': '4964.83', 'isAutoAddMargin': 'false', 'unRealizedProfit': '55.7112', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 3697.27, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 12.45, 'isolatedMargin': '167.3814', 'liquidationPrice': '453.01', 'cumfilledSize': 29.17, 'uid': 100022, 'positionAmt': '-27.63', 'markPrice': '295.82', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 12.45, 'side': 'BUY', 'origQty': 56.8, 'positionSide': 'SHORT', 'updateTime': 1740551014791, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.21', 'cumfilledVol': '8662.11', 'isAutoAddMargin': 'false', 'unRealizedProfit': '38.4057', 'marginType': 'full', 'lastfilledprice': '296.97', 'status': 'FILLED'},
                {'lastfilledVolume': '', 'orderType': 'market', 'lastfilledSize': '', 'amount': 27.63, 'side': 'BUY', 'origQty': 56.8, 'cumfilledSize': 56.8, 'userId': 76332266, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '16867.11', 'price': '', 'lastfilledprice': '', 'tag': 'K', 'status': 'PARTIALLY_FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '918.5077', 'liquidationPrice': '312.99', 'cumfilledSize': 81.96, 'uid': 100022, 'positionAmt': '-151.62', 'markPrice': '295.82', 'price': '296.91', 'tag': 'K', 'direction': 'SPACE', 'amount': 81.96, 'side': 'BUY', 'origQty': 233.58, 'positionSide': 'SHORT', 'updateTime': 1740551014848, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.35', 'cumfilledVol': '24334.74', 'isAutoAddMargin': 'false', 'unRealizedProfit': '231.9786', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '495.0564', 'liquidationPrice': '328.43', 'cumfilledSize': 151.86, 'uid': 100022, 'positionAmt': '-81.72', 'markPrice': '295.82', 'price': '296.91', 'tag': 'K', 'direction': 'SPACE', 'amount': 69.9, 'side': 'BUY', 'origQty': 233.58, 'positionSide': 'SHORT', 'updateTime': 1740551015309, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.35', 'cumfilledVol': '45088.75', 'isAutoAddMargin': 'false', 'unRealizedProfit': '125.0316', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '104.7062', 'liquidationPrice': '454.23', 'cumfilledSize': 216.3, 'uid': 100022, 'positionAmt': '-17.28', 'markPrice': '295.89', 'price': '296.91', 'tag': 'K', 'direction': 'SPACE', 'amount': 64.44, 'side': 'BUY', 'origQty': 233.58, 'positionSide': 'SHORT', 'updateTime': 1740551015823, 'userId': 69728812, 'market': 'BCH-USDT', 'entryPrice': '297.35', 'cumfilledVol': '64221.63', 'isAutoAddMargin': 'false', 'unRealizedProfit': '25.2288', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 5130.6, 'orderType': 'limit', 'lastfilledSize': 17.28, 'amount': 17.28, 'side': 'BUY', 'origQty': 233.58, 'cumfilledSize': 233.58, 'userId': 69728812, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '69352.23', 'price': '296.91', 'lastfilledprice': '296.91', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 49, 'lastfilledSize': '', 'isolatedMargin': '1232.0246', 'liquidationPrice': '312.61', 'cumfilledSize': 43.66, 'uid': 100022, 'positionAmt': '-203.51', 'markPrice': '296.62', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 43.66, 'side': 'BUY', 'origQty': 240.0, 'positionSide': 'SHORT', 'updateTime': 1740551018641, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.16', 'cumfilledVol': '12953.04', 'isAutoAddMargin': 'false', 'unRealizedProfit': '109.8954', 'marginType': 'full', 'lastfilledprice': '', 'status': 'PARTIALLY_FILLED'},
                {'lastfilledVolume': 58252.11, 'orderType': 'market', 'leverage': 49, 'lastfilledSize': 196.34, 'isolatedMargin': '43.4063', 'liquidationPrice': '828.39', 'cumfilledSize': 240.0, 'uid': 100022, 'positionAmt': '-7.17', 'markPrice': '296.62', 'price': '', 'tag': 'K', 'direction': 'SPACE', 'amount': 196.34, 'side': 'BUY', 'origQty': 240.0, 'positionSide': 'SHORT', 'updateTime': 1740551018693, 'userId': 47383786, 'market': 'BCH-USDT', 'entryPrice': '297.16', 'cumfilledVol': '71205.16', 'isAutoAddMargin': 'false', 'unRealizedProfit': '3.8718', 'marginType': 'full', 'lastfilledprice': '296.69', 'status': 'FILLED'},
                {'lastfilledVolume': 2127.26, 'orderType': 'market', 'lastfilledSize': 7.17, 'amount': 7.17, 'side': 'BUY', 'origQty': 7.17, 'cumfilledSize': 7.17, 'userId': 47383786, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '2127.26', 'price': '', 'lastfilledprice': '296.69', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'},
                {'lastfilledVolume': 2.97, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 0.01, 'isolatedMargin': '0.0606', 'liquidationPrice': '435085.92', 'cumfilledSize': 0.01, 'uid': 100022, 'positionAmt': '-0.01', 'markPrice': '296.80', 'price': '296.96', 'tag': 'K', 'direction': 'SPACE', 'amount': 0.01, 'side': 'SELL', 'origQty': 0.01, 'positionSide': 'SHORT', 'updateTime': 1740552793258, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.01', 'cumfilledVol': '2.97', 'isAutoAddMargin': 'false', 'unRealizedProfit': '0.0021', 'marginType': 'full', 'lastfilledprice': '297.01', 'status': 'FILLED'},
                {'lastfilledVolume': 2.97, 'orderType': 'market', 'lastfilledSize': 0.01, 'amount': 0.01, 'side': 'BUY', 'origQty': 0.01, 'cumfilledSize': 0.01, 'userId': 76332266, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '2.97', 'price': '', 'lastfilledprice': '297.28', 'tag': 'K', 'status': 'FILLED', 'direction': 'SPACE'}]
        
        for ws in temp_data:
            await self.on_adl(ws)
            await asyncio.sleep(0.2)
        
        # try:
        #     # 获取账户信息
        #     # balance = await self.okx_rest.fetch_balance()
        #     # print(f"ok账户权益:{balance}")
        #     # 获取外盘仓位
        #     hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
        #     self.log.info(f"ok账户持仓:{hedge_pos}")
        # except:
        #     self.log.error(f"风险验证报错 {traceback.format_exc()}")
        self.risk_clock = False
    
    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''
    
    async def handle_deals(self):
        self.log.info(f"对冲线程")
        if self.hedge_clock:    # 正在对冲中
            return
        self.hedge_clock = True
        self.t1 = time.time()*1000
        # 测试数据
        # self.deals_dict = {'ADA-USDT': [{'lastfilledVolume': 8.03, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 10, 'isolatedMargin': '0.4017', 'liquidationPrice': '0.76713', 'cumfilledSize': 10, 'uid': 100022, 'positionAmt': '+10', 'markPrice': '0.80296', 'price': '0.80347', 'tag': 'J', 'direction': 'MANY', 'side': 'SELL', 'origQty': 10, 'positionSide': 'LONG', 'updateTime': 1740135870195, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80347', 'cumfilledVol': '8.03', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0051', 'marginType': 'isolated', 'lastfilledprice': '0.80347', 'status': 'FILLED'},]}
        matchs = self.deals_dict.copy()
        self.log.info(f"要对冲的数据{matchs}")
        for symbol, match in matchs.items():
            # 按交易对并行处理对冲
            if len(match) != 0:
                try:
                    await self.agg_deals(symbol, match)
                except:
                    self.log.error(f"agg报错记录,为了防止多线程警告 {traceback.format_exc()}")
        self.hedge_clock = False
    
    # 聚合
    async def agg_deals(self, symbol, matchs):
        match_amt = 0   # 计算聚合成交金额
        match_size = 0  # 计算聚合成交量
        length = len(matchs)    # 聚合的订单数量
        deals_mess = ''
        for match in matchs:
            temp = match
            deals_mess += str(match)+'\n'
            side = match['side']    # 方向
            deal_amt = float(match['cumfilledVol']) if side == 'BUY' \
                 else -float(match['cumfilledVol'])  # 成交金额
            match_amt += deal_amt
            # deal_size = float(match['origQty']) if side == 'BUY' \
            #       else -float(match['origQty'])  # 成交数量
            deal_size = float(match['amount']) if side == 'BUY' \
                  else -float(match['amount'])  # 成交数量
            match_size += deal_size
            self.log.info(f"单次成交:{deal_size} 累计成交:{match_size} length:{length}")

            # 调整对冲量
            match_size = match_size*getattr(self, 'follow_ratio', 1)

            # 下单
            try:
                await self.hedge(symbol, match_size)
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]  # 已完全对冲后,删除已对冲部分的订单
                self.log.info(f"对冲后的数据{self.deals_dict[symbol]}")
                self.hedge_clock = False
                self.log.info(f"对冲耗时:{time.time() * 1000 - self.t1}ms")
            except KeyboardInterrupt as e:   # 修改
                self.hedge_clock = False
                self.log.error(f"手动停止 {traceback.format_exc()}")
                raise e
            except:
                temp['side'] = 'BUY' if self.thisVol > 0 else 'SELL'
                temp_amt = abs(self.thisVol)*float(temp['lastfilledprice'])
                temp['amount'] = abs(self.thisVol)
                temp['cumfilledVol'] = temp_amt
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]
                self.deals_dict[symbol].append(temp)
                self.hedge_clock = False
                self.log.error((f'此条pendingTask处理失败! {traceback.format_exc()}'))
                tb.warning(f"演示策略{symbol}合约对冲策略hedge报错:hedge函数报错信息:{traceback.format_exc()}", 'risk')
                tb.sendmail(f'演示策略{symbol}合约对冲策略hedge报错', f"hedge函数报错信息:{traceback.format_exc()}")
    
    # 对冲
    async def hedge(self, symbol, vol):
        tempLog = ''
        hedgeMess = ''
        
        # 更新交易对精度
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            mess = f"{symbol}在ok没有获取到交易对信息,无法对冲,立即人工介入"
            self.log.warning(mess)
            tb.warning(f'26号演示ok对冲策略:合约对冲失败,无法获取交易对信息','risk')
            tb.sendmail('合约对冲失败,无法获取交易对信息', mess)
            self.deals_dict[symbol] = []
            return

        vol = round(vol, amount_precision)
        hedgeMess += f"需要外盘对冲 {vol}\n"
        hedge_vol = 0   # 已对冲数量
        while 1:
            thisVol = round(vol - hedge_vol, amount_precision) if amount_precision > 0 else int(
                vol - hedge_vol)
            self.thisVol = thisVol
            hedgeMess += f"下单量{thisVol} 总成交量{hedge_vol}\n"
            thisVol = round(thisVol, amount_precision)
            if vol == 0.0:
                self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
                return
            depth = await self.okx_rest.fetch_depth(symbol)
            bid = depth['bids'][0][0]
            ask = depth['asks'][0][0]

            if vol > 0:
                bidPrice = round(ask*(1+self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{bidPrice}  对冲数量:{thisVol}  方向:buy\n"
                self.log.info(f"最终下单参数:{hedgeMess}")
                return
                t11 = time.time()*1000
                # result = await self.okx_rest.create_order(symbol=symbol, side='BUY', orderType=ocb.OrderType.LIMIT, 
                #                                         amount=abs(thisVol), price=bidPrice)
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='buy', amount=abs(thisVol), 
                                                          price=bidPrice, tdMode='cross')
                tempLog += f"buy下单延时:{round(time.time()*1000-t11, 2)}ms  "
            elif vol < 0:
                askPrice = round(bid*(1-self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{askPrice}  对冲数量:{thisVol}  方向:sell\n"
                self.log.info(f'最终下单参数:{hedgeMess}')
                return
                t11 = time.time()*1000
                # result = await self.okx_rest.create_order(symbol=symbol, side='SELL', orderType=ocb.OrderType.LIMIT, 
                #                                         amount=abs(thisVol), price=askPrice)
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='sell', amount=abs(thisVol), 
                                                          price=askPrice, tdMode='cross')
                tempLog += f"sell下单延时:{round(time.time()*1000-t11, 2)}ms  "
            {'symbol': 'ADA-USDT', 'result': True, 'id': '2269739899423547392', 'clientOrderId': '', 'timestamp': 1740145920218, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2269739899423547392', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1740145920218'}], 'inTime': '1740145920217640', 'msg': '', 'outTime': '1740145920219807'}}
            hedge_order_id = result['id']
            self.log.info(f"对冲下单 {result} 延时:{round(time.time()*1000-t11, 2)}ms hedge_order_id:{hedge_order_id}")
            
            t33 = time.time()*1000
            try:
                cancelRes = await self.okx_rest.cancel_order(symbol, order_id=hedge_order_id)
                self.log.info(f"撤单 {cancelRes}")
            except:
                self.log.error(f"撤单报错{traceback.format_exc()}")
            tempLog += f"撤单延时:{round(time.time()*1000-t33, 2)}ms  "
            
            t44 = time.time()*1000
            while 1:  # 等待ws回调函数到达
                # 查询接口
                res = await self.okx_rest.fetch_order_detail(symbol, order_id=hedge_order_id)
                self.log.info(f"对冲订单状态:{res}")
                if res['status'] in ['closed', 'canceled', 'rejected']:
                    hedge_vol += res['filled'] if res['side'] == 'buy' else -res['filled']
                    hedge_vol = round(hedge_vol*face_value, amount_precision)
                    break
                await asyncio.sleep(0.5)

            tempLog += f"验证订单状态耗时:{round(time.time()*1000-t44, 2)}ms  "
            t55 = time.time()*1000
            self.log.info(f"{symbol}已对冲量:{hedge_vol}, 需要对冲总量:{vol}")
            if abs(hedge_vol) >= abs(vol):
                self.log.info(f"{symbol}对冲完全成交")
                tempLog += f"循环后跳出前耗时:{round(time.time()*1000-t55, 2)}ms\n"
                break
            await asyncio.sleep(0.5)
        self.log.info(tempLog)        


def main(path='tag_user_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()
    while True:
        time.sleep(999999)



if __name__ == '__main__':
    main()
