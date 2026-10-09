'''
    连接行情中心的demo
    所有内容测试成功
    等内盘wss订单推送完成后,对策略进行全面改版
'''
import sys
import time
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from template.template_timer import TemplateTimer, CronTrigger
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools

class data_demo(TemplateTimer):
    """铺单"""
    def __init__(self, symbol: str, debug: bool = False):
        super().__init__(scheduler=True, gcc=True)
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.symbol = 'ETH-USDT'
        self.db = 1

    async def on_first(self):
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()

        # 成功
        channel_depth = [f"contract.depth.{self.symbol}.websea"]  #, f"contract.depth.{self.symbol}.binance", f"contract.depth.{self.symbol}.okex", f"contract.bids_asks.{self.symbol}"] # okex,binance
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_depth, callback=self.on_bn_message))
        self.log.info("subscribe: 深度数据")

        # 成功,最优挂单是多家交易所聚合的
        # channel_asks_bids = f"contract.bids_asks.{self.symbol}"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_asks_bids, callback=self.on_bn_message))
        # self.log.info("subscribe: 最优挂单")

        # 成功
        # channel_kline = [f"contract.kline.1m.{self.symbol}.binance", f"contract.kline.1m.{self.symbol}.okex"]  # okex
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_kline, callback=self.on_bn_message))
        # self.log.info("subscribe: binance K线数据")

        # 底层没有跑起来
        # channel_kline = [f"contract.trade.{self.symbol}.binance", f"contract.trade.{self.symbol}.okex"]  # okex
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_kline, callback=self.on_bn_message))
        # self.log.info("subscribe: 成交数据")

        # 权益、持仓、成交都没问题
        # channel_pos = [f"contract.balance.okex", f"contract.position.okex", f"contract.order.okex"]
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: binance权益")

        # 内盘私有成交数据推送
        # channel_pos = f"contract.order.websea.{self.symbol}.44c614fa9131929291e4fa7325d47653000"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: websea成交")
        
        # okx私有数据推送
        # channel_pos = f"contract.position.okex"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: okex持仓")
        # channel_pos = f"contract.balance.okex"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: okex权益")
        # channel_pos = f"contract.order.okex"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: okex成交")


    async def on_bn_message(self, channel: str, item: dict):
        """
        contract.depth.ETH-USDT.websea {'symbol': 'ETH-USDT', 'asks': [[2105.73, 2127.0], [2105.75, 1645.0], [2105.77, 3986.0], [2105.91, 13588.0], [2105.99, 3353.0]], 'bids': [[2105.69, 6242.0], [2105.65, 3602.0], [2105.59, 3306.0], [2105.52, 3467.0], [2105.44, 6695.0]], 'timestamp': 1741066525707, 'messageType': 'message'}
        contract.bids_asks.ETH-USDT {'symbol': 'ETH-USDT', 'ask': 2105.31, 'askVolume': 22.915, 'bid': 2105.3, 'bidVolume': 20.644, 'timestamp': 1741066525674, 'messageType': 'message', 'ratio': {'binance': 1, 'bybit': 0, 'okex': 0}}
        
        contract.balance.okex {'balance': {'USDT': {'currency': 'USDT', 'free': None, 'used': None, 'total': 63231.83395632147, 'timestamp': 1741067742114}}, 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.8339563214709507', 'ccy': 'USDT', 'uTime': '1741067742114'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '1.22', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903030', 'uTime': '1741067742114'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903030'}]}]}}
        contract.position.okex {'position': [{'symbol': 'ETH-USDT', 'id': '2280462420233936896', 'contracts': 1.22, 'contractSize': None, 'side': 'net', 'entryPrice': 2110.05, 'markPrice': None, 'notional': None, 'leverage': None, 'unrealizedPnl': None, 'realizedPnl': None, 'marginMode': 'crossed', 'marginRatio': None, 'initialMargin': None, 'maintenanceMargin': None, 'stopLossPrice': None, 'takeProfitPrice': None, 'timestamp': 1741067742114}], 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.8339563214709507', 'ccy': 'USDT', 'uTime': '1741067742114'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '1.22', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903030', 'uTime': '1741067742114'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903030'}]}]}}
        contract.balance.okex {'balance': {'USDT': {'currency': 'USDT', 'free': None, 'used': None, 'total': 63231.77867301147, 'timestamp': 1741067742115}}, 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.7786730114709507', 'ccy': 'USDT', 'uTime': '1741067742115'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '2.53', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903031', 'uTime': '1741067742115'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903031'}]}]}}
        contract.position.okex {'position': [{'symbol': 'ETH-USDT', 'id': '2280462420233936896', 'contracts': 2.53, 'contractSize': None, 'side': 'net', 'entryPrice': 2110.05, 'markPrice': None, 'notional': None, 'leverage': None, 'unrealizedPnl': None, 'realizedPnl': None, 'marginMode': 'crossed', 'marginRatio': None, 'initialMargin': None, 'maintenanceMargin': None, 'stopLossPrice': None, 'takeProfitPrice': None, 'timestamp': 1741067742115}], 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.7786730114709507', 'ccy': 'USDT', 'uTime': '1741067742115'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '2.53', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903031', 'uTime': '1741067742115'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903031'}]}]}}
        contract.balance.okex {'balance': {'USDT': {'currency': 'USDT', 'free': None, 'used': None, 'total': 63231.46343154147, 'timestamp': 1741067742115}}, 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.4634315414709507', 'ccy': 'USDT', 'uTime': '1741067742115'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '10', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903032', 'uTime': '1741067742115'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903032'}]}]}}
        contract.position.okex {'position': [{'symbol': 'ETH-USDT', 'id': '2280462420233936896', 'contracts': 10.0, 'contractSize': None, 'side': 'net', 'entryPrice': 2110.05, 'markPrice': None, 'notional': None, 'leverage': None, 'unrealizedPnl': None, 'realizedPnl': None, 'marginMode': 'crossed', 'marginRatio': None, 'initialMargin': None, 'maintenanceMargin': None, 'stopLossPrice': None, 'takeProfitPrice': None, 'timestamp': 1741067742115}], 'timestamp': 1741067742115, 'messageType': 'message', 'info': {'arg': {'channel': 'balance_and_position', 'uid': '678147541502075525'}, 'data': [{'balData': [{'cashBal': '63231.4634315414709507', 'ccy': 'USDT', 'uTime': '1741067742115'}], 'eventType': 'filled', 'pTime': '1741067742115', 'posData': [{'avgPx': '2110.05', 'baseBal': '', 'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'mgnMode': 'cross', 'pos': '10', 'posCcy': '', 'posId': '2280462420233936896', 'posSide': 'net', 'quoteBal': '', 'tradeId': '1794903032', 'uTime': '1741067742115'}], 'trades': [{'instId': 'ETH-USDT-SWAP', 'tradeId': '1794903032'}]}]}}
        
        contract.order.okex {'symbol': 'ETH-USDT', 'id': '2300721341674545152', 'clientOrderId': '', 'price': 2090.0, 'stopPrice': None, 'triggerPrice': None, 'amount': 10.0, 'amountIsNum': True, 'side': 'sell', 'type': 'limit', 'status': 'closed', 'leverage': 10.0, 'timeInForce': None, 'postOnly': None, 'reduceOnly': False, 'marginMode': 'crossed', 'average': 2096.37, 'filled': 10.0, 'cost': None, 'remaining': 0.0, 'takeProfitPrice': None, 'stopLossPrice': None, 
            'fee': {'feeCurrency': 'USDT', 'cost': -0.419274}, 'timestamp': 1741069239150, 'messageType': 'message', 
            'info': {'arg': {'channel': 'orders', 'instType': 'SWAP', 'uid': '678147541502075525'}, 
                     'data': [{'instType': 'SWAP', 'instId': 'ETH-USDT-SWAP', 'tgtCcy': '', 'ccy': '', 'ordId': '2300721341674545152', 'clOrdId': '', 'algoClOrdId': '', 'algoId': '', 'tag': '', 'px': '2090', 'sz': '10', 'notionalUsd': '2095.0283232', 'ordType': 'limit', 'side': 'sell', 'posSide': 'net', 'tdMode': 'cross', 'accFillSz': '10', 'fillNotionalUsd': '2095.0283232', 'avgPx': '2096.37', 'state': 'filled', 'lever': '10', 'pnl': '-13.68', 'feeCcy': 'USDT', 'fee': '-0.419274', 'rebateCcy': 'USDT', 'rebate': '0', 'category': 'normal', 'uTime': '1741069239150', 'cTime': '1741069239148', 'source': '', 'reduceOnly': 'false', 'cancelSource': '', 'quickMgnType': '', 'stpId': '', 'stpMode': 'cancel_maker', 'attachAlgoClOrdId': '', 'lastPx': '2096.37', 'isTpLimit': 'false', 'slTriggerPx': '', 'slTriggerPxType': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'slOrdPx': '', 'fillPx': '2096.37', 'tradeId': '1794953643', 'fillSz': '10', 'fillTime': '1741069239149', 'fillPnl': '-13.68', 'fillFee': '-0.419274', 'fillFeeCcy': 'USDT', 'execType': 'T', 'fillPxVol': '', 'fillPxUsd': '', 'fillMarkVol': '', 'fillFwdPx': '', 'fillMarkPx': '2096.5', 'amendSource': '', 'reqId': '', 'amendResult': '', 'code': '0', 'msg': '', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'linkedAlgoOrd': {'algoId': ''}, 'attachAlgoOrds': []}]}}
        """
        print(channel, item)
        # if channel.endswith("okex"):
        #     self.ok_ts = item['timestamp']
        # elif channel.endswith("binance"):
        #     self.binance_ts = item['timestamp']
        # elif channel.endswith("websea"):
        #     self.websea_ts = item['timestamp']
        # elif 'bids_asks' in channel:
        #     self.bid_ask_ts = item['timestamp']
        
    async def check_ts(self):
        sys_ts = int(time.time()*1000)
        print(f"okex延时:{sys_ts - self.ok_ts} binance延时:{sys_ts - self.binance_ts} websea延时:{sys_ts - self.websea_ts} bid_ask延时:{sys_ts - self.bid_ask_ts}")
        if sys_ts - self.ok_ts > 10000:
            self.log.info("okex depth推送超时")
        if sys_ts - self.binance_ts > 10000:
            self.log.info("binance depth推送超时")
        if sys_ts - self.websea_ts > 10000:
            self.log.info("websea depth推送超时")
        if sys_ts - self.bid_ask_ts > 10000:
            self.log.info("bid_ask 推送超时")

    
    async def on_timer(self):
        self.log.info("=====timer====")
        # self.schedule.add_job(self.check_ts, CronTrigger(second="*/5"))

if __name__ == "__main__":
    try:
        sys.argv[2]
    except IndexError:
        db = False
    else:
        db = True
    print(sys.argv[1], db)
    data_demo(symbol=sys.argv[1], debug=db).run()

