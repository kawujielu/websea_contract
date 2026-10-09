'''
    连接行情中心的demo
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

        # okx私有数据推送
        #channel_pos = f"contract.position.okex"
        #self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        #self.log.info("subscribe: okex持仓")
        #channel_pos = f"contract.balance.okex"
        #self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        #self.log.info("subscribe: okex权益")
        #channel_pos = f"contract.order.okex"
        #self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        #self.log.info("subscribe: okex成交")

        # websea私有数据成交推送
        channel_pos = f"contract.order.websea.{self.symbol}.44c614fa9131929291e4fa7325d47653000"
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        self.log.info("subscribe: websea成交")

        # 成功
        #channel_depth = [f"contract.depth.{self.symbol}.websea", f"contract.depth.{self.symbol}.binance", f"contract.depth.{self.symbol}.okex", f"contract.bids_asks.{self.symbol}"] # okex,binance
        #self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_depth, callback=self.on_bn_message))
        #self.log.info("subscribe: 深度数据")

        # 成功,最优挂单是多家交易所聚合的
        # channel_asks_bids = f"contract.bids_asks.{self.symbol}"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_asks_bids, callback=self.on_bn_message))
        # self.log.info("subscribe: 最优挂单")

        # 成功
        # channel_kline = [f"contract.kline.1m.{self.symbol}.binance", f"contract.kline.1m.{self.symbol}.okex"]  # okex
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_kline, callback=self.on_bn_message))
        # self.log.info("subscribe: binance K线数据")

        # 
        # channel_pos = f"contract.balance.binance"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: binance权益")

        # # 
        # channel_pos = f"contract.position.binance"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: binance持仓")

        # # 
        # channel_pos = f"contract.trade.{self.symbol}.binance"
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_bn_message))
        # self.log.info("subscribe: binance成交")
        

    async def on_bn_message(self, channel: str, item: dict):
        """
        contract.depth.ETH-USDT.websea {'symbol': 'ETH-USDT', 'asks': [[2105.73, 2127.0], [2105.75, 1645.0], [2105.77, 3986.0], [2105.91, 13588.0], [2105.99, 3353.0]], 'bids': [[2105.69, 6242.0], [2105.65, 3602.0], [2105.59, 3306.0], [2105.52, 3467.0], [2105.44, 6695.0]], 'timestamp': 1741066525707, 'messageType': 'message'}
        contract.bids_asks.ETH-USDT {'symbol': 'ETH-USDT', 'ask': 2105.31, 'askVolume': 22.915, 'bid': 2105.3, 'bidVolume': 20.644, 'timestamp': 1741066525674, 'messageType': 'message', 'ratio': {'binance': 1, 'bybit': 0, 'okex': 0}}

        """
        print(channel, item)
        if channel.endswith("okex"):
            self.ok_ts = item['timestamp']
        elif channel.endswith("binance"):
            self.binance_ts = item['timestamp']
        elif channel.endswith("websea"):
            self.websea_ts = item['timestamp']
        elif 'bids_asks' in channel:
            self.bid_ask_ts = item['timestamp']
        
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

