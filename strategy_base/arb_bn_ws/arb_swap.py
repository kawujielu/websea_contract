'''
    bn和websea套利策略,合约
'''
import sys
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
import time
import traceback
import importlib
import asyncio
import pytz
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
import objects.contract_request.websea as ocw
from client.env_pro.rest.websea.contract import WebseaContract as old_WebseaContract
from crypto_center.client.rest.websea.contract_quan import WebseaContract     # 新接口做下单



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, symbol, config):
        super().__init__(scheduler=True, gcc=True)
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()
        self.symbol = symbol
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
    
    async def on_first(self):
        self.log.info("=======first======")
        self.log.add(f"swap_log/{self.symbol.split('-')[0].lower()}_log.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(0.5)
        await self.redis_conn.sub_channel(f"contract.bids_asks.{self.symbol}.binance")
        # {'exchange': 'okex', 'symbol': 'ETH-USDT', 'ask': 2392.01, 'askVolume': 6142.39, 'bid': 2392, 'bidVolume': 395.44, 'timestamp': 1776875634666, 'messageType': 'message', 'info': {'arg': {'channel': 'tickers', 'instId': 'ETH-USDT-SWAP'}, 'data': [{'askPx': '2392.01', 'askSz': '6142.39', 'bidPx': '2392', 'bidSz': '395.44', 'high24h': '2422.79', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'last': '2392.01', 'lastSz': '0.14', 'low24h': '2282', 'open24h': '2316.3', 'sodUtc0': '2326.2', 'sodUtc8': '2402.76', 'ts': '1776875634666', 'vol24h': '46355958.82', 'volCcy24h': '4635595.882'}]}}
        await self.redis_conn.sub_channel(f"contract.depth.{self.symbol}.websea")
        print(f"contract.depth.{self.symbol}.websea")
        # await self.redis_conn.sub_channel(f"contract.bids_asks.{self.symbol}")
        
        await self.get_precision()          # 内盘精度
        await self.get_symbol_unit()        # 内盘合约单位
        # await self.risk()

    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.main, CronTrigger(second="*/1"))  # 每1s执行一次
        # self.schedule.add_job(self.risk, CronTrigger(second="*/30"))         # 每10s执行一次
        # self.schedule.add_job(self.reload_config, CronTrigger(second="00"))  # 每分钟的0s执行

    def _load_config(self, config):
        """读取配置文件
        """
        self.init_config = config
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rest = WebseaContract(config['base_token'], config['base_secret'])
        self.old_rest = old_WebseaContract(config['base_token'], config['base_secret'])
        
    def _initParams(self):
        """初始化参数
        """
        self.deal_amt = 0   # 下单金额
        pass
                
    async def get_precision(self):
        """更新 币对信息"""
        self.precision = await self.old_rest.get_precision(self.symbol, quan=True)
        self.log.info(f"币对信息:{self.precision}")
        self.min_price_step = 10 ** (-self.precision.price)
        if self.symbol == "BTC-USDT":
            self.min_price_step = 0.1
    
    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                data = await self.old_rest.get_symbols(symbol=self.symbol, quan=True)
                self.log.info(f"合约单位:{data}")
                # 若走这个逻辑,表示交易对下架
                if not isinstance(data, list):
                    self.symbol_unit = data.contract_size
                    break
                else:
                    raise f'{self.symbol}交易对已下架,获取不到合约单位'
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    async def reload_config(self):
        try:
            importlib.reload(self.init_config)
            [setattr(self, k, v) for k, v in vars(self.init_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"reload_config报错{traceback.format_exc()}")
            except:
                pass
            
    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    # websea深度
    async def on_message(self, items, content):
        self.log.info(f"on_message:items:{items} content:{content}")
        try:
            if 'depth' in items:
                # {'exchange': 'websea', 'symbol': 'ETH-USDT', 
                # 'asks': 
                # [[4474, 5100], [4474.03, 10167], [4474.06, 14538], [4474.08, 6163], [4474.1, 5300]], 
                # 'bids': 
                # [[4473.98, 19037], [4473.97, 14834], [4473.96, 31255], [4473.95, 28668], [4473.94, 27719]], 
                # 'timestamp': 1756400699832, 'messageType': 'message'}
                self.ws_depth = [content['bids'], content['asks']]
                # websea盘口变化触发判断
                await asyncio.gather(self.main())
                
            elif 'bids_asks' in items and content['messageType'] == 'message':
                # {'exchange': 'aggregation', 'symbol': 'ETH-USDT', 'ask': 4474.168, 'askVolume': 108.985, 
                # 'bid': 4474.158, 'bidVolume': 17.976, 'timestamp': 1756400699720, 'messageType': 'message', 
                # 'info': {'data': {'A': '108.985', 'B': '17.976', 'E': 1756400699720, 'T': 1756400699720, 
                #                 'a': '4474.18', 'b': '4474.17', 'e': 'bookTicker', 's': 'ETHUSDT', 
                #                 'u': 8462813660441}, 'stream': 'ethusdt@bookTicker'}, 'ratio': {'okex': 0.15, 'bybit': 0.15, 'binance': 0.7}}
                self.bid_one_price = content['bid']
                self.ask_one_price = content['ask']
        except:
            self.log.info(f"报错内容:{traceback.format_exc()} items:{items} content:{content}")
            
    # 检查wss推送是否正常
    async def check_wss(self):
        # 外盘数据
        if time.time() - self.bn_bid_ask_ts/1000 > 30:
            mess = f"on_bn_bid_ask数据wss推送异常,重新订阅"
            await self.bn_wss.sub_kline(self.symbol, '1m')
            tb.warning(mess, 'risk')
            # tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)
        # 内盘数据
        if time.time() - self.last_on_depth_ts/1000 > 30:
            mess = f"压盘口策略{self.symbol},on_depth数据wss推送异常,重新订阅"
            await self.ws_wss.sub_depth(symbol=self.symbol)
            tb.warning(mess, 'risk')
            # tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)
        
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''
    async def risk(self):
        try:
            # 做市账户持仓
            symbol_pos = {'buy': 0,'sell': 0}
            pos = await self.old_rest.get_position(symbol=self.symbol, quan=True)
            for i in pos:
                if i.type == 1:      # 多单
                    symbol_pos['buy'] += i.amount
                elif i.type == 2:    # 空单
                    symbol_pos['sell'] -= i.amount
            for k, v in symbol_pos.items():
                symbol_pos[k] = v*self.symbol_unit    # TODO 测试合约单位
            self.symbol_pos = symbol_pos
            self.log.info(f"账户总持仓:\n{symbol_pos}")
        except Exception as e:
            self.log.warning(f"风控异常:{traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        t1 = time.time()
        # 拿binance和websea盘口数据,按fee计算有套利空间的铺单,直接吃掉
        bn_ask = self.ask_one_price*(1+self.fee)
        bn_bid = self.bid_one_price*(1-self.fee)
        bid_price = 0
        bid_vol = 0
        for bp in self.ws_depth[0]:
            con = bp[0] > bn_ask
            if con:
                bid_vol += bp[1]
                bid_price = bp[0]
        ask_price = 0
        ask_vol = 0
        for ap in self.ws_depth[1]:
            con = ap[0] < bn_bid
            if con:
                ask_vol += ap[1]
                ask_price = ap[0]
        if bid_price != 0:
            side = 'sell'
            pos = self.symbol_pos['buy']
            close = True if bid_vol*100 < pos else False
            if self.deal_amt < -self.amt_limit:
                self.log.info(f"ask定价有套利空间,因金额打满未下单")
                return
            # await self.make_order(bid_price, bid_vol, side, close)
            self.deal_amt -= bid_vol*bid_price
        elif ask_price != 0:
            side = 'buy'
            pos = self.symbol_pos['sell']
            close = True if ask_vol*100 < pos else False
            if self.deal_amt > self.amt_limit:
                self.log.info(f"bid定价有套利空间,因金额打满未下单")
                return
            # await self.make_order(ask_price, ask_vol, side, close)
            self.deal_amt += ask_vol*ask_price
        t2 = time.time()
        ts = int((t2-t1)*1000000)
        self.log.info(f"调用main函数耗时:{ts}μs bid_price:{bid_price} bid_vol:{bid_vol} ask_price:{ask_price} ask_vol:{ask_vol}")
        
    async def make_order(self, price, vol, side, close):
        return
        try:
            od_type = 'buy-limit' if side == 'buy' else 'sell-limit'
            vol = int(vol*self.symbol_unit) # TODO 测试
            self.log.info(f"下单信息:{side} 价:{price} 量:{vol} 是否平仓:{close}")
            if side == 'buy':
                if close:
                    res = await self.rest.create_order(self.symbol, order_type=od_type, \
                                                    price=price, amount=vol, side=side,\
                                                    precision=self.precision, \
                                                    contract_type='close', quan=True)
                else:
                    res = await self.rest.create_order(self.symbol, order_type=od_type, \
                                                price=price, amount=vol, side=side, \
                                                precision=self.precision, quan=True)
            elif side == 'sell':
                if close:
                    res = await self.rest.create_order(self.symbol, order_type=od_type, \
                                                    price=price, amount=vol, side=side, \
                                                    precision=self.precision, \
                                                    contract_type='close', quan=True)
                else:
                    res = await self.rest.create_order(self.symbol, order_type=od_type, \
                                                price=price, amount=vol, side=side, \
                                                precision=self.precision, quan=True)
            self.log.info(f"开仓信息:{side} 价:{price} 量:{vol} 是否平仓:{close} 回报:{res}")
        except Exception as e:
            self.log.warning(f"{self.symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")
    



def main(symbol, path='arb_swap_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(symbol, config).run()


if __name__ == '__main__':
    main(sys.argv[1])


