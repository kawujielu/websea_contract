'''
    bn和websea套利策略,现货
'''
import sys
sys.path.append("/usr/local/server/wbfAPI/exchange")
from binanceSpot import DataWss, AccountRest
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
import time
import traceback
import importlib
import asyncio
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
import objects.contract_request.websea as ocw
from client.env_pro.rest.websea.contract import WebseaContract as old_WebseaContract
# from crypto_center.client.rest.websea.contract_quan import WebseaContract     # 新接口做下单
from client.env_pro.rest.websea.spot import WebseaSpot
from client.env_pro.wss.websea.spot import WebSeaSpot as ws_spot_wss



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, symbol, config):
        super().__init__(scheduler=True, gcc=True)
        self.rc_task = rc.RestClient()
        self.symbol = symbol
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
    
    async def on_first(self):
        self.log.info("=======first======")
        self.log.add(f"spot_log/{self.symbol.split('-')[0].lower()}_log.log", rotation="100 MB", retention=10)
        symbol = self.symbol.replace('-', '/').lower()
        DataWss(symbol, topics=['bidAsk'], rspFunc=self.on_bid_ask)
        self.ws_spot_wss = ws_spot_wss()
        self.ws_spot_wss.on_depth = self.spot_on_depth
        self.loop.create_task(self.ws_spot_wss.only_subscribe())
        self.loop.create_task(self.ws_spot_wss.sub_depth(symbol=self.symbol))
        
        await self.get_precision()          # 内盘精度
        # await self.risk()

    async def on_timer(self):
        # self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/1"))  # 每1s执行一次
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
        self.rest = WebseaSpot(config['base_token'], config['base_secret'])
        # self.old_rest = old_WebseaContract(config['base_token'], config['base_secret'])
        
    def _initParams(self):
        """初始化参数
        """
        self.deal_amt = 0   # 下单金额
        self.buy_num = 0    # 买入次数
        self.sell_num = 0   # 卖出次数
        pass
                
    async def get_precision(self):
        """更新 币对信息"""
        self.precision = await self.rest.get_precision(self.symbol)
        self.log.info(f"币对信息:{self.precision}")
        self.precision = self.precision[self.symbol]

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
    def on_bid_ask(self, content):
        # print(f"bid_ask:{content}")
        self.bid_one_price = content[1][0][0]
        self.ask_one_price = content[2][0][0]
    
    async def spot_on_depth(self, content):
        print(content)
        if content['bids'] != []:
            spot_bid = [[float(i['price']), float(i['number'])] for i in content['bids']]
        else:
            spot_bid = []
        if content['asks'] != []:
            spot_ask = [[float(i['price']), float(i['number'])] for i in content['asks']]
        else:
            spot_ask = []
        self.ws_depth = [spot_bid, spot_ask]
        
    # websea深度
    async def get_depth(self):
        try:
            depth = await self.rest.get_depth(self.symbol)
            # print(f"websea深度:{depth}")
            bids = [[float(i[0]), float(i[1])] for i in depth['bids']]
            asks = [[float(i[0]), float(i[1])] for i in depth['asks']]
            self.ws_depth = [bids, asks]
            # print(f"websea深度:{self.ws_depth}")
        except:
            self.log.info(f"报错内容:{traceback.format_exc()}")
            
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''
    async def risk(self):
        pass
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        t1 = time.time()
        # 拿binance和websea盘口数据,按fee计算有套利空间的铺单,直接吃掉
        # await self.get_depth()
        bn_ask = self.ask_one_price*(1+self.fee)
        bn_bid = self.bid_one_price*(1-self.fee)
        bid_price = 0
        bid_vol = 0
        for bp in self.ws_depth[0]:
            con = bp[0] > bn_ask
            if con:
                # self.log.info(f"bp:{bp[0]} {bp[1]} bn_ask:{bn_ask} bn一档:{self.ask_one_price} buy符合套利条件")
                bid_vol += bp[1]
                bid_price = bp[0]
        ask_price = 0
        ask_vol = 0
        for ap in self.ws_depth[1]:
            con = ap[0] < bn_bid
            if con:
                # self.log.info(f"ap:{ap[0]} {ap[1]} bn_bid:{bn_bid}  bn一档:{self.bid_one_price} sell符合套利条件")
                ask_vol += ap[1]
                ask_price = ap[0]
        if bid_price != 0:
            side = 'sell'
            self.buy_num += 1   # bid定价高
            if self.deal_amt < -self.amt_limit:
                self.log.info(f"ask定价有套利空间,因金额打满未下单")
                return
            bid_vol = min(bid_vol, self.amt_limit/bid_price)
            await self.make_order(bid_price, bid_vol, side)
            self.deal_amt -= bid_vol*bid_price
        elif ask_price != 0:
            side = 'buy'
            self.sell_num += 1  # ask定价低
            if self.deal_amt > self.amt_limit:
                self.log.info(f"bid定价有套利空间,因金额打满未下单")
                return
            ask_vol = min(ask_vol, self.amt_limit/ask_price)
            await self.make_order(ask_price, ask_vol, side)
            self.deal_amt += ask_vol*ask_price
        t2 = time.time()
        ts = int((t2-t1)*1000)
        self.log.info(f"调用main函数耗时:{ts}ms bid_price:{bid_price} bid_vol:{bid_vol} ask_price:{ask_price} ask_vol:{ask_vol}")
        
    async def make_order(self, price, vol, side):
        try:
            order_type = 'buy-limit' if side == 'buy' else 'sell-limit'
            # vol = round(vol, self.precision[self.symbol]['amount'])
            self.log.info(f"下单信息:{side} 价:{price} 量:{vol} sell套利{self.sell_num}次 buy套利{self.buy_num}次")
            # if side == 'buy':
            #     res = await self.rest.order_create(self.symbol, order_type=order_type, \
            #                                     price=price, amount=vol, \
            #                                     precision=self.precision)
            # elif side == 'sell':
            #     res = await self.rest.order_create(self.symbol, order_type=order_type, \
            #                                     price=price, amount=vol, \
            #                                     precision=self.precision)
            # self.log.info(f"下单信息2:{side} 价:{price} 量:{vol} 回报:{res}")
        except Exception as e:
            self.log.warning(f"{self.symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")
    

def main(symbol, path='arb_spot_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(symbol, config).run()


if __name__ == '__main__':
    main(sys.argv[1])



