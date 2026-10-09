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
        self.log.add(f"spot_log/sol2_log.log", rotation="100 MB", retention=10)
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
        # print(content)
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
    async def check_arb(self):
        # await self.get_depth()
        bn_ask = self.ask_one_price*(1+self.fee+0.0002)
        bn_bid = self.bid_one_price*(1-self.fee+0.0002)
        bid_price = 0
        for bp in self.ws_depth[0]:
            con = bp[0] > bn_ask
            if con:
                bid_price = bp[0]
        ask_price = 0
        for ap in self.ws_depth[1]:
            con = ap[0] < bn_bid
            if con:
                ask_price = ap[0]
        return bid_price, ask_price
    
    async def main(self):
        # 拿binance和websea盘口数据,按fee计算有套利空间的铺单,直接吃掉
        bid_price, ask_price = await self.check_arb()
        if bid_price != 0 or ask_price != 0:
            await asyncio.sleep(1)
            bid_price, ask_price = await self.check_arb()
            if bid_price != 0:
                self.buy_num += 1
                self.log.info(f"延迟500ms后,依然有套利机会,buy触发{self.buy_num}次 sell触发{self.sell_num}次")
            if ask_price != 0:
                self.sell_num += 1
                self.log.info(f"延迟500ms后,依然有套利机会,buy触发{self.buy_num}次 sell触发{self.sell_num}次")
        
        
def main(symbol, path='arb_spot_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(symbol, config).run()


if __name__ == '__main__':
    main(sys.argv[1])


