'''
    订阅websea压盘口策略成交推送
'''
import sys
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from typing import Optional, Dict, List
from utils import Toolbox as tb
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger

class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.deal_log = tb.Log('log/deals.log')
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
    
    async def on_first(self):
        self.log.info("=======first======")

        # 订阅websea私有成交数据
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        channel_pos = f"contract.order.websea.{self.symbol}.{self.token}"
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_pos, callback=self.on_hedge_message))
        self.log.info("subscribe: websea私有数据成交")

    async def on_timer(self):
        self.log.info("=======timer======")

    def _load_config(self, config):
        """读取配置文件
        """
        config = __import__(config)
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        self.token = config['token']

    def _initParams(self):
        """初始化参数
        """
        pass
            
    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    # 接收订阅的外盘一档价格    测试成功
    async def on_hedge_message(self, channel: str, item: dict):
        print('redis订阅信息:', channel, item)
        # contract.order.websea.ETH-USDT.44c614fa9131929291e4fa7325d47653000 
        # {'exchange': 'websea', 'symbol': 'ETH-USDT', 'id': 'BL4765151749281775174P8C7IM', 
        #  'clientOrderId': '', 'price': 2493.01, 'stopPrice': None, 'triggerPrice': None, 'amount': 411.04, 
        #  'side': 'buy', 'type': 'limit', 'status': 'open', 'leverage': None, 'timeInForce': None, 
        #  'postOnly': None, 'reduceOnly': False, 'marginMode': None, 'average': None, 'filled': 164.64, 
        #  'cost': None, 'remaining': 246.4, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': None, 
        #  'dealRole': 100022, 'timestamp': 1749281775202, 'messageType': 'message', 
        #  'info': {'amount': 411.04, 'dealRole': 100022, 'filled': 164.64, 'orderId': 'BL4765151749281775174P8C7IM', 
        #           'orderType': '1', 'price': '2493.01', 'side': 1, 'status': 2, 'symbol': 'ETH-USDT', 
        #           'timestamp': 1749281775202, 'tradeAmt': 27.2}}
        if 'order' in channel:
            self.deal_log.write(item)

def main(config):
    strategy(config).run()


if __name__ == '__main__':
    main(sys.argv[1])


