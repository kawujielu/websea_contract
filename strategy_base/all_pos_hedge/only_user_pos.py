'''
    根据交易所全部用户持仓做对冲
    版本信息:v1.0.0
    日期:2026-04-09
    作者:sky

    重点：
    1、判断哪些持仓需要对冲
       判断条件:
       a. 历史盈亏,历史交易笔数大于50笔,整体盈利超过1000u的对冲
       b. 交易风格,高胜率低盈亏的不对冲
       c. 持仓时间小于5min占比超过30%的持仓不对冲
'''

import sys
import time
import copy
import datetime
import importlib
import traceback
import asyncio
import numpy as np
import pos_hedge_config
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.redis_cluster_shard import RedisClusterShard
from config.redis_cfg import RedisConfigClusterAwsPro
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest


strategy_name = "pos_hedge对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()

        self._load_config(config)   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
        self.wss_log = tb.Log(f'wss_log/all_wss_data.log')
        self.deal_log = tb.Log(f'deal_log/all_hedge_deals.log')
        
                
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
        self.rest.DEBUG = False
        self.ws_rest.DEBUG = False
        self.okx_rest.DEBUG = False

    def _initParams(self):
        """初始化参数
        """
        self.user_symbol_pos = {}           # 每个用户的持仓
        
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_all_data.log')
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})    # 保存所有成交聚合数据

    async def on_first(self):
        self.log.add(f"log/all_pos_hedge.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        pos = await self.fetch_all_hold_list()
        await self.count_symbols_pos(pos)
        exit()

    async def on_timer(self):
        self.log.info("=======timer======")
        
    async def fetch_all_hold_list(self, page_size: int = 100) -> List[dict]:
        rows: List[dict] = []
        last_page = 0
        while True:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_hold_list(page_size=page_size)
                    total_page = int(data["pager"]["total_page"])
                    for i in data["data"]:
                        rows.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, total_page + 1):
                    last_page = n
                    data = await self.rest.fetch_hold_list(page=n, page_size=page_size)
                    for i in data["data"]:
                        rows.append(i)
                    await asyncio.sleep(0.5)
                break
            except Exception:
                print(traceback.format_exc())
                await asyncio.sleep(5)
        return rows
    
    async def count_symbols_pos(self, pos: List[dict]):
        check_uid_type = []
        for i in pos:
            tag = i['tag']
            if 'E' in tag:
                continue
            uid = i['user_id']
            symbol = i['symbol']
            side = i['openDirection']
            user_pos = float(i['tradeNum']) if side == 1 else -float(i['tradeNum'])  # 币的数量(非张数)
            # profit = float(i['profit_loss'])
            if symbol in self.user_symbol_pos:
                if uid in self.user_symbol_pos[symbol]:
                    if user_pos > 0:
                        self.user_symbol_pos[symbol][uid][0] = user_pos
                    else:
                        self.user_symbol_pos[symbol][uid][1] = user_pos
                else:
                    if user_pos > 0:
                        self.user_symbol_pos[symbol][uid] = [user_pos, 0]
                    else:
                        self.user_symbol_pos[symbol][uid] = [0, user_pos]
            else:
                if user_pos > 0:
                    self.user_symbol_pos[symbol] = {uid: [user_pos, 0]}
                else:
                    self.user_symbol_pos[symbol] = {uid: [0, user_pos]}
        # user_symbol_pos数据聚合成{symbol: sum(pos)}
        symbols_pos = {}
        for symbol, pos_dict in self.user_symbol_pos.items():
            # print(symbol, pos_dict)
            symbols_pos[symbol] = sum([v[0] + v[1] for v in pos_dict.values()])
        self.log.info(f"初始化用户持仓1:{symbols_pos}")
        return symbols_pos
    

def main(path='pos_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()


