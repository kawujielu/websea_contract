'''
    大户实时跟单策略
    版本信息:v1.0.0
    日期:2025-11-28
    作者:sky

    测试方案:
    1、快速开平仓
    2、未完全对冲时继续加仓,不可以超过最大对冲金额
'''

import sys
import time
import copy
import datetime
import importlib
import traceback
import asyncio
import numpy as np
import major_player_hedge_config
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest


strategy_name = "606019大户对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, id, config):
        super().__init__(scheduler=True, gcc=True)
        self.id = int(id)
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()

        self._load_config(config)   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
        self.wss_log = tb.Log(f'wss_log/{self.id}_wss_data.log')
        self.deal_log = tb.Log(f'deal_log/{self.id}_hedge_deals.log')
        
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag='E12'))  # CHECK self.tag
                
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

    def _initParams(self):
        """初始化参数
        """
        self.symbols_markprice = {}         # 保存内盘标记价格
        self.contract_unit = {}             # 保存内盘合约单位
        self.symbols_bid_ask = {}           # 保存外盘wss推送来的一档价格
        self.deals_dict = {}                # 保存内盘成交数据
        self.hedge_symbol_precision = {}    # 保存交易对精度
        self.symbol_precision = {}          # 保存ok交易对精度+面值
        self.unhedge_vol_dict = {}          # 保存未对冲量
        self.hedge_clock = False            # 对冲锁
        self.check_hedge_clock = False      # 追单锁
        self.hedge_pos = {}                 # 保存对冲端仓位
        self.wss_filled_list = {}           # 保存okx的wss推送过来的成交订单id
        self.hedge_symbols = []             # 保存对冲端交易对列表
        # self.id = list(self.leads.keys())[0]  # 保存用户id
        self.uid = int(self.leads[self.id])      # 带单员UID列表
        self.this_wss_data = ""
        
        self.each_vol_check_ts = 0          # 上次查询成交量的时间戳
        self.each_vol_dict = {}             # 保存每次对冲数量
        self.hedge_time = 60                # 对冲时长
        self.hedge_type = 'maker'           # 对冲类型
        self.duration = 1                   # maker挂单持续时间
        self.max_plan_num = self.hedge_time/self.duration    # 最大计划下单次数,根据对冲时长和挂单持续时间计算
        
    
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_follow_data.log')
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})    # 保存所有成交聚合数据
    
    async def on_first(self):
        self.log.add(f"log/{self.id}_follow_hedge.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_hedge_message))
        await asyncio.sleep(1)
        await self.redis_conn.sub_channel(f"market.tag.hold.A2")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.A1")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.E8")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.E9")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.E11")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.E12")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.E13")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.Z2")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.D")       # 标记组持仓推送
        await self.redis_conn.sub_channel(f"market.tag.hold.M")       # 标记组持仓推送
        self.log.info(f"初始化完成")
        
    async def on_timer(self):
        self.log.info("=======timer======")

    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
    
    async def on_hedge_message(self, channel: str, item: dict):
        self.log.info(f"redis订阅信息: {channel}, {item}")


def main(id, path='major_player_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(id, config).run()



if __name__ == '__main__':
    id = sys.argv[1]
    main(id)





