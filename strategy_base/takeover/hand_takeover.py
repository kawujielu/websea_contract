
'''
    需求:用户触发穿仓后,平掉用户的仓位
    大概率,如果用户穿仓,做市商的头寸是赚钱的,也就是做市商头寸方向与穿仓用户头寸方向相反
    策略逻辑:
    有用户穿仓
        1、若mm仓位盈利,在市场上跟mm成交
        2、若mm仓位不盈利,通过adl跟其他盈利用户成交
    细节:
        1、穿仓在市场下单,用限价单一笔开仓,若超过单笔最大下单量,就拆分下单。限价单不撤单,mm策略补单时会吃掉,保证mm利润最大化
        2、无论穿仓用户持仓与mm是否一致,只要mm盈利就可以跟mm成交。成交后若触发adl,后续逻辑由adl策略处理
    问题:
        1、穿仓用户的信息有单独的接口吗?
        2、如何将用户持仓平掉?用adl接口?还是有其他接口可以处理用户持仓?

    需求更新:
    目的是穿仓+扎针
    对于我们不盈利的仓位,根本不管。不是完全不做处理,可以加方向判断。
    在下跌的时候,人工爆仓更安全合理
    若用户穿仓,mm可以盈利,就处理。否则不处理

'''

import sys
import traceback
import time
import json
import requests
import asyncio
from loguru import logger
sys.path.append('../..')
from client.env_pro.rest.websea import contract

import pytz
import datetime


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        self.token = config['tokon1']
        self.uid = config['uid1']
        self.rest = contract.WebseaContract(config['tokon1'],config['secret1'])
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        symbol = 'BTC-USDT'
        side = 'LONG'or'SHORT'
        mm_uid = 19,20,21,22
        adl_uid = ???
        adl_percent = 90

        data = await self.rest.adl_order(token=self.takeover_token, market=symbol, \
                                         mmUserId=mm_uid, side=side, \
                                         adlUserId=adl_uid, adlPercent=adl_percent, \
                                         marginType='crossed')
        print(f"adl下单回报:{data}")
    


async def main(path='takeover_config4.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
    await task.main()  # 主循环

asyncio.run(main())

