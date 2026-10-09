
import sys
import traceback
import time
import json
import asyncio
import requests
sys.path.append('../..')
from client.env_pro.rest.websea import contract
import objects.contract_request.websea as ocw

import datetime


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置
        # self._init_params()         # 初始化参数

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        self.token = config['tokon1']
        self.mm_uid = config['uid1']
        self.ordertype_map: dict = {
            "buy": ocw.OrderType.buy_limit,
            "sell": ocw.OrderType.sell_limit,
        }
        # self.rest = contract.WebseaContract(config['tokon1'],config['secret1'])
        self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口 多单24416
        self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口 空
        self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位 多单158
        self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量 空单24639 多单65
        self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略测试账户
        self.rest6 = contract.WebseaContract('7f81fa4496ec0064dda3ccbc5bd45703200','c8a0lwr60cb7ws3ktl86')  # 我自己的测试账户

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        # 注意修改rest1-5,交易对名称,方向,价格,数量
        symbol = "FTM-USDT"
        price = 0.825
        amount = 849262
        #data = await self.rest5.get_walletList(symbol='BNB-USDT', quan=True)
        #print('账户权益:', data)
        data = await self.rest5.order_create(symbol, self.ordertype_map['sell'], price=price, amount=amount, contract_type='close', quan=True)
        print(f"下单回报:{data}")
        data = await self.rest5.order_create(symbol, self.ordertype_map['buy'], price=price, amount=amount, contract_type='close', quan=True)
        print(f"下单回报:{data}")
        #data = await self.rest5.get_position(symbol='BNB-USDT', quan=True)
        #print(data)


async def main(path='config1.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())

