
import sys
import traceback
import time
import json
import asyncio
import requests
sys.path.append('../..')
from client.env_pro.rest.websea import contract

import datetime


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置
        # self._init_params()         # 初始化参数

    def _load_config(self, config):
        # [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        # config = config.config
        # # 交易所实例初始化
        # self.token = config['tokon1']
        # self.mm_uid = config['uid1']
        # self.rest = contract.WebseaContract(config['tokon1'],config['secret1'])
        self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口 多单24416
        self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口 空
        self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位 多单158
        self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量 空单24639 多单65
        self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略测试账户
        self.rest6 = contract.WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        data = await self.rest1.get_symbols('ALLO-USDT')
        print(f"{data.symbol}合约单位:{data.contract_size}")

        all_pos = 0
        for num, task in enumerate([self.rest6]):  #enumerate([self.rest1, self.rest2, self.rest3, self.rest4]):
            net_pos = await self.get_pos(task, num)
            all_pos += net_pos
        print(f"4个线上账户总持仓:{all_pos*data.contract_size}")
        #new_pos = await self.get_pos(self.rest5, 'new')
        #print(f"新账户净持仓:{new_pos*data.contract_size}")

    
    async def get_pos(self, task, num):
        pos = await task.get_position(quan=True)
        #print(f"做市账户持仓:{pos}  {type(pos)}")
        if pos is None:
            return 0
        symbol_pos = {}
        symbols = []
        net_pos = 0     # 净持仓
        for i in pos:
            if i.symbol not in symbol_pos:
                symbol_pos[i.symbol] = [0, 0]
            if i.type == 1:      # 多单
                net_pos += i.amount
                symbol_pos[i.symbol][0] = i.amount
            elif i.type == 2:    # 空单
                net_pos -= i.amount
                symbol_pos[i.symbol][1] = i.amount
            if i.symbol not in symbols:
                symbols.append(i.symbol)
        print(f"{num}账户净持仓:{symbol_pos}")
        print(symbols)
        return net_pos



async def main(path='config1.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
    while 1:
        try:
            await task.main()  # 主循环
        except:
            try:
                task.writeLog(f'策略报错!\n{traceback.format_exc()}')
            except:
                pass
        time.sleep(60)  # TODO 根据adl接口数据更新频率决定sleep间隔

asyncio.run(main())


