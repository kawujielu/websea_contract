
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

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        A_book = []
        B_book = []
        hold_list = []
        temp_hold_list = []
        data = await self.rest1.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20)
        hold_list_page = data['data']['pager']['total_page']
        temp_hold_list += data['data']['data']
        for n in range(2, hold_list_page+1):
            data = await self.rest1.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20)
            temp_hold_list += data['data']['data']
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        sum_profit = 0
        hold_pos = 0
        for i in hold_list:
            # print(i)
            {'amount': '100', 'avgPrice': '0.000020450', 'freeze_amount': '0', 'in_position': '2.63', 
             'isFull': 2, 'is_full': 2, 'mark_price': '0.000021221', 'multiple': 20, 'openDirection': 1, 
             'open_time': 1729908770, 'parity': '--', 'profitLoss': '0.7710', 'profit_loss': '0.7710', 
             'risk_ratio': '2.63', 'symbol': 'BONK-USDT', 'tag': 'A', 'time': 1729908770, 
             'user_id': 150387, 'user_name': 150387, 'zhqy': '284.5676'}
            tag = i['tag']
            symbol = i['symbol']
            if tag == 'Z3':
                uid = i['user_id']
                pos = float(i['amount'])
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                profit = float(i['profitLoss'])
                if symbol == 'BCH-USDT':
                    hold_pos += pos
                sum_profit += profit
                A_book.append([uid, symbol, pos, side, profit])
        for i in A_book:
            print(i)
        print(f"总持仓:{hold_pos} 盈亏:{sum_profit}")





async def main(path='config1.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
    await task.main()  # 主循环

asyncio.run(main())



