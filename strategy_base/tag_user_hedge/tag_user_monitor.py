'''
    监控指定标签+指定uid用户的成交
    若平仓则立即在外盘平仓
'''

import sys
import time
import datetime
import requests
import traceback
import asyncio
import json
import numpy as np
sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss

# import contract as okx
# from crypto_center.client.rest.okex.contract import OkexContract



class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()

        self._load_config()  # 读取配置
        self._initParams()    # 初始化参数

        # 订阅base数据
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())     # 必须写这个才能订阅
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self):
        """读取配置文件
        """
        # 配置账号
        # self.ws_rest = ws_contract_rest('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')   # 压盘口策略
        # self.task = OkexContract(apiKey='e1934f97-f831-418e-b154-1ec5a0415f9a',secret='A2D8A15D0DE4B59BD7B270A34BB1273C',passphrase='usRolUVNuyBbEF@7 ')
        pass

    def _initParams(self):
        """初始化参数
        """
        self.tag = 'I'
        # self.symbol = 'XRP-USDT'            # 对冲合约
        self.send_info = ''
    
    async def on_first(self):
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))  # 每1s执行一次
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        # 原始数据
        # 平仓ws成交推送数据:{'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.6002', 'liquidationPrice': '1094.25', 'cumfilledSize': '', 'uid': '', 'positionAmt': '-0.02', 'markPrice': '600.25', 'price': '', 'tag': 'A', 'direction': 'SPACE', 'side': 'BUY', 'origQty': 0.02, 'positionSide': 'SHORT', 'updateTime': 1729147616045, 'userId': 60142803, 'market': 'BNB-USDT', 'entryPrice': '600.50', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '0.005', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}, <class 'dict'>
        # 平仓ws成交推送数据:{'lastfilledVolume': 12.0, 'orderType': 'market', 'lastfilledSize': 0.02, 'side': 'BUY', 'origQty': 0.02, 'cumfilledSize': 0.02, 'userId': 60142803, 'market': 'BNB-USDT', 'uid': 0, 'cumfilledVol': '12.00', 'price': '', 'lastfilledprice': '600.41', 'tag': 'A', 'status': 'FILLED', 'direction': 'SPACE'}, <class 'dict'>
        if content['tag'] == self.tag and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            symbol = content['market']
            price = content['lastfilledprice']
            size = content['lastfilledSize']
            side = content['side']
            uid = content['userId']
            self.send_info += f"{uid}用户交易{symbol}合约 价格:{price} 数量:{size} 方向:{side}\n"

    async def risk(self):
        if self.send_info != '':
            self.log.info(self.send_info)
            tb.warning(f'{self.tag}组用户成交监控:\n{self.send_info}','risk')
            tb.sendmail(f'{self.tag}组用户成交监控', self.send_info)
            self.send_info = ''
    


def main():
    task = Strategy().run()
    while True:
        time.sleep(999999)



if __name__ == '__main__':
    main()

