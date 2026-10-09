
import sys
import traceback
import time
import datetime
import json
import asyncio
import requests
sys.path.append('../..')
from client.env_pro.rest.websea.contract import WebseaContract as contract
# from client.env_pro.rest.websea import contract
# from client.env_pro.rest.binance import u_contract
# from client.env_dev.rest.websea_contract import WebseaContract as contract    # 测试环境
# from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss      # 测试环境
import objects.contract_request.websea as ocw

import datetime


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()   # 读取配置
        # self._init_params()         # 初始化参数

    def _load_config(self):
        # self.rest = contract.WebseaContract(config['tokon1'],config['secret1'])
        # self.rest = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')      # 近盘口 多单24416
        # self.rest = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口 空
        # self.rest = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位 多单158
        # self.rest = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量 空单24639 多单65
        # self.rest = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')      # 新账户
        # self.bn_rest = u_contract.UBinanceContract('ZlbbTVrOSfCUBtj6b6eNTCiTNjsojoxMRaDGIEZjORCRTm2dSax7sdwi9PgyMvGu', 'M62FUBt8Prey2Q2B0YszyB2Ms9nXb5hd5o01sF8LEablSG1qtVev7bjJXkbT0fSu')
        # self.rest = contract.WebseaContract('e2fa096f88dcf7682256fc1bb0r52580220', '2ocytzj1udbqpbru0e5d')  # 实盘测试跟单账户
        #self.rest = contract('3f57905a03a32432fae784037cb44f4c','sx4n8k2nh8hfz5gjs672')   # 测试环境账户
        self.rest = contract('57b49384ee4e0b236f0f38a45fj50733190', 'bu8qqmoyaq0o10xeepdd') # 生产环境

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        symbol = 'MH-USDT'
        side = ocw.OrderType.buy_limit

        open_orders = await self.rest.get_currentList(symbol, direct='prev', limit=999)
        print(f"当前委托:{open_orders} {len(open_orders)}笔")
        for i in open_orders:
            print(i.type, i.price)
        #exit()

        # data = await self.rest.get_walletList()
        # print(f"当前钱包:{data}")

        data = await self.rest.get_position()
        print(f"当前仓位:{data}")

        #precision = await self.rest.get_precision(symbol)
        #print(f"精度:{precision}")

        # data = await self.rest.order_detail('SL97173499030029619T2MM')
        # print(f"订单详情:{data}")

        price = 8.98   #precision.minPrice
        amount = 648  #precision.minQuantity
        #data = await self.rest.order_create(symbol, od_type=side, price=price, amount=amount)
        #print(f"下单回报:{data}")

        res = await self.rest.order_cancel(symbol=symbol)
        print(f"cancel_all回报:{res}")

        # data = await self.bn_rest.get_position()
        # for k,v in data.items():
        #     if float(v['positionAmt']) != 0.:
        #         print(f"binance持仓:{v}")


async def main():
    task = takeOverStrategy()
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())

