
import sys
import time
import datetime
import asyncio
import traceback
import numpy as np
# sys.path.append('/homu/ubuntu/strategy_base')
sys.path.append('../..')
from client.env_pro.rest.websea.contract import WebseaContract as contract
from crypto_center.client.rest.okex import contract as okx



class Strategy():

    def __init__(self):
        self._load_config()  # 读取配置
        self.symbol_precision = {}

    def _load_config(self):
        """读取配置文件
        """
        self.rest = contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')
        #self.task = okx.OkexContract(apiKey='e1934f97-f831-418e-b154-1ec5a0415f9a',secret='A2D8A15D0DE4B59BD7B270A34BB1273C',passphrase='usRolUVNuyBbEF@7 ')

    async def main(self):
        # OK合约精度+面值
        symbol_precision = {}
        #res = await self.task.fetch_precision()
        # print(f"{res}")
        #for s, v in res.items():
        #    price_precision = int(-np.log10(v['price']))    # 价格精度
        #    amount_precision = int(-np.log10(v['amount']))  # 数量精度
        #    face_value = v['faceValue']  # 合约面试,1张=多少币
        #    text = f"价格精度:{price_precision},数量精度:{amount_precision},合约面值(一张是多少个币):{face_value}"
        #    symbol_precision[s] = text
        #print(f"OK交易对精度+面值:{symbol_precision}")

        # websea合约精度+合约单位
        # symbol_precision = {}
        # data = await self.rest.get_symbols(quan=True)
        # print(f"websea合约单位:{data}")
        # for i in data:
        #     text = f"最小下单量:{i['min_size']}张,最大下单量:{i['max_size']}张,最小下单价格:{i['min_price']},最大下单价格:{i['max_price']},合约单位(一张是多少个币):{i['contract_size']} "
        #     symbol_precision[i['symbol']] = text
        
        symbol_precision = {}
        data = await self.rest.get_precision(quan=True)
        # print(f"websea精度:{data}")
        for k, v in data.items():
            text = f"价格精度:{v.price},数量精度:{v.amount},合约面值(一张是多少个币):{v.faceValue}"
            symbol_precision[k] = text
        print(f"websea交易对精度+面值:{symbol_precision}")

async def main():
    task = Strategy()
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())

