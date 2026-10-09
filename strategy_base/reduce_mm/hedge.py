
import sys
import traceback
import time
import datetime
import json
import asyncio
import requests
sys.path.append('../..')
from utils.ToolBoxNew import ToolBox as tb
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from crypto_center.client.rest.binance.u_contract import UBinanceContract as bn_rest
# from client.env_dev.rest.websea_contract import WebseaContract as contract    # 测试环境
# from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss      # 测试环境


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()   # 读取配置
        # self._init_params()         # 初始化参数

    def _load_config(self):
        task = tb()
        accts = task.load_account()
        # print(f"账户:{accts}")
        self.accts = []
        for id, token in accts.items():
            task = Contract(token[0], token[1])
            task.DEBUG = False
            self.accts.append([task, id])
        self.rest = Contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')   # 压盘口策略
        # self.spot = WebseaSpot('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')
        # self.bn = u_contract.UBinanceContract('','')

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗 
    async def main(self):
        all = 0
        for task in self.accts:
            rest, mm_uid = task
            try:
                data = await rest.fetch_balance()
                if 'result' in data:
                    print(f"{mm_uid}没有数据")
                    continue
            except:
                print(f"{mm_uid}没有数据")
                continue
            #print(f"当前权益:{data}")
            total = 0
            for k, v in data.items():
                total += v['total']
            all += total
            print(mm_uid, total)
        print(all)
        return

        open_orders = await self.rest.fetch_current_list('ETH-USDT', direct='prev', limit=9999)
        print(f"当前委托:{open_orders} {len(open_orders)}笔")
        
        res = await self.rest.cancel_order_batch(symbol='ETH-USDT')
        print(f"撤单:{res}")

        #data = await self.rest.get_position(symbol=symbol)
        #print(f"当前仓位:{data}")

        #data = await self.rest.fetch_precision(symbol)
        #print(f"合约单位:{data}")

        #data = await self.rest.order_detail('SL4765151741129463648TSQFQI')
        #print(f"订单详情:{data}")
        
        symbol = 'ETH-USDT'
        price = 1000
        amount = 0.1
        side = 'buy'
        #data = await self.rest.create_order(symbol='ETH-USDT', reduce_only=False, side=side, leverage=5, margin_mode='cross', \
        #                                    price=price, amount=amount, client_order_id=f"{int(time.time()*1000)}")
        #print(f"下单回报:{data}")
        
        #for s in ['BCH-USDT']:
        #    res = await self.rest.cancel_order_batch(symbol=s)
        #    print(f"cancel回报:{res}")


async def main():
    task = takeOverStrategy()
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())



