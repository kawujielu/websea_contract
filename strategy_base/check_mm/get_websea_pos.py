
import sys
import traceback
import time
import asyncio
sys.path.append('../..')
from client.env_pro.rest.websea import contract


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()         # 读取配置
        self.symbol_pos = {}        # 交易对持仓
        # self.symbol_price = {}      # 交易对价格
        self.symbol_size = {}       # 交易对合约单位
        self.all_sum_amt = 0        # 所有账户总持仓的净金额
        self.all_sum_amt_long = 0   # 所有账户总持仓的多单金额
        self.all_sum_amt_short = 0  # 所有账户总持仓的空单金额

    def _load_config(self):
        self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口
        self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口
        self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位
        self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量
        self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略
        self.rest6 = contract.WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')    # 压盘口策略

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 获取
        self.symbol_size = {}
        res = await self.rest1.get_symbols(quan=True)
        for s in res:
            self.symbol_size[s.symbol] = s.contract_size
        
        data = await self.rest1.get_symbols()
        for i in data:
            self.symbol_size[i.symbol] = i.contract_size

        await self.get_pos()
            
    async def get_pos(self):
        pos = await self.rest1.get_hold()
        temp_dict = {}
        for s, p in pos.items():
            get_index = await self.rest1.get_index(s)
            price = get_index.price
            temp_dict[s] = int(p.volume*self.symbol_size[s]*price)
        # 按字典的vol从大到小排序
        temp_dict = sorted(temp_dict.items(), key=lambda x: x[1], reverse=True)
        print(f"所有交易对持仓")
        for s, v in temp_dict:
            if v > 5000000:
                print(f"{s}持仓:{v}")


async def main():
    task = takeOverStrategy()
    await task.main()  # 主循环

asyncio.run(main())





