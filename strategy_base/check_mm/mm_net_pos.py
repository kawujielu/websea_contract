
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
        self.symbol_price = {}      # 交易对价格
        self.symbol_size = {}       # 交易对合约单位

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
        self.symbol_pos = {}    # 交易对持仓
        for num, task in enumerate([self.rest1, self.rest2, self.rest3, self.rest4, self.rest5, self.rest6]):
            await self.get_pos(task)
        mess = f"6个线上账户总持仓:\n"
        for k, v in sorted(self.symbol_pos.items(), key=lambda x: abs(x[1]), reverse=True):
            if v != 0 and self.symbol_price[k]:
                mess += f"【{k}】:仓位{v}张 金额:{round(v*self.symbol_size[k]*self.symbol_price[k])}usdt\n"
        print(mess)
    
    async def get_pos(self, task):
        pos = await task.get_position(quan=True)
        # print(f"做市账户持仓:{pos}")
        if pos is None:
            return 0
        for i in pos:
            if i.type == 1:      # 多单
                try:
                    self.symbol_pos[i.symbol] += i.amount
                except:
                    self.symbol_pos[i.symbol] = i.amount
                    self.symbol_price[i.symbol] = i.mark_price
            elif i.type == 2:    # 空单
                try:
                    self.symbol_pos[i.symbol] -= i.amount
                except:
                    self.symbol_pos[i.symbol] = -i.amount
                    self.symbol_price[i.symbol] = i.mark_price


async def main():
    task = takeOverStrategy()
    data = await task.rest1.get_symbols()
    for i in data:
        task.symbol_size[i.symbol] = i.contract_size
    while 1:
        try:
            await task.main()  # 主循环
        except:
            try:
                print(f'策略报错!\n{traceback.format_exc()}')
            except:
                pass
        time.sleep(60)

asyncio.run(main())


