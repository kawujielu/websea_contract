
import sys
import time
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.okex import contract as okx_rest

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        
    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # 对冲
    async def hedge(self):
        res = await self.okx_rest.fetch_balance()
        print(res)

        res = await self.okx_rest.fetch_order_detail(symbol='ETH-USDT')
        print(res)
        

def main():
    Strategy().run()



if __name__ == '__main__':
    main()
