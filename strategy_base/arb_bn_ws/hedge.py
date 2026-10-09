
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_quan import WebseaContract
from client.env_pro.rest.websea.spot import WebseaSpot
import objects.contract_request.websea as ocw



class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        # 配置账号
        self.swap_rest = WebseaContract('d6678ea254de541f67594668b8ee7937','tz1rtf00gudmgch3jchl')
        # self.swap_rest = WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')
        self.spot_rest = WebseaSpot('dfe9b8bdc54f4461b04b876e065201f1','01k2iq47q99o5npoi6p2')
        
    async def on_first(self):
        await self.main()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # main
    async def main(self):
        res = await self.swap_rest.cancel_order_batch('BIO-USDT')
        print(res)
        
        # res = await self.spot_rest.get_account()
        # print(f"现货权益:{res}")
        
        # res = await self.swap_rest.fetch_balance()
        # print(f"合约权益:{res}")
        
        # pos = await self.swap_rest.fetch_position('BTC-USDT')
        # print(f"合约持仓:{pos}")
        
        # 现货下单
        # symbol = 'BTC-USDT'
        # price = 1
        # vol = 0.01
        # order_type = 'buy-limit'
        # precision = await self.spot_rest.get_precision(symbol)
        # res = await self.spot_rest.order_create(symbol, precision=precision[symbol],
        #                                         order_type=order_type, \
        #                                         price=price, amount=vol)
        # print(res)
        
        # 合约下单
        # symbol = 'BTC-USDT'
        # price = 120000
        # vol = 0.01
        # side = 'buy'
        # od_type = 'buy-limit' if side == 'buy' else 'sell-limit'
        # res = await self.swap_rest.create_order(symbol, order_type=od_type, \
        #                                         side=side, \
        #                                             price=price, amount=vol, \
        #                                             # precision=self.precision, \
        #                                             contract_type='close',
        #                                             quan=True)
        # print(f"下单结果:{res}")

def main():
    Strategy().run()



if __name__ == '__main__':
    main()




