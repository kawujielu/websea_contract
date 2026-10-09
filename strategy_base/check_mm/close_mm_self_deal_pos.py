'''
    22账户:
    FLOKI, DOGE

    23账户:
    PEPE, BONK, XEC, FLOKI, DOGE

    24账户:
    XEC, SHIB, PEPE
'''
import sys
import traceback
import time
import asyncio
import random
sys.path.append('../..')
# from client.env_pro.rest.websea import contract
# import objects.contract_request.websea as ocw
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口




class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()         # 读取配置
        # self.loop = asyncio.get_event_loop()
        self.symbol_pos = {}        # 交易对持仓
        self.symbol_price = {}      # 交易对价格
        self.symbol_size = {}       # 交易对合约单位
        self.clear_pos = False      # 是否清空持仓
        self.amt_limit = 1000000    # 平仓阈值

    def _load_config(self):
        # self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口
        # self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口
        # self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位
        # self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量
        # self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略
        # self.rest6 = contract.WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4') # 压盘口策略
        # self.rest7 = contract.WebseaContract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3')    # 防守策略
        # self.rest8 = Contract('fd0bc370e0f8dc47a923f54fca31d3d2','zbihesbcp6yz5j0awlqv',dev=False)    # 新铺单策略-2 24
        # self.rest9 = Contract('d6678ea254de541f67594668b8ee7937','tz1rtf00gudmgch3jchl',dev=False)   # 新铺单策略-3

        self.rest1 = Contract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz',dev=False)    # 新策略 22
        self.rest2 = Contract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3',dev=False)    # 防守策略 23
        self.rest3 = Contract('fd0bc370e0f8dc47a923f54fca31d3d2','zbihesbcp6yz5j0awlqv',dev=False)    # 新铺单策略-2 24
        self.rest1.DEBUG = False
        self.rest2.DEBUG = False
        self.rest3.DEBUG = False

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 查询合约单位
        symbol_unit = {}
        precision = await self.rest1.fetch_precision()
        # print(f"币对信息:{precision}")
        for symbol, data in precision.items():
            symbol_unit[symbol] = data['faceValue']    # 合约单位
        
        num = 0
        while num <= 1000000:
            for i in [[self.rest1, ['BNB-USDT']],
                      [self.rest2, ['OP-USDT', 'PEPE-USDT', 'PUMP-USDT', 'SHIB-USDT', 'VET-USDT']]]:   #'PEPE-USDT', 'BONK-USDT', 'XEC-USDT', 'FLOKI-USDT', 'DOGE-USDT','DOOD-USDT']]]:
                      #[self.rest3, ['XEC-USDT', 'SHIB-USDT', 'PEPE-USDT']]]:
                task = i[0]
                for symbol in i[1]:
                    try:
                        res = await task.fetch_kline(symbol, interval='1m', limit=100)
                        # print(data)
                    except:
                        continue
                    ave_vol = 0
                    for i in res:
                        if symbol in ['BTC-USDT', 'ETH-USDT']:
                            ave_vol += i['volume']*0.01
                        else:
                            ave_vol += i['volume']
                    ave_vol = ave_vol/len(res)
                    print(f"{symbol}合约1分钟平均成交量:{ave_vol}")
                    # if symbol in ['PEPE-USDT','FLOKI-USDT','BONK-USDT','XEC-USDT','SHIB-USDT']:
                    #     deal_vol = int(ave_vol*0.1*1000)
                    # else:
                    #     deal_vol = int(ave_vol*0.1)
                    deal_vol = int(ave_vol*0.3)*symbol_unit[symbol]
                    price = res[0]['close']
                    print(f"下单价量:{price} {deal_vol}")
                    # continue
                    num += 1
                    try:    
                        res1 = await task.create_order(symbol, reduce_only=True, side='buy', \
                                                price=price, amount=deal_vol, leverage=5, margin_mode='crossed',\
                                                client_order_id=int(time.time()*1000000))
                    except:
                        print(f"下单报错:{traceback.format_exc()}")
                    await asyncio.sleep(0.5)
                    try:
                        res2 = await task.create_order(symbol, reduce_only=True, side='sell', \
                                                price=price, amount=deal_vol, leverage=5, margin_mode='crossed',\
                                                client_order_id=int(time.time()*1000000))
                        print(f"下单回报:{res1}\n{res2}")
                    except:
                        print(f"下单报错:{traceback.format_exc()}")
            random_time = random.randint(10, 40)
            await asyncio.sleep(random_time)


async def main():
    task = takeOverStrategy()
    await task.main()  # 主循环

asyncio.run(main())







