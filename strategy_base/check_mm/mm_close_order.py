
import sys
import traceback
import time
import asyncio
import random
sys.path.append('../..')
from client.env_pro.rest.websea import contract
import objects.contract_request.websea as ocw


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

    def _load_config(self):
        self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口19
        self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口20
        self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位21
        self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量18
        self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略 22
        self.rest6 = contract.WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')    # 压盘口策略
        self.rest7 = contract.WebseaContract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3')    # 防守策略 23
        self.rest8 = contract.WebseaContract('5accd1fb1d03e26f299a4297d9q52646135','nyuc69aljff4g297uj99') # MH做市策略
        self.rest9 = contract.WebseaContract('57b49384ee4e0b236f0f38a45fj50733190','bu8qqmoyaq0o10xeepdd')
        self.rest10 = contract.WebseaContract('3f57905a03a32432fae784037cb44f4c','sx4n8k2nh8hfz5gjs672')
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 需要修改的参数
        symbols = ['ETC-USDT', 'H-USDT', 'PEPE-USDT', 'XTZ-USDT', 'OP-USDT', 'FIL-USDT', 'XRP-USDT', 'STX-USDT', 'PROVE-USDT', 'ZORA-USDT', 'XPL-USDT', 'WCT-USDT', 'LTC-USDT', 'PUMP-USDT', 'SIGN-USDT', 'TON-USDT', 'GALA-USDT', 'VINE-USDT', 'GRT-USDT', 'ICP-USDT', 'FLOW-USDT', 'APT-USDT', 'IDOL-USDT', 'RESOLV-USDT', 'TRX-USDT', 'PYTH-USDT', 'ARB-USDT', 'KERNEL-USDT', 'BNB-USDT', 'LINK-USDT', 'DOOD-USDT', 'CROSS-USDT', 'SHIB-USDT', '0G-USDT', 'ETH-USDT', 'AVAX-USDT', 'SEI-USDT', 'BCH-USDT', 'SUI-USDT', 'SPK-USDT', 'ORDI-USDT', 'BTC-USDT', 'CYBER-USDT', 'FARTCOIN-USDT', 'KAS-USDT', 'AXS-USDT', 'ADA-USDT', 'TIA-USDT', 'GMT-USDT', 'HOME-USDT', 'WIF-USDT', 'XMR-USDT', 'RUNE-USDT', 'XEC-USDT', 'PLUME-USDT', 'DYDX-USDT', 'HYPER-USDT', 'SOL-USDT', 'DOGE-USDT', 'XLM-USDT', 'NEWT-USDT', 'AAVE-USDT', 'ERA-USDT', 'OKB-USDT', 'ENS-USDT', 'DOT-USDT', 'SAHARA-USDT', 'LINEA-USDT', 'ONDO-USDT', 'GMX-USDT', 'NEAR-USDT', 'PEOPLE-USDT', 'HYPE-USDT', 'POPCAT-USDT', 'ASTER-USDT', 'SOMI-USDT', 'XNY-USDT', 'TREE-USDT', 'BONK-USDT', 'CFX-USDT', 'RAY-USDT', 'ATOM-USDT', 'BIO-USDT', 'LDO-USDT', 'BSV-USDT', 'WLFI-USDT', 'AR-USDT', 'FLOKI-USDT', 'C-USDT', 'INJ-USDT', 'EGLD-USDT', 'CUDIS-USDT', 'M-USDT', 'UNI-USDT', 'THETA-USDT', 'IMX-USDT', 'HBAR-USDT', 'VET-USDT', 'SXT-USDT', 'ENA-USDT', 'NXPC-USDT', 'APE-USDT', 'CRV-USDT']  #['ATOM-USDT','DYDX-USDT','DOT-USDT','TRX-USDT','BNB-USDT','BTC-USDT','ETH-USDT']
        tasks = [self.run(s) for s in symbols]
        await asyncio.gather(*tasks)
    
    async def run(self, symbol):
        if self.clear_pos:
            #res = await self.rest5.order_cancel(symbol=symbol, quan=True)
            #print(f"撤单:{res}")
            acc1 = self.rest5   # 账户
            acc2 = self.rest5   # 账户
            acc1_price = 0.00000942   # 价格
            acc2_price = 0.00000942   # 价格
            vol = 10000           # 数量
            acc1_od_type = ocw.OrderType.sell_limit # 账户1方向
            acc2_od_type = ocw.OrderType.buy_limit  # 账户2方向
            #res = await acc1.cancel_order_batch(symbol = symbol)
            #print(res)
            precision = await acc1.get_precision(symbol)
            for i in range(1):
                res1 = await acc1.order_create(symbol, od_type=acc1_od_type, \
                                    price=acc1_price, amount=vol, \
                                    precision=precision, contract_type='close', quan=True)
                res2 = await acc2.order_create(symbol, od_type=acc2_od_type, \
                                    price=acc2_price, amount=vol, \
                                    precision=precision, contract_type='close', quan=True)
                print(f"下单回报:{res1}\n{res2}")
        else:
            acc1 = self.rest5
            acc2 = self.rest5
            precision = await acc1.get_precision(symbol, quan=True)
            pos = await acc1.get_position(symbol, quan=True)
            pos_side = 0
            for i in pos:
                if i.type == 1:      # 多单
                    pos_side += i.amount
                elif i.type == 2:    # 空单
                    pos_side -= i.amount
            data = await self.rest1.get_kline(symbol, ocw.Kline.min1, quan=True)
            sum_vol = 0
            # print(data)
            for i in data:
                sum_vol += int(i['amount'])
            ave_vol_1min = sum_vol/len(data)
            min_add_ratio = 0.1     # 每分钟增加成交量比例
            add_vol = ave_vol_1min*min_add_ratio
            vol = random.randint(int(add_vol*0.5), int(add_vol*1.5))
            loop_num = abs(int(pos_side/add_vol))
            print(f"{symbol}1分钟平均成交量:{ave_vol_1min} 单笔增加:{add_vol} 循环次数:{loop_num}")
            # return
            acc1_od_type = ocw.OrderType.sell_limit if pos_side > 0 else ocw.OrderType.buy_limit
            acc2_od_type = ocw.OrderType.buy_limit if pos_side > 0 else ocw.OrderType.sell_limit
            
            #loop_num = 1
            for _ in range(loop_num):
                loop_time = random.randint(10, 20)
                depth = await acc1.get_depth(symbol, 1, quan=True)
                bid = depth.bids[0].price
                ask = depth.asks[0].price
                acc1_price = bid if pos_side > 0 else ask
                acc2_price = ask if pos_side > 0 else bid
                print(f"{symbol}下单:{acc1_od_type} {acc1_price} {vol}")
                print(f"{symbol}下单:{acc2_od_type} {acc2_price} {vol}")
                try:
                    res1 = await acc1.order_create(symbol, od_type=acc1_od_type, \
                                            price=acc1_price, amount=vol, \
                                            precision=precision, contract_type='close', quan=True)
                except:
                    pass
                try:
                    res2 = await acc2.order_create(symbol, od_type=acc2_od_type, \
                                            price=acc2_price, amount=vol, \
                                            precision=precision, contract_type='close', quan=True)
                    print(f"下单回报:{res1} {res2}")
                except:
                    pass
                await asyncio.sleep(loop_time)


async def main():
    task = takeOverStrategy()
    while 1:
        try:
            await task.main()  # 主循环
        except:
            try:
                print(f'策略报错!\n{traceback.format_exc()}')
            except:
                pass
        time.sleep(10)

asyncio.run(main())





