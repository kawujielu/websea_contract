
import sys
import pytz
import asyncio
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
# from client.env_pro.rest.websea import contract


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, symbol, side, uid, config):
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数
        self.symbol, self.side, self.id = symbol, side, uid

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.rest1 = Contract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')     # 刷量 18
        self.rest2 = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')     # 近盘口 19
        self.rest3 = Contract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')     # 远盘口 20
        self.rest4 = Contract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')     # 补挡位 21
        self.rest5 = Contract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')     # 新策略 22
        self.rest6 = Contract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3')     # 防守策略 23
        self.rest7 = Contract('fd0bc370e0f8dc47a923f54fca31d3d2','zbihesbcp6yz5j0awlqv')     # 新铺单策略-24
        self.rest8 = Contract('d6678ea254de541f67594668b8ee7937','tz1rtf00gudmgch3jchl')     # 新铺单策略-25
        self.rest9 = Contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')  # 压盘口策略 476515
        self.rest.DEBUG = False
        self.rest6.DEBUG = False
        self.rest7.DEBUG = False
        self.rest8.DEBUG = False

    def _init_params(self):
        self.symbol_size = {}
        self.symbol_price = {}
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        symbol = self.symbol  # 'INJ-USDT'
        mm_uid = 23
        side = self.side  # 'SHORT'
        uid = self.id     # 18
        margin_type = 'isolated'
        adl_percent = 100
        print(f"adl下单: {symbol} {mm_uid} {side} {uid}")
        data = await self.rest.adl_order(token=self.adl_token, market=symbol, \
                        mm_user_id=mm_uid, side=side, adl_user_id=uid, \
                        adl_percent=adl_percent, margin_type=margin_type)  #'isolated,'crossed')
        print(f"adl下单回报:{data}")
                
        # mm_pos = await self.get_pos()
        
        # for id, pos in mm_pos.items():
        #     for s, pos_list in pos.items():
        #         symbol = s
        #         mm_uid = 24 if id == 23 else 23
        #         side = 'LONG' if pos_list[0] > 0 else 'SHORT'
        #         uid = id
        #         margin_type = 'isolated'
        #         adl_percent = 98
        #         data = await self.rest.adl_order(token=self.adl_token, market=symbol, \
        #                         mm_user_id=mm_uid, side=side, adl_user_id=uid, \
        #                         adl_percent=adl_percent, margin_type=margin_type)  #'isolated,'crossed')
        #         print(f"adl下单: {symbol} mm:{mm_uid} {side} adl_id:{uid}")
        #         print(f"adl下单回报:{data}")
        #         await asyncio.sleep(1)
    
    async def get_pos(self):
        data = await self.rest1.fetch_symbol_info()
        for s, i in data.items():
            self.symbol_size[s] = i['contractSize']
        print(self.symbol_size)
        
        mm_pos = {}
        #for num, task in enumerate([self.rest1, self.rest2, self.rest3, self.rest4, self.rest5, self.rest6, self.rest7, self.rest8, self.rest9]):
        for num, task in enumerate([self.rest7, self.rest6]):
            if num == 0:
                id = 23
            elif num == 1:
                id = 24
            elif num == 2:
                id = 20
            elif num == 3:
                id = 21
            elif num == 4:
                id = 22
            elif num == 5:
                id = 23
            elif num == 6:
                id = 24
            elif num == 7:
                id = 25
            elif num == 8:
                id = 476515
                
            pos = await task.fetch_position()
            if pos is None:
                return 0
            symbol_pos = {}
            for i in pos:
                if i['side'] == 'long':      # 多单
                    try:
                        symbol_pos[i['symbol']] += i['contracts']
                    except:
                        symbol_pos[i['symbol']] = i['contracts']
                        self.symbol_price[i['symbol']] = i['entryPrice']
                elif i['side'] == 'short':    # 空单
                    try:
                        symbol_pos[i['symbol']] -= i['contracts']
                    except:
                        symbol_pos[i['symbol']] = i['contracts']*-1
                        self.symbol_price[i['symbol']] = i['entryPrice']
            pos_dict = {}
            amt_sum_long = 0
            amt_sum_short = 0
            for k, v in sorted(symbol_pos.items(), key=lambda x: abs(x[1]), reverse=True):
                if v != 0 and self.symbol_price[k]:
                    pos_amt = round(v*self.symbol_size[k]*self.symbol_price[k])
                    if pos_amt > 0:
                        amt_sum_long += pos_amt
                    else:
                        amt_sum_short += pos_amt
                    if abs(pos_amt) > 0:
                        pos_dict[k] = [v, pos_amt, amt_sum_long, amt_sum_short]
                        # mess += f"【{k}】:仓位{v}张 金额:{pos_amt}usdt\n"
            # 按照字典vol是list的第二个值从大到小排序
            pos_dict = {k: v for k, v in sorted(pos_dict.items(), key=lambda x: x[1][1], reverse=True)}
            mm_pos[id] = pos_dict
            
            mess = ""
            for s, p in pos_dict.items():
                #if abs(p[1]) > 5000000:
                mess += f"【{s}】:仓位{p[0]}张 金额:{p[1]}usdt 多单:{p[2]} 空单:{p[3]}\n"
            print(f"{num}账户持仓: \n{mess}")
        return mm_pos
        

async def main(symbol, side, uid, path='adl_config.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(symbol, side, uid, config)
    await task.main()  # 主循环

asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3]))

