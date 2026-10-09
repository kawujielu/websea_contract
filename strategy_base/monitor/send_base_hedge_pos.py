"""
    有对冲交易完成后推送
"""
import sys
import time
import datetime
import numpy as np
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
from utils import restclient as rc

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.symbol_size = {}   # 保存合约单位
        self.symbol_precision = {}   # 保存合约精度
        
        self.rc_task = rc.RestClient()
        self.ws_rest = ws_contract_rest('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')
        self.okx_mike = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.okx_Z2 = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        
    async def on_first(self):
        await self.get_symbol_unit()        # 内盘合约单位
        await self.hedge_contract_info()    # 外盘合约精度
        # await self.main()
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="*/10"))    # (second="*/3")
    
    async def get_symbol_unit(self):
        res = await self.ws_rest.get_symbols(quan=True)
        for s in res:
            self.symbol_size[s.symbol] = s.contract_size
    
    async def hedge_contract_info(self):
        res = await self.okx_Z2.fetch_precision()
        for s, v in res.items():
            face_value = v['faceValue']  # 合约面值,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']   # 最小交易数量
            self.symbol_precision[s] = face_value*hedge_vol_limit
    
    async def main(self):
        try:
            await self.mike_monitor()
            #await self.Z2_monitor()
        except:
            self.log.error(f"run error: {traceback.format_exc()}")
    
    async def mike_monitor(self):
        hedge_mess = f"{datetime.datetime.now()} mike带单员\n"
        base_mike = 0
        hedge_dict = {}
        res = await self.okx_mike.fetch_position()
        for i in res:
            if abs(i['contracts']) < self.symbol_precision[i['symbol']]:
                continue
            side = 'buy' if i['contracts'] > 0 else 'sell'
            hedge_mess += (f"{i['symbol']} 对冲持仓:{i['contracts']} 方向:{side} 开仓价:{round(float(i['entryPrice']), 2)} 盈亏:{round(float(i['unrealizedPnl']), 2)} 手续费:{round(float(i['info']['fee']), 2)} 杠杆:{i['leverage']}")
            hedge_dict[i['symbol']] = hedge_mess
        
        base_dict = {}
        data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id='485142')
        for i in data['data']['data']:
            # print(data)
            profit = i['profit_loss']
            open_price = i['avgPrice']
            side = 'buy' if i['openDirection'] == 1 else 'sell'
            side_type = 1 if i['openDirection'] == 1 else -1
            amount = float(i['amount'])*side_type*self.symbol_size[i['symbol']]
            balance = i['zhqy']
            base_mike = f"{i['symbol']} 内盘持仓:{round(amount, 2)} 方向:{side} 开仓价:{open_price} 盈亏:{profit} 账户余额:{balance}"
            base_dict[i['symbol']] = base_mike
            
        # 内盘持仓
        have_symbol = []
        for s, v in base_dict.items():
            have_symbol.append(s)
            hedge_mess = hedge_dict.get(s, f"{datetime.datetime.now()} mike带单员\n{s}没有对冲仓位")
            base_mess = v
            tg_mess = f"{hedge_mess}\n\n{base_mess}"
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
            self.log.info(tg_mess)
        
        # 外盘持仓
        for s, v in hedge_dict.items():
            if s in have_symbol:
                continue
            hedge_mess = v
            base_mess = f"{s}没有内盘仓位"
            tg_mess = f"{hedge_mess}\n\n{base_mess}"
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
            self.log.info(tg_mess)
        
            
    async def Z2_monitor(self):
        hedge_mess = f"{datetime.datetime.now()} 252552用户\n"
        base_z2 = 0
        res = await self.okx_Z2.fetch_position()
        hedge_dict = {}
        for i in res:
            # print(i)
            # 完全成交
            if abs(i['contracts']) < self.symbol_precision[i['symbol']]:
                continue
            side = 'buy' if i['contracts'] > 0 else'sell'
            hedge_mess += (f"{i['symbol']} 对冲持仓:{i['contracts']} 方向:{side} 开仓价:{round(float(i['entryPrice']), 2)} 当前价:{i['markPrice']} 爆仓价:{round(float(i['info']['liqPx']), 2)} 盈亏:{round(float(i['unrealizedPnl']), 2)}(包含手续费、资金费率) 手续费:{round(float(i['info']['fee']), 2)} 杠杆:{i['leverage']}")
            hedge_dict[i['symbol']] = hedge_mess
        
        base_dict = {}
        data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id='252552')
        for i in data['data']['data']:
            # print(data)
            profit = i['profit_loss']
            open_price = i['avgPrice']
            side = 'buy' if i['openDirection'] == 1 else 'sell'
            side_type = 1 if i['openDirection'] == 1 else -1
            amount = float(i['amount'])*side_type*self.symbol_size[i['symbol']]
            balance = i['zhqy']
            mark_price = i['mark_price']
            parity = i['parity']
            base_z2 = f"{i['symbol']} 内盘持仓:{round(amount, 2)} 方向:{side} 开仓价:{open_price} 标记价格:{mark_price} 爆仓价:{parity} 盈亏:{profit} 账户余额:{balance}"
            base_dict[i['symbol']] = base_z2
        
        # 内盘持仓
        have_symbol = []
        for s, v in base_dict.items():
            have_symbol.append(s)
            hedge_mess = hedge_dict.get(s, f"{datetime.datetime.now()} 252552用户\n{s}没有对冲仓位")
            base_mess = v
            tg_mess = f"{hedge_mess}\n\n{base_mess}"
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
            self.log.info(tg_mess)
        
        # 外盘持仓
        for s, v in hedge_dict.items():
            if s in have_symbol:
                continue
            hedge_mess = v
            base_mess = f"{s}没有内盘仓位"
            tg_mess = f"{hedge_mess}\n\n{base_mess}"
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
            self.log.info(tg_mess)
            
        # if hedge_mess or base_z2:
        #     if not hedge_mess:
        #         hedge_mess = "252552没有对冲仓位"
        #     if not base_z2:
        #         base_z2 = "252552没有内盘仓位"
        #     tg_mess = f"{hedge_mess}\n{base_z2}"
        #     await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
        #     self.log.info(tg_mess)

def main():
    Strategy().run()



if __name__ == '__main__':
    main()





