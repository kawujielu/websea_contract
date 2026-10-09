"""
    有对冲交易完成后推送
"""
import sys
import time
import datetime
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
        self.last_mike = 0  # 最新完成成交的持仓
        self.last_z2 = 0
        self.base_mike = 'mike初始化'
        self.base_z2 = 'z2初始化'
        
        self.rc_task = rc.RestClient()
        self.ws_rest = ws_contract_rest('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')
        self.okx_mike = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.okx_Z2 = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        
    async def on_first(self):
        # await self.main()
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="*"))
        
    # 对冲
    async def main(self):
        try:
            await self.mike_monitor()
            await self.Z2_monitor()
        except:
            self.log.error(f"run error: {traceback.format_exc()}")
    
    async def mike_monitor(self):
        res = await self.okx_mike.fetch_position_history(symbol='ETH-USDT', limit=1)
        for i in res:
            # print(i)
            # 完全成交
            if i['info']['type'] == '2':
                date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
                side = 'buy' if i['direction'] == 'long' else'sell'
                hedge_mess = (f"{date} {i['symbol']} 对冲持仓:{round(i['openMaxPos']*0.1, 2)} 方向:{side} 开仓价:{round(float(i['openAvgPrice']), 2)} 平仓价:{round(float(i['closeAvgPrice']), 2)} 盈亏:{round(float(i['realizedPnl']), 2)} 手续费:{round(float(i['fee']), 2)} 资金费率:{round(float(i['info']['fundingFee']), 2)} 杠杆:{i['leverage']}")
                if self.last_mike == 0:
                    self.last_mike = hedge_mess
                    break
                if hedge_mess != self.last_mike:
                # if True:
                    # 发送tg
                    tg_mess = f"mike完成一笔交易\n{hedge_mess}\n{self.base_mike}"
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
                    self.log.info(tg_mess)
                    self.last_mike = hedge_mess
        
        data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id='485142')
        if data['data']['data']:
            data = data['data']['data'][0]
            # print(data)
            profit = data['profit_loss']
            open_price = data['avgPrice']
            amount = float(data['amount'])
            side = 'buy' if data['openDirection'] == 1 else 'sell'
            balance = data['zhqy']
            self.base_mike = f"{i['symbol']} 内盘持仓:{round(amount*0.01, 2)} 方向:{side} 开仓价:{open_price} 盈亏:{profit} 账户余额:{balance}"
        
            
    async def Z2_monitor(self):
        res = await self.okx_Z2.fetch_position_history(symbol='ETH-USDT', limit=1)
        for i in res:
            # print(i)
            # 完全成交
            if i['info']['type'] == '2':
                date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
                side = 'buy' if i['direction'] == 'long' else'sell'
                hedge_mess = (f"{date} {i['symbol']} 对冲持仓:{round(i['openMaxPos']*0.1, 2)} 方向:{side} 开仓价:{round(float(i['openAvgPrice']), 2)} 平仓价:{round(float(i['closeAvgPrice']), 2)} 盈亏:{round(float(i['realizedPnl']), 2)} 手续费:{round(float(i['fee']), 2)} 资金费率:{round(float(i['info']['fundingFee']), 2)} 杠杆:{i['leverage']}")
                if self.last_z2 == 0:
                    self.last_z2 = hedge_mess
                    break
                if hedge_mess != self.last_z2:
                # if True:
                    # 发送tg
                    tg_mess = f"252552完成一笔交易\n{hedge_mess}\n{self.base_z2}"
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=tg_mess)
                    self.log.info(tg_mess)
                    self.last_z2 = hedge_mess
        
        data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id='252552')
        if data['data']['data']:
            data = data['data']['data'][0]
            # print(data)
            profit = data['profit_loss']
            open_price = data['avgPrice']
            amount = float(data['amount'])
            side = 'buy' if data['openDirection'] == 1 else 'sell'
            balance = data['zhqy']
            self.base_z2 = f"{i['symbol']} 内盘持仓:{round(amount*0.01, 2)} 方向:{side} 开仓价:{open_price} 盈亏:{profit} 账户余额:{balance}"
        

def main():
    Strategy().run()



if __name__ == '__main__':
    main()


