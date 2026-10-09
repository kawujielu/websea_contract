'''
    实时跟单策略
'''

import sys
import time
import copy
import datetime
import requests
import traceback
import asyncio
import json
import numpy as np
sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.okex import contract as okx_rest

strategy_name = "follow_hedge实时对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.rc_task = rc.RestClient()
        self.okx_rest1 = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.okx_rest2 = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        self.okx_rest3 = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')
        
    async def on_first(self):
        await self.risk()
        pass
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.risk, CronTrigger(hour="*"))
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self):
        try:
            # 获取外盘权益
            res = await self.okx_rest1.fetch_balance()
            text = f"e193_带单包赔策略账户净值:{round(res['USDT']['total'])}\n"
            res = await self.okx_rest2.fetch_balance()
            text += f"b1c2_实时对冲策略账户净值:{round(res['USDT']['total'])}\n"
            res = await self.okx_rest3.fetch_balance()
            text += f"b297_网格对冲策略账户净值:{round(res['USDT']['total'])}\n"
            
            # 获取外盘仓位
            text += 'e193_带单包赔策略:\n'
            res = await self.okx_rest1.fetch_position()  # 仓位带正负
            sum_profit = 0
            for i in res:
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{round(i['unrealizedPnl'],2)} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
                sum_profit += round(i['unrealizedPnl'],2)
            text += f"e193_总浮动盈亏:{sum_profit}\n"
            
            text += 'b1c2_实时对冲策略:\n'
            res = await self.okx_rest2.fetch_position()  # 仓位带正负
            sum_profit = 0
            for i in res:
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{round(i['unrealizedPnl'],2)} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
                sum_profit += round(i['unrealizedPnl'],2)
            text += f"b1c2_总浮动盈亏:{sum_profit}\n"
            
            text += 'b297_网格对冲策略:\n'
            res = await self.okx_rest3.fetch_position()  # 仓位带正负
            sum_profit = 0
            for i in res:
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{round(i['unrealizedPnl'],2)} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
                sum_profit += round(i['unrealizedPnl'],2)
            text += f"b297_总浮动盈亏:{sum_profit}"
            
            self.log.info(text)
            tb.warning(text, 'notice')
            
            # 发送tg
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=text)
            
        except:
            self.log.error(f"风险验证报错 {traceback.format_exc()}")


def main():
    Strategy().run()



if __name__ == '__main__':
    main()



