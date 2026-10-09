'''
    查询指定symbol的当前持仓
    版本信息:v1.0.0
    日期:2026-04-10
    作者:sky
'''

import sys
import traceback
import asyncio
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


strategy_name = "smart_money_hedge策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.rc_task = rc.RestClient()

        self._load_config()   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
    def _load_config(self):
        """读取配置文件
        """
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.rest.DEBUG = False

    def _initParams(self):
        """初始化参数
        """
        self.symbol = 'DOT-USDT'
        self.last_pos = ""
    
    def _setLocalDict(self):
        """配置本地数据
        """
        pass

    async def on_first(self):
        # await self.risk()
        # exit()
        pass

    async def on_timer(self):
        self.schedule.add_job(self.risk, CronTrigger(minute="*/10"))
        
    async def send_tg(self, mess):
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-5223829567, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-5223829567, content=send_text)
    
    # 发送报警信息    测试成功
    async def send_warning(self, title, warning_mess):
        try:
            print(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            print(f"send_warning发送报警信息失败:{traceback.format_exc()}")
    
    async def risk(self):
        # 获取内盘仓位
        while 1:
            temp_hold_list = []
            try:
                data = await self.rest.fetch_hold_list(page_size=100)
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                print(f"获取到{len(temp_hold_list)}条持仓")
                for n in range(2, hold_list_page+1):
                    data = await self.rest.fetch_hold_list(page=n, page_size=100)
                    temp_hold_list += data['data']
                    await asyncio.sleep(0.5)
                break
            except:
                print(f"获取持仓报错:{traceback.format_exc()}")
            await asyncio.sleep(5)
        base_pos = {}
        sum_pos = 0
        for i in temp_hold_list:
            if i['symbol'] != self.symbol:
                continue
            side = 1 if i['openDirection'] == 1 else -1
            if i['user_id'] in base_pos:
                base_pos[i['user_id']] += float(i['amount'])*side
                sum_pos += float(i['amount'])*side
            else:
                base_pos[i['user_id']] = float(i['amount'])*side
                sum_pos += float(i['amount'])*side
        mess = f"{self.symbol}当前用户持仓:{base_pos}, 总持仓:{sum_pos}"
        print(mess)
        if self.last_pos == "":
            self.last_pos = sum_pos
        elif self.last_pos != "" and self.last_pos != sum_pos:
            await self.send_tg(mess)



def main():
    Strategy().run()


if __name__ == '__main__':
    main()







