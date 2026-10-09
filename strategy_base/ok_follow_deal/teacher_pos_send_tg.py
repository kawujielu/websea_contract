
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as old_ws_contract_rest
from crypto_center.client.rest.websea.contract_quan import WebseaContract as ws_contract_rest
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接
from crypto_center.client.rest.okex import contract as okx_rest
from utils import restclient as rc

strategy_name = "带单员持仓统计"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self.rc_task = rc.RestClient()
        self.chat_id = -4845897439
        
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.old_ws_rest = old_ws_contract_rest(config['base_token'], config['base_secret'])
        # self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.ws_rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        
    async def on_first(self):
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.run_main, CronTrigger(hour="*"))
        
    # 对冲
    async def run_main(self):
        all_mess = ""
        for id in [[555555, 'James'], [555559, 'one more'], [557412, 'eric_deal'], [557471, '要想富满仓隔夜是条路'], [557472, '404alive'], [557476, '五条悟虚式【茈】'], [557478, 'labubugogogo'], [557479, 'zero量化'], [557501, '酉时三刻'], [557503, '致敬teacher郑']]:
            mess = f"{id[1]} 带单员({id[0]})持仓:\n"
            profit = 0
            while 1:
            #    break
                hold_list = []
                temp_hold_list = []
                try:
                    data = await self.ws_rest.fetch_hold_list(user_id=id[0])  # Z组478061
                    print(data)
                    hold_list_page = data['pager']['total_page']
                    temp_hold_list += data['data']
                    for n in range(2, hold_list_page+1):
                        data = await self.ws_rest.fetch_hold_list(page=n, user_id=id[0])
                        temp_hold_list += data['data']
                        await asyncio.sleep(0.5)
                    break
                except:
                    print(traceback.format_exc())
                await asyncio.sleep(5)
            for i in temp_hold_list:
                if i not in hold_list:
                    hold_list.append(i)
            # print(f"持仓列表:{hold_list}")
            for i in hold_list:
            #    #print(i)
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                profit += float(i['profit_loss'])
                mess += f"{i['symbol']} 持仓:{i['amount']} 方向:{side} 盈亏:{round(float(i['profit_loss']), 2)} 成本:{round(float(i['avgPrice']), 8)} 当前价:{round(float(i['mark_price']), 8)}\n"  # 爆仓价:{i['parity']}\n"
            all_mess += mess + f"总盈亏:{round(profit, 2)}\n\n"
        print(all_mess)
        await self.send_tg(self.chat_id, all_mess)
    
    async def send_tg(self, id, text):
        text_temp = text.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=id, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=id, content=send_text)
        

def main(path='ok_follow_deal_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()





