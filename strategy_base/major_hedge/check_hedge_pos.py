
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from utils import restclient as rc
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
# from crypto_center.client.rest.websea.contract_quan import WebseaContract
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from crypto_center.client.rest.okex import contract as okx_rest

strategy_name = "指定用户ok网格对冲策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self.last_pos = 0
        self.rc_task = rc.RestClient()
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest = Contract(config['base_token'], config['base_secret'])
        self.token_rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        # self.new_rest = WebseaContract(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'], config['hedge_secret'], config['passphrase'])
        #self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        #self.okx_rest = okx_rest.OkexContract('47a613f4-06de-4bb9-aa7b-d1758d3f224e','6AFCB49A175F20474BE122717DFE52DB','Tbtb794972.')
        #self.okx_rest = okx_rest.OkexContract('02a22951-cc40-4852-ae04-8da58c24df36','C88659593F833C4C316ACD21416DCAE3','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')

    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.hedge, CronTrigger(second="*/10"))
        
    # 对冲
    async def hedge(self):
        while 1:
            #break
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.token_rest.fetch_hold_list(page_size=20, user_id='606019')
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                for n in range(2, hold_list_page+1):
                    data = await self.token_rest.fetch_hold_list(page=n, page_size=20, user_id='606019')
                    temp_hold_list += data['data']
                    await asyncio.sleep(0.5)
                break
            except:
                pass
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        print(f"内盘持仓列表:")
        # if hold_list == []:
        #     tb.warning('迷人等待空仓了，启动策略', 'risk')
        mess = f"{datetime.datetime.now()}\n"
        sum_pos = 0
        for i in hold_list:
            print(i)
            side = '多单' if i['openDirection'] == 1 else '空单'
            vol = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
            sum_pos += vol
            # print(vol, type(vol))
            # amt = abs(vol*0.01*float(i['avgPrice']))
            # level = round(amt/float(i['zhqy']), 1)
            print(f"{i['symbol']} {side} 持仓:{vol} 成本:{i['avgPrice']} 盈亏:{i['profit_loss']}")   # 爆仓价:{i['parity']}, 账户权益:{i['zhqy']}, 真实杠杆:{level}")
            side = '多' if i['openDirection'] == 1 else '空'
            mess += f"--ID:606019 {i['symbol']} 全 {vol}张 ({side} {round(float(vol)*0.01*float(i['avgPrice']), 2)}U) 浮:{round(float(i['profit_loss']), 2)}  成本价:{round(float(i['avgPrice']), 2)}\n"
        
        hedge_pos = await self.okx_rest.fetch_position()
        hedge_mess = (f"外盘对冲持仓:\n")  #{hedge_pos}")
        for i in hedge_pos:
            side = '多' if i['contracts'] > 0 else '空'
            liqPx = round(float(i['info']['liqPx']), 2) if i['info']['liqPx'] != '' else 0
            hedge_mess += (f"{i['symbol']}持仓:{i['contracts']}({side} {round(float(i['contracts'])*float(i['entryPrice']), 2)}U) 浮动盈亏:{round(float(i['unrealizedPnl']), 2)} 成本:{round(float(i['entryPrice']), 2)} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{liqPx}\n")
        
        minute = datetime.datetime.now().minute
        sec = datetime.datetime.now().second
        if minute % 3 == 0 and sec < 9:
            if '606019' in mess:
                mess += hedge_mess
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=mess)
        if self.last_pos == 0:
            self.last_pos = sum_pos
        if self.last_pos != sum_pos:
            tb.warning(f"持仓改变, 上一次持仓:{self.last_pos}, 本次持仓:{sum_pos}", 'risk')
            self.last_pos = sum_pos

def main(path='major_player_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()





