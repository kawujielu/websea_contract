
import sys
import pytz
import traceback
import importlib
import time
import asyncio
import adl_designated_user_config
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from utils import restclient as rc
from utils.ToolBoxNew import ToolBox as tb
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class ADL(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数
        self.rc_task = rc.RestClient()

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        # 交易所实例初始化
        task = tb()
        accts = task.load_account()
        # print(f"账户:{accts}")
        self.accts = []
        for id, token in accts.items():
            task = Contract(token[0], token[1])
            task.DEBUG = False
            self.accts.append([task, id])
        # print(f"账户:{self.accts}")
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef', '', dev=False)
        self.rest.DEBUG = False
        self.rest.rest_timeout = 60
        self.adl_profit = 100
    
    def _init_params(self):
        # self.symbols = ['SENT-USDT']
        pass

    async def on_first(self):
        self.log.add(f"log/adl_for_lab.log", rotation="100 MB", retention=10)
        self.log.info(f"初始化完成")

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/10"))  # 每1s执行一次
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*/5"))  # 每5min执行一次

    async def reload_config(self):
        try:
            importlib.reload(adl_designated_user_config)
            [setattr(self, k, v) for k, v in vars(adl_designated_user_config).items()]
        except:
            self.log.warning(f"reload_config报错{traceback.format_exc()}")
    
    async def send_tg(self, mess):
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=1843312449, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=1843312449, content=send_text)
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        # for s in self.symbols:
        temp_hold_list = []
        last_page = 0
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_hold_list(page_size=100)
                    hold_list_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in temp_hold_list:
                            temp_hold_list.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, hold_list_page+1):
                    last_page = n
                    data = await self.rest.fetch_hold_list(page=n, page_size=100)
                    for i in data['data']:
                        if i not in temp_hold_list:
                            temp_hold_list.append(i)
                    await asyncio.sleep(0.5)
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
        self.log.info(f"查询到{len(temp_hold_list)}条持仓")
        for i in temp_hold_list:
            uid = i['user_id']
            symbol = i['symbol']
            if symbol in ['LAB-USDT']:
                # amount = i['amount']
                # open_time = i['open_time']
                profit = float(i['profitLoss'])
                # mmr = float(i['risk_ratio'])
                side = 'LONG' if i['openDirection'] == 1 else 'SHORT'
                margin_type = 'isolated' if i['is_full'] == 1 else 'crossed'
                user_info = [uid, symbol, side, profit, margin_type]
                # print(user_info)
                # break
                
                if profit > self.adl_profit:   # or profit < self.adl_loss):    # and mmr > 20:
                    mess = f"adl策略,查询到达标用户:{user_info} 原始信息:{i}"
                    print(mess)
                    # await self.send_tg(mess)
                    # await self.warning(mess, '有用户盈亏达标')
                    for task in self.accts:
                        rest, mm_uid = task
                        data = await self.rest.adl_order(token=self.adl_token, market=symbol, \
                                                    mm_user_id=mm_uid, side=side, adl_user_id=uid, \
                                                    adl_percent=50,  margin_type=margin_type)  #'isolated,'crossed')
                        print(f"adl下单回报:{data}")
                        if data['msg'] == '成功成交':
                            break



def main(config=adl_designated_user_config):
    ADL(config).run()


if __name__ == '__main__':
    main()


