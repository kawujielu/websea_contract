
import sys
import pytz
import traceback
import importlib
import time
import json
import asyncio
import requests
import adl_designated_user_config
from datetime import datetime
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from utils.ToolBoxNew import ToolBox as tb
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class ADL(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        # 交易所实例初始化
        task = tb()
        accts = task.load_account()
        print(f"账户:{accts}")
        self.accts = []
        for id, token in accts.items():
            task = Contract(token[0], token[1])
            self.accts.append([task, id])
        print(f"账户:{self.accts}")
    
    def _init_params(self):
        self.user_ids = [599442]

    async def on_first(self):
        self.log.add(f"log/adl_designated_user.log", rotation="100 MB", retention=10)
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
    
    async def warning(self, content, contractSymbol='', method='normal'):
        larkDic = {
            'normal': 'https://open.feishu.cn/open-apis/bot/v2/hook/bad3eefd-381c-409e-927d-4900ff58850a',
        }
        if method in larkDic:
            url = larkDic.get(method)
            date = datetime.fromtimestamp(int(time.time()), tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
            content = f"==={date} 报警 {contractSymbol}===\n{content}\n"
            headers = {"Content-Type": "application/json ;charset=utf-8 "}
            msg = {"msg_type": "text",
                   "content": {"text": content}
                  }  # 飞书
            try:
                requests.post(url, headers=headers, data=json.dumps(msg))
            except:
                print(f'飞书报错 {traceback.format_exc()}')
            return
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        for uid in self.user_ids:
            temp_hold_list = []
            last_page = 0
            while 1:
                try:
                    if last_page == 0: 
                        data = await self.accts[0][0].fetch_hold_list(user_id=uid, page_size=100)
                        hold_list_page = data['pager']['total_page']
                        for i in data['data']:
                            if i not in temp_hold_list:
                                temp_hold_list.append(i)
                    begin = 2 if last_page == 0 else last_page
                    for n in range(begin, hold_list_page+1):
                        last_page = n
                        data = await self.accts[0][0].fetch_hold_list(page=n, page_size=100)
                        for i in data['data']:
                            if i not in temp_hold_list:
                                temp_hold_list.append(i)
                        await asyncio.sleep(0.5)
                    break
                except:
                    self.log.info(traceback.format_exc())
                    await asyncio.sleep(5)
        
        for i in temp_hold_list:
            uid = i['user_id']
            # tag = i['tag']
            if uid in self.user_ids:
                symbol = i['symbol']
                amount = i['amount']
                open_time = i['open_time']
                profit = float(i['profitLoss'])
                mmr = float(i['risk_ratio'])
                side = 'LONG' if i['openDirection'] == 1 else 'SHORT'
                margin_type = 'isolated' if i['is_full'] == 1 else 'crossed'
                user_info = [uid, symbol, side, profit, str(mmr)+'%', margin_type]
                print(user_info)
                break
                
                if (profit > self.adl_profit or profit < self.adl_loss):    # and mmr > 20:
                    mess = f"adl策略,查询到达标用户:{user_info} 原始信息:{i}"
                    print(mess)
                    await self.warning(mess, '有用户盈亏达标')
                    for task in self.accts:
                        rest, mm_uid = task
                        data = await rest.adl_order(token=self.adl_token, market=symbol, \
                                                        mmUserId=mm_uid, side=side, adlUserId=uid, \
                                                        adlPercent=self.adl_percent,  marginType=margin_type)  #'isolated,'crossed')
                        print(f"adl下单回报:{data}")
                        if data in [True, '成功成交']:
                            break

async def main(path='adl_designated_user_config.py'):
    config = __import__(path.split(".py")[0])
    task = ADL(config)
    while 1:
        try:
            await task.main()  # 主循环
        except:
            try:
                print(f'策略报错!\n{traceback.format_exc()}')
            except:
                pass
        time.sleep(120)  # TODO 根据adl接口数据更新频率决定sleep间隔

asyncio.run(main())


