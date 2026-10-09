
import sys
import pytz
import traceback
import time
import json
import asyncio
import requests
from datetime import datetime
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from client.env_pro.rest.websea import contract


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        self.rest18 = contract.WebseaContract(config['tokon1'],config['secret1'])
        self.rest19 = contract.WebseaContract(config['tokon2'],config['secret2'])
        self.rest20 = contract.WebseaContract(config['tokon3'],config['secret3'])
        self.rest21 = contract.WebseaContract(config['tokon4'],config['secret4'])
        self.rest22 = contract.WebseaContract(config['tokon5'],config['secret5'])
        self.rest23 = contract.WebseaContract(config['tokon6'],config['secret6'])
        self.rest24 = contract.WebseaContract(config['tokon7'],config['secret7'])
        self.rest25 = contract.WebseaContract(config['tokon8'],config['secret8'])
    
    def _init_params(self):
        self.last_user = 0
        self.last_symbol = 0
        self.last_amount = 0
        self.last_open_time = 0
    
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
        hold_list = []
        temp_hold_list = []
        last_page = 0
        while 1:
            try:
                data = await self.rest18.hold_list(token=self.adl_token, page_size=20)
                hold_list_page = data['data']['pager']['total_page']
                temp_hold_list += data['data']['data']
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, hold_list_page+1):
                    last_page = n
                    data = await self.rest18.hold_list(token=self.adl_token, page=n, page_size=20)
                    temp_hold_list += data['data']['data']
                for i in temp_hold_list:
                    if i not in hold_list:
                        hold_list.append(i)
                break
            except:
                print(traceback.format_exc())
                await asyncio.sleep(3)
        
        for i in hold_list:
            uid = i['user_id']
            tag = i['tag']
            if tag == 'T':
                # if tag in self.filter_tag:
                    # print(f'{i}被过滤')
                    # continue
                # print(f"原始消息:{i}")
                symbol = i['symbol']
                amount = i['amount']
                open_time = i['open_time']
                profit = float(i['profitLoss'])
                mmr = float(i['risk_ratio'])
                side = 'LONG' if i['openDirection'] == 1 else 'SHORT'
                margin_type = 'isolated' if i['is_full'] == 1 else 'crossed'
                user_info = [uid, symbol, side, profit, str(mmr)+'%', margin_type]
                # con = profit > self.profit_limit

                con = uid == self.last_user and \
                    symbol == self.last_symbol and \
                    amount == self.last_symbol and \
                    open_time == self.last_open_time
                if (profit > self.adl_profit or profit < self.adl_loss) and not con:    # and mmr > 20:
                    mess = f"adl策略,查询到达标用户:{user_info} 原始信息:{i}"
                    print(mess)
                    await self.warning(mess, '有用户盈亏达标')
                    for task in [(self.rest18, 18), (self.rest19, 19), (self.rest20, 20), 
                                (self.rest21, 21), (self.rest22, 22), (self.rest23, 23), 
                                (self.rest24, 476515), (self.rest25, 465880)]:
                        rest, mm_uid = task
                        data = await rest.adl_order(token=self.adl_token, market=symbol, \
                                                        mmUserId=mm_uid, side=side, adlUserId=uid, \
                                                        adlPercent=self.adl_percent,  marginType=margin_type)  #'isolated,'crossed')
                        print(f"adl下单回报:{data}")
                        self.last_user = uid
                        self.last_symbol = symbol
                        self.last_amount = amount
                        self.last_open_time = open_time
                        if data in [True, '成功成交']:
                            break

async def main(path='adl_config.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
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


