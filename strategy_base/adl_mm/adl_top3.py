
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
from utils import restclient as rc


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数
        self.rc_task = rc.RestClient()

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
        self.first = True
        self.last_hour = 0
        self.all_symbols = []
        self.filter_symbols = []
        self.lock = False
            
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
        if self.lock:
            print("策略已锁定,跳过")
            return
        self.lock = True
        if self.first:
            self.first = False
            res = await self.rest18.get_symbols(quan=True)
            for s in res:
                try:
                    data = await self.rest18.get_hr24(s.symbol)
                except:
                    continue
                self.all_symbols.append(s.symbol)
            print(f"初始交易对:{self.all_symbols}")
            
        # 波动最大的交易对
        hour = datetime.now(timezone).hour
        if hour != self.last_hour:
            self.last_hour = hour
            rate_list = []
            for s in self.all_symbols:
                try:
                    data = await self.rest18.get_hr24(s)
                except:
                    print(f"获取{s}数据报错:{traceback.format_exc()}")
                    continue
                rate = round((data.high/data.low-1)*100, 2)
                rate_list.append([s, rate])
            # rate_list根据[1]从大到小排序
            rate_list.sort(key=lambda x:x[1], reverse=True)
            print(f"波动最大的3个交易对:{rate_list[:3]}")
            for i in rate_list[:3]:
                self.filter_symbols.append(i[0])
            
        hold_list = []
        temp_hold_list = []
        last_page = 0
        while 1:
            try:
                if last_page == 0:
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
            if tag not in ['E7','E8','E12'] and i['symbol'] in self.filter_symbols:  # 过滤保本跟单组用户
                print(f"经过过滤的数据:{i}")
                symbol = i['symbol']
                amount = i['amount']
                open_time = i['open_time']
                profit = float(i['profitLoss'])
                mmr = float(i['risk_ratio'])
                side = 'LONG' if i['openDirection'] == 1 else 'SHORT'
                margin_type = 'isolated' if i['is_full'] == 1 else 'crossed'
                user_info = [uid, symbol, side, profit, str(mmr)+'%', margin_type]
                
                con = uid == self.last_user and \
                    symbol == self.last_symbol and \
                    amount == self.last_symbol and \
                    open_time == self.last_open_time
                if profit > self.adl_profit and not con:    # and mmr > 20:
                    mess = f"adl_top3策略,查询到达标用户:{user_info} 原始信息:{i}"
                    print(mess)
                    await self.warning(mess, '有用户盈亏达标')
                    for task in [(self.rest18, 18), (self.rest19, 19), (self.rest20, 20), 
                                (self.rest21, 21), (self.rest22, 22), (self.rest23, 23), 
                                (self.rest24, 476515), (self.rest25, 465880)]:
                        rest, mm_uid = task
                        adl_percent = 100 if profit >=self.adl_profit2 else self.adl_percent
                        data = await rest.adl_order(token=self.adl_token, market=symbol, \
                                                        mmUserId=mm_uid, side=side, adlUserId=uid, \
                                                        adlPercent=adl_percent,  marginType=margin_type)  #'isolated,'crossed')
                        print(f"adl下单回报:{data}")
                        mess = f"adl_top3策略,执行adl操作,用户:{uid} mm:{mm_uid} {symbol} 盈亏:{profit} adl比例:{adl_percent} adl执行回报:{data}"
                        print(mess)
                        await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4945950334, content=mess)
                        self.last_user = uid
                        self.last_symbol = symbol
                        self.last_amount = amount
                        self.last_open_time = open_time
                        if data in [True, '成功成交']:
                            break
        print("结束循环")
        self.lock = False

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
        time.sleep(300)  # TODO 根据adl接口数据更新频率决定sleep间隔

asyncio.run(main())


