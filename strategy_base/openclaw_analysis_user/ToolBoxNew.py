'''
    统计所有做市账户每个交易对的净持仓
    版本信息:v1.0.0
    日期:2025-10-09
    作者:sky
'''

import os
import sys
import json
import datetime
import traceback
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

sys.path.append('../..')
from utils import restclient as rc
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class ToolBox():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        # super().__init__(scheduler=True, gcc=True)
        self.api_key_list = {}
        self._load_config()          # 读取配置
        self.rc_task = rc.RestClient()

    def _load_config(self):
        # self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        # self.old_rest = old_contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')       # 近盘口
        # self.rest = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp',dev=False)    # 近盘口 19
        # self.rest2 = Contract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1',dev=False)    # 远盘口 20
        # self.rest3 = Contract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k',dev=False)    # 补挡位 
        # self.rest4 = Contract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr',dev=False)    # 刷量策略 18
        # self.rest5 = Contract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz',dev=False)    # 新策略 22
        # self.rest6 = Contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4',dev=False) # 压盘口策略 
        # self.rest7 = Contract('5accd1fb1d03e26f299a4297d9q52646135','nyuc69aljff4g297uj99',dev=False) # MH合约 
        # self.rest8 = Contract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3',dev=False)    # 防守策略 23
        # self.rest9 = Contract('fd0bc370e0f8dc47a923f54fca31d3d2','zbihesbcp6yz5j0awlqv',dev=False)    # 新铺单策略-2 24
        # self.rest10 = Contract('d6678ea254de541f67594668b8ee7937','tz1rtf00gudmgch3jchl',dev=False)   # 新铺单策略-3
        # self.rest11 = Contract('57b49384ee4e0b236f0f38a45fj50733190','bu8qqmoyaq0o10xeepdd',dev=False)
        # self.bn_rest = bn_rest.UBinanceContract('AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi','FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj')
        api_key_list = self.load_account()
        self.rest = Contract(api_key_list['risk'][0], api_key_list['risk'][1], dev=False)
        self.rest.DEBUG = False
    
    # 读取账户配置
    def load_account(self):
        api_key_list = {}
        file_path = "/home/ubuntu/CCGo/resources/account.config.json"
        with open(file_path, 'r') as f:
            data = json.load(f)
        for k,v in data['websea'].items():
            if "测试" not in v['description']:
                id = v['uid'].split(',')[0] if ',' in v['uid'] else v['uid']
                if id == '':
                    id = 'risk'
                api_key_list[id] = [v['apikey'], v['secret']]
        # print(f"api_key:{api_key_list}")
        return api_key_list
    
    # 获取所有交易对
    async def get_all_symbols(self):
        res = await self.rest.fetch_symbol_info()
        all_symbols = list(res.keys())
        for s in all_symbols[-3:]:
            try:
                depth = await self.rest.fetch_depth(s)
            except:
                self.log.warning(f"获取{s}深度失败,报错:{traceback.format_exc()}")
                await asyncio.sleep(0.1)
                continue
            if depth['bids'] == [] and depth['asks'] == []:
                await asyncio.sleep(0.1)
                continue
            all_symbols.append(s)    # 交易对,交易对id
            await asyncio.sleep(0.1)
        return all_symbols
    
    # 查询合约单位
    async def get_symbol_unit(self):
        symbol_unit = {}
        while True:
            try:
                res = await self.rest.fetch_symbol_info()
                for s, v in res.items():
                    symbol_unit[s] = v['contractSize']
                print(f"初始化合约单位:{symbol_unit}")
                break
            except:
                print(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)
        return symbol_unit
    
    # 获取模拟金用户
    async def get_sim_user(self):
        temp_tag_list = []
        filter_users = []
        last_page = 0
        while 1:
            for i in range(50):
                num = i+1
                try:
                    if last_page == 0:
                        data = await self.rest.fetch_tag_list(tag=f'E{num}', page=1, page_size=1000)
                        tag_list_page = data['pager']['total_page']
                        for i in data['data']:
                            if i not in temp_tag_list:
                                temp_tag_list.append(i)
                    begin = 2 if last_page == 0 else last_page
                    for n in range(begin, tag_list_page+1):
                        last_page = n
                        data = await self.rest.fetch_tag_list(tag=f'E{num}', page=n, page_size=1000)
                        for i in data['data']:
                            if i not in temp_tag_list:
                                temp_tag_list.append(i)
                        await asyncio.sleep(0.5)
                    continue
                except:
                    print(traceback.format_exc())
                    await asyncio.sleep(5)
            break
                
        tag_users = {}
        for data in temp_tag_list:
            if data['tag'] in tag_users and data['user_id'] not in tag_users[data['tag']]:
                tag_users[data['tag']].append(data['user_id'])
            else:
                tag_users[data['tag']] = [data['user_id']]
        for tag, users in tag_users.items():
            if 'E' in tag:
                filter_users += users
        # print(f"E组用户:{filter_users}")
        return filter_users
        
    # 发送tg消息
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
        
    
            
    

if __name__ == '__main__':
    main()





