'''
    统计持仓浮动盈亏达到阈值的用户,发送tg
    版本信息:v1.0.0
    日期:2025-10-20
    作者:sky

    每15分钟通过fetch_hold_list接口统计一次持仓盈亏
    将盈亏超过阈值的用户的持仓信息发送到tg群

    TODO:
    后续增加根据盈亏统计用户交易情况的数据展示
'''

import os
import sys
import time
import datetime
import random
import traceback
import asyncio
import pytz
import pandas as pd
import numpy as np
import mysql.connector
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from client.env_pro.rest.websea.contract import WebseaContract as old_contract
# from crypto_center.client.rest.websea import contract_quan
from crypto_center.client.rest.binance import u_contract as bn_rest
# from client.env_dev.rest.websea_contract import WebseaContract as ws_contract_rest    # 测试环境
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class fund(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()         # 读取配置
        self.symbol_pos = {}        # 交易对持仓
        self.symbol_size = {}       # 交易对合约单位
        self.chat_id = -4917988058  # tg群编号
        self.profit_limit = 10000   # 盈亏阈值
        self.filter_users = []      # 标记组用户列表
        self.db = 1
        self.rc_task = rc.RestClient()

    def _load_config(self):
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        self.old_rest = old_contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口
        self.rest1 = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp',dev=False)    # 近盘口

    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        # self.redis_pool = MyAioredis(test=False)
        # self.redis_conn = await self.redis_pool.open()
        # await asyncio.sleep(1)
        # 订阅外盘一档数据
        # self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        # await asyncio.sleep(1)
        # await self.redis_conn.sub_channel(f"market.profit.hold")          # 普通用户持仓变化推送,用于adl策略
        # await self.redis_conn.sub_channel(f"market.documentary.hold.19429")  # 保本跟单对冲用户推送（特殊标签推送）
        # await self.redis_conn.sub_channel(f"market.normal.order.uid")       # 订单推送（全量全状态） 没有数据
        # await self.redis_conn.sub_channel(f"market.profit.hold")          # 普通用户持仓变化推送,用于adl策略
        # {'userId': 19429, 'userUid': 34551221, 'symbol': 'ETH-USDT', 'markPrice': '4314.4353255823335', 'avgPrice': '4315.37', 'openDirection': 1, 'amount': '1'}
        # await self.redis_conn.sub_channel(f"market.single.hold.uid")    # 风控用户组仓位推送  没有数据
        # await self.redis_conn.sub_channel(f"market.tag.hold.E")       # 标记组持仓推送
        
        # 查询初始数据
        await self.update_tag_user()
        self.log.info(f"策略初始化完成")
        await self.main()
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="*/30"))         # 判断是否需要调整资费
        self.schedule.add_job(self.update_tag_user, CronTrigger(hour="*"))   # 判断是否需要调整资费
    
    ''' ============================================================================'''
    ''' ================================== funcation ==============================='''
    ''' ============================================================================'''
    # 更新标记用户
    async def update_tag_user(self):
        temp_tag_list = []
        last_page = 0
        while 1:
            for i in range(20):
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
                    self.log.info(traceback.format_exc())
                    await asyncio.sleep(5)
            break
                
        tag_users = {}
        for data in temp_tag_list:
            if data['tag'] in tag_users and data['user_id'] not in tag_users[data['tag']]:
                tag_users[data['tag']].append(data['user_id'])
            else:
                tag_users[data['tag']] = [data['user_id']]
        print(tag_users)
        for tag, users in tag_users.items():
            if 'E' in tag:
                self.filter_users += users
        self.log.info(f"E组用户:{self.filter_users}")
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
    
    ''' ============================================================================'''
    ''' ==================================== main =================================='''
    ''' ============================================================================'''
    async def main(self):
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
            # {'data': [{'amount': '1366', 'avgPrice': '4370.027467057101024885', 'isFull': 2, 'is_full': 2, 'mark_price': '4288.350558136167', 
            # 'multiple': 50, 'openDirection': 1, 'open_time': 1756910192, 'profitLoss': '-1115.7065758599587799291', 'profit_loss': '-1115.7065758599587799291', 
            # 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756910192, 'tradeNum': '13.66', 'user_id': '10466', 'user_name': '10466'}] 
            # 'pager': {'bengin_time': 1757395886, 'end_time': 1757395886, 'item_count': 9, 'page': '1', 'page_size': '100', 'total_page': 1}}
        temp_data = []
        for i in temp_hold_list:
            uid = i['user_id']
            if uid in self.filter_users:
                self.log.info(f"{uid}是E组模拟金用户")
                continue
            symbol = i['symbol']
            pos = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
            profit = int(float(i['profit_loss']))
            side = 'long' if i['openDirection'] == 1 else 'short'
            ave_price = round(float(i['avgPrice']), 4)
            mark_price = round(float(i['mark_price']), 4)
            if abs(profit) > self.profit_limit:
                temp_data.append([symbol, uid, profit, side, ave_price, mark_price, pos])
        mess = "用户持仓浮动盈亏数据:\nsymbol  uid  profit  side  open_price  mark_price  pos\n"
        for i in sorted(temp_data, key=lambda x: x[0], reverse=False):
            mess += f"{i[0]} | {i[1]} | {i[2]} | {i[3]} | {i[4]} | {i[5]} | {i[6]}\n"
        print(mess)
        await self.send_tg(self.chat_id, mess)
        
    
    ''' ============================================================================'''
    ''' =================================== mysql =================================='''
    ''' ============================================================================'''
    # 连接数据库
    # async def connect_db(self):
    #     self.db = mysql.connector.connect(
    #         host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
    #         user="contract_user",
    #         password="}jnB+wZ#EgmUpob",
    #         database="contract_db",
    #         port=3306  # 默认端口
    #     )
    #     self.cursor = self.db.cursor()
    
    # # 保存到mysql数据库
    # async def save_mysql(self, save_sql_data, save_sql_filter_data):
    #     try:
    #         await self.connect_db()
    #         insert_sql = "INSERT INTO user_pos_float_profit (pair_name, exchange, fund_rate, fund_value, create_ts, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
    #         self.log.info(f"保存数据:{insert_sql}\n{save_sql_data}")
    #         # 批量插入
    #         self.cursor.executemany(insert_sql, save_sql_data)
    #         self.db.commit()  # 提交事务
    #     except:
    #         mess = f"{datetime.datetime.now()} 保存mysql资金费率数据失败,报错:{traceback.format_exc()}"
    #         self.log.info(mess)
    #         tb.warning(mess, 'risk')
    #         tb.sendmail(f'合约调整资金费率策略保存mysql失败', mess)
    #     try:
    #         insert_sql = "INSERT INTO crypto_filter_funding_rates (pair_name, exchange, fund_rate, fund_value, create_ts, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
    #         self.log.info(f"保存数据:{insert_sql}\n{save_sql_filter_data}")
    #         # 批量插入
    #         self.cursor.executemany(insert_sql, save_sql_filter_data)
    #         self.db.commit()  # 提交事务
    #         # 查询表信息
    #         fund_dict = {}
    #         filter_fund_dict = {}
    #         self.cursor.execute("SELECT * FROM crypto_funding_rates")
    #         for row in self.cursor.fetchall():
    #             fund_dict[row[1]] = [row[3], row[4]]
    #         self.cursor.execute("SELECT * FROM crypto_filter_funding_rates")
    #         for row in self.cursor.fetchall():
    #             filter_fund_dict[row[1]] = [row[3], row[4]]
    #         self.log.info(f"{datetime.datetime.now()} 查询mysql资金费率数据:\n 交易对   没过滤体验金用户的资费   过滤体验金用户的资费\n")
    #         for k, v in sorted(fund_dict.items(), key=lambda x: abs(float(x[1][1])), reverse=True):
    #             self.log.info(f"{k} {round(v[0]*100, 4)}% {round(v[1], 2)}    {round(filter_fund_dict[k][0]*100, 4)}% {round(filter_fund_dict[k][1], 2)}")
    #     except:
    #         mess = f"{datetime.datetime.now()} 保存mysql资金费率数据失败,报错:{traceback.format_exc()}"
    #         self.log.info(mess)
    #         tb.warning(mess, 'risk')
    #         tb.sendmail(f'合约调整资金费率策略保存mysql失败', mess)
    #     self.db.close()  # 关闭数据库连接
    


def main():
    fund().run()

if __name__ == '__main__':
    main()


