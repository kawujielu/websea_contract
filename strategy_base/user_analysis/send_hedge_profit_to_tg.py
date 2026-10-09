'''
    统计每日对冲用户的内外盘盈亏
    版本信息:v1.0.0
    日期:2025-10-25
    作者:sky
'''

import os
import sys
import datetime
import traceback
import asyncio
from pytz import timezone
import mysql.connector
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
from utils import restclient as rc
from utils import Toolbox as tb

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.rc_task = rc.RestClient()
        self.hour = 0
        self.minute = 0

    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        self.log.info(f"策略初始化完成")
        # await self.main()
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(hour=f"8", minute="1", timezone=timezone("Asia/Shanghai")))
    
    ''' ============================================================================'''
    ''' ================================== mysql ==================================='''
    ''' ============================================================================'''
    # 连接数据库
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_db",
            port=3306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
        self.db2 = mysql.connector.connect(
            host="10.0.208.249",
            user="lh_sky",
            password="sky_123456",
            database="contract_dws",
            port=33306  # 默认端口
        )
        self.cursor2 = self.db2.cursor()
        
    ''' ============================================================================'''
    ''' ================================= strategy ================================='''
    ''' ============================================================================'''
    async def main(self):
        try:
            await self.connect_db()
            mess = ""
            today = datetime.datetime.now().day
            for i in [[485087,'okx_hedge_acct'], [540519,'okx_hedge_acct2'], [580609,'okx_hedge_acct3']]:
                id = i[0]
                table = i[1]
                # 查询内盘用户每日盈亏
                # self.cursor2.execute("SHOW COLUMNS FROM contract_dws.contract_order_user_d")
                self.cursor2.execute(f"SELECT * from contract_dws.contract_order_user_d WHERE user_id = ({id})")
                user_profit = 0
                for row in self.cursor2.fetchall()[-3:]:
                    if row[0].day == int(today-1):
                        user_profit = row[2]
                        print(f"内盘数据:{row}")
                
                # 查询外盘对冲盈亏
                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                yesterday_begin = int(today-2)
                yesterday_end = int(today-1)
                self.cursor.execute(f"SELECT * FROM {table}")
                for row in self.cursor.fetchall()[-80:]: 
                    # print(yesterday_begin, row[-1].day, row[-1].hour)
                    if row[-1].day == yesterday_begin and row[-1].hour == 16:
                        balance_begin = float(row[2])
                        print(f"昨日起始:{row}")
                    if row[-1].day == yesterday_end and row[-1].hour == 16:
                        balance_end = float(row[2])
                        print(f"昨日结束:{row}")
                hedge_profit = round(balance_end-balance_begin, 2)
                mess += f"{yesterday}内盘{id}用户盈亏:{user_profit}对冲盈亏:{hedge_profit}\n"
            print(mess)
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4917988058, content=mess)
        except:
            print(f"{datetime.datetime.now()} 查询mysql对冲账户权益数据失败,报错:{traceback.format_exc()}")
        
        
    
if __name__ == '__main__':
    Strategy().run()





