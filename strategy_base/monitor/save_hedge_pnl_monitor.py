
import sys
import datetime
import traceback
import asyncio
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
        self.okx_rest = okx_rest.OkexContract('02a22951-cc40-4852-ae04-8da58c24df36','C88659593F833C4C316ACD21416DCAE3','Tbtb794972.')
        self.okx_rest2 = okx_rest.OkexContract('47a613f4-06de-4bb9-aa7b-d1758d3f224e','6AFCB49A175F20474BE122717DFE52DB','Tbtb794972.')
        self.okx_rest3 = okx_rest.OkexContract('1fc38746-c7aa-4680-800e-8767b17d8a28','B649D39D00758BE695FA0475764F1BA5','Tbtb794972.')
        self.tasks = {'新火量化专用对冲账户': self.okx_rest,
                      '余总团队专用对冲账户': self.okx_rest2,
                      '外部资费套利团队专用对冲账户': self.okx_rest3}
        
    async def on_first(self):
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.save_mysql, CronTrigger(hour="*"))
        
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
        
    async def get_balance(self, name, task):
        res = await task.fetch_balance()
        balance = int(res['USDT']['total'])
        free = int(res['USDT']['free'])
        frozen = int(res['USDT']['used'])
        ts = datetime.datetime.now()
        # 把持仓浮动盈亏加上
        pos = await task.fetch_position()
        pos_profit = 0
        for i in pos:
            pos_profit += i['unrealizedPnl']
        print(f"{name}权益:{balance}, 持仓浮动盈亏:{pos_profit}, 可用:{free}, 冻结:{frozen}, 时间:{ts}")
        save_sql_data = [(name, balance, round(balance+pos_profit,2), free, frozen, ts)]
        return save_sql_data
        
    # 对冲
    async def save_mysql(self):
        for name, task in self.tasks.items():
            save_sql_data = await self.get_balance(name, task)
            
            await self.connect_db()
            try:
                insert_sql = f"INSERT INTO hedge_pnl_monitor (account_name, balance, balance_add_unrealized_pnl, free, frozen, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
                print(f"保存数据:{insert_sql}\n{save_sql_data}")
                # 批量插入
                self.cursor.executemany(insert_sql, save_sql_data)
                self.db.commit()  # 提交事务
            except:
                mess = f"{datetime.datetime.now()} 对冲账户权益数据保存mysql失败,报错:{traceback.format_exc()}"
                print(mess)
                tb.warning(mess, 'risk')
                tb.sendmail(f'对冲账户权益保存mysql失败', mess)
        self.db.close()   # 关闭数据库连接
        
    
if __name__ == '__main__':
    Strategy().run()




