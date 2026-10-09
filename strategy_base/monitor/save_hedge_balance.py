
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
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.okx_rest2 = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        self.okx_rest3 = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')
        self.tasks = {'e193_带单包赔策略': [self.okx_rest, 'okx_hedge_acct', '超短线-mike'], 
                      'b1c2_实时对冲策略': [self.okx_rest2, 'okx_hedge_acct2', 'Z2组高胜率用户535229'],
                      'b297_网格对冲策略': [self.okx_rest3, 'okx_hedge_acct3', 'Z组长期持仓用户478061']}

        self._local_base = tb.LocalDict('local_base_balance.log')
        self.local_base_balance = self._local_base.load().get('balance', {})
        self.hour = 16
        self.minute = 5
        
    async def on_first(self):
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.save_mysql, CronTrigger(hour="*"))
        self.schedule.add_job(self.save_mysql,CronTrigger(hour=self.hour, minute=self.minute))  # 每天 00:22 运行
    
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
            save_sql_data = await self.get_balance(name, task[0])
            table_name = task[1]    # mysql表格名称
            
            await self.connect_db()
            # 每日0点发送对冲账户权益到tg群 内容:前一天净值、当天净值、盈亏、对冲的是谁
            try:
                date_hour = datetime.datetime.now().hour
                date_minute = datetime.datetime.now().minute
                if date_hour == self.hour and date_minute == self.minute:
                    # 查询table_name表中24小时前的数据
                    now = datetime.datetime.now()
                    yesterday = now.date() - datetime.timedelta(days=1) # 计算昨天的日期
                    hour = now.hour     # 当前小时
                    sql = f"""
                        SELECT * 
                        FROM {table_name}
                        WHERE DATE(created_at) = %s
                        AND HOUR(created_at) = %s
                    """
                    self.cursor.execute(sql, (yesterday, hour))
                    row = self.cursor.fetchall()
                    print(name, task, row)
                    #self.cursor.execute(f"SELECT * FROM {table_name}")
                    #row = self.cursor.fetchall()[-25:-24]
                    #print(name, row)
                    profit = round(save_sql_data[0][1]-float(row[0][2]), 2)
                    profit_add_unrealized_pnl = round(save_sql_data[0][2]-float(row[0][3]), 2)
                    
                    # 盈亏存入mysql
                    save_profit_data = [(name, profit, profit_add_unrealized_pnl, datetime.datetime.now().date())]
                    insert_sql = f"INSERT INTO okx_hedge_profit (account_id, profit, profit_add_unrealized_pnl, date) VALUES (%s, %s, %s, %s)"
                    print(f"profit保存数据:{insert_sql}\n{save_profit_data}")
                    # 批量插入
                    self.cursor2.executemany(insert_sql, save_profit_data)
                    self.db2.commit()  # 提交事务
            except:
                mess = f"{datetime.datetime.now()} 保存mysql报错:{traceback.format_exc()}"
                print(mess)
                tb.warning(mess, 'risk')
                tb.sendmail(f'保存mysql报错', mess)

            try:
                insert_sql = f"INSERT INTO {table_name} (account_id, balance, balance_add_unrealized_pnl, free, frozen, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
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
        self.db2.close()  # 关闭数据库连接
        
    
if __name__ == '__main__':
    Strategy().run()




