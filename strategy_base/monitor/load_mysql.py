
import sys
import datetime
import traceback
import asyncio
import mysql.connector
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.okex import contract as okx_rest

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.account_id = 'e1934f97'
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        
    async def on_first(self):
        await self.load_mysql()
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
    
    # 连接数据库
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_swap_db",   #"contract_db",
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

    # 对冲
    async def load_mysql(self):
        try:
            await self.connect_db()
            # 查询表信息
            #self.cursor2.execute("SHOW COLUMNS FROM contract_dws.contract_order_user_d")
            #self.cursor.execute("SELECT symbol FROM cfg_symbol_source WHERE account_id NOT IN ('pro_5')")
            #self.cursor.execute("SELECT * FROM okx_hedge_acct")
            #for row in self.cursor.fetchall()[-24:]:
            #    print(row)
            #return
            #self.cursor2.execute("SHOW COLUMNS FROM contract_dws.fund_cost")  # 查看表结构
            #self.cursor2.execute("ALTER TABLE fund_cost MODIFY COLUMN sim_cost DECIMAL(20,4) NOT NULL COMMENT '做市账户资费金额'") # 修改表字段
            #self.cursor2.execute("SELECT * from contract_dws.contract_order_user_d WHERE user_id = (485142)")
            #self.cursor2.execute("SELECT user_id,SUM(vol) AS total_vol FROM user_vol_rank WHERE date BETWEEN '2025-11-24' AND '2025-11-30' GROUP BY user_id ORDER BY total_vol DESC LIMIT 11")
            self.cursor2.execute("SELECT * from user_vol_rank WHERE user_id = 605434")
            #print(f"{datetime.datetime.now()} 查询mysql对冲账户权益数据:")
            for row in self.cursor2.fetchall():
                print(row)
        except:
            mess = f"{datetime.datetime.now()} 对冲账户权益数据保存mysql失败,报错:{traceback.format_exc()}"
            print(mess)
            #tb.warning(mess, 'risk')
            #tb.sendmail(f'对冲账户权益保存mysql失败', mess)
        self.db.close()  # 关闭数据库连接
        self.db2.close()

def main():
    Strategy().run()



if __name__ == '__main__':
    main()

