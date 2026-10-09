
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
        # self.ws_rest = ws_contract_rest('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.okx_rest2 = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        self.okx_rest3 = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')
        self.tasks = {'e193_带单包赔策略': [self.okx_rest, 'okx_hedge_acct', '超短线-mike'], 
                      'b1c2_实时对冲策略': [self.okx_rest2, 'okx_hedge_acct2', 'Z2组高胜率用户252552'],
                      'b297_网格对冲策略': [self.okx_rest3, 'okx_hedge_acct3', 'Z组长期持仓用户478061']}
        
        self._local_base = tb.LocalDict('local_base_balance.log')
        self.local_base_balance = self._local_base.load().get('balance', {})
        self.hour = 16
        self.minute = 30

    async def on_first(self):
        # await self.save_mysql()
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.save_mysql,CronTrigger(hour="*"))
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
        
    # 对冲
    async def save_mysql(self):
        all_mess = ''
        for name, task in self.tasks.items():
            table_name = task[1]    # mysql表格名称
            user_name = task[2]     # 策略名称
            
            await self.connect_db()
            # 每日0点发送对冲账户权益到tg群 内容:前一天净值、当天净值、盈亏、对冲的是谁
            try:
                date_hour = datetime.datetime.now().hour
                minute = datetime.datetime.now().minute
                if date_hour == self.hour and minute == self.minute:
                # 查询table_name表中24小时前的数据
                    self.cursor2.execute(f"SELECT * FROM okx_hedge_profit")
                    row = self.cursor2.fetchall()[-3:]
                    profit = None
                    profit2 = None
                    for i in row:
                        if user_name == '超短线-mike' and i[1] == 'e193_带单包赔策略':
                            profit = int(i[2])
                            profit2 = int(i[3])
                        elif user_name == 'Z2组高胜率用户252552' and i[1] == 'b1c2_实时对冲策略':
                            profit = int(i[2])
                            profit2 = int(i[3])
                        elif user_name == 'Z组长期持仓用户478061' and i[1] == 'b297_网格对冲策略':
                            profit = int(i[2])
                            profit2 = int(i[3])
                    
                    base_profit = 0
                    if user_name == '超短线-mike':
                        # self.cursor2.execute("SELECT t1.* FROM dwt.contract_protect_trader_d t1 JOIN (SELECT trader_id,MAX(trade_date) As max_date FROM dwt.contract_protect_trader_d GROUP BY trader_id) t2 ON t1.trader_id = t2.trader_id AND t1.trade_date = t2.max_date")
                        self.cursor2.execute("SELECT * FROM dwt.contract_protect_trader_d WHERE name in ('超短线-Mike')")
                        for base_row in self.cursor2.fetchall()[-1:]:
                            if base_row[-1] == '超短线-Mike':
                                base_profit = base_row[2]
                    
                    elif user_name == 'Z2组高胜率用户252552':
                        self.cursor2.execute("SELECT * from contract_dws.contract_order_user_d WHERE userid in (252552)")
                        for base_row in self.cursor2.fetchall()[-1:]:
                            base_profit = base_row[2]
                    
                    elif user_name == 'Z组长期持仓用户478061':
                        self.cursor2.execute("SELECT * from contract_dws.contract_order_user_d WHERE userid in (478061)")
                        for base_row in self.cursor2.fetchall()[-1:]:
                            base_profit = base_row[2]
                    
                    mess = f"{user_name}对冲策略\n昨日盈亏(已实现):{profit}\n昨日盈亏(包含浮动盈亏):{profit2}\n" \
                            f"对应内盘用户的盈亏:{base_profit}\n=================================\n"
                    self.log.info(mess)
                    all_mess += mess
            except:
                mess = f"{datetime.datetime.now()} 发送对冲账户权益到tg群失败,报错:{traceback.format_exc()}"
                self.log.info(mess)
                tb.warning(mess, 'risk')
                tb.sendmail(f'发送tg每日权益失败', mess)
                
        if all_mess != '':
            #await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4574197143, content=all_mess)
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=all_mess)
        self.db.close()   # 关闭数据库连接
        self.db2.close()  # 关闭数据库连接
        
        
    
if __name__ == '__main__':
    Strategy().run()




