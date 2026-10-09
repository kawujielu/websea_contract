
import sys
import datetime
import traceback
import asyncio
import mysql.connector
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
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
        self.minute = 5
        
    async def on_first(self):
        # await self.main()
        pass
    
    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.main,CronTrigger(hour="*"))
        self.schedule.add_job(self.main,CronTrigger(hour="*", minute=self.minute))  # 每天 00:22 运行
    
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
        
    # main
    async def main(self):
        all_mess = ''
        # 每日0点发送对冲账户权益到tg群 内容:前一天净值、当天净值、盈亏、对冲的是谁
        try:
            await self.connect_db()
            xinhuo, yuzong, zifei = 0, 0, 0
            self.cursor.execute(f"SELECT * FROM hedge_pnl_monitor")
            row = self.cursor.fetchall()
            for i in row:
                if xinhuo == 0 and '新火' in i[1]:
                    xinhuo = i[2]
                elif yuzong == 0 and '余总' in i[1]:
                    yuzong = i[2]
                elif zifei == 0 and '外部资费套利团队' in i[1]:
                    zifei = i[2]
            self.cursor.execute(f"SELECT * FROM hedge_pnl_monitor")
            row = self.cursor.fetchall()[-3:]
            for i in row:
                # 增加8小时
                # date = datetime.datetime.strptime(i[-1], '%Y-%m-%d %H:%M:%S')
                date = i[-1] + datetime.timedelta(hours=8)
                if '新火' in i[1]:
                    profit = i[2] - xinhuo
                    all_mess += f"{i[1]}\n{date}\n净值:{i[2]}\n包含未实现盈亏的净值:{i[3]}\n初始净值:{xinhuo}\n盈亏:{profit}\n\n"
                elif '余总' in i[1]:
                    profit = i[2] - yuzong
                    all_mess += f"{i[1]}\n{date}\n净值:{i[2]}\n包含未实现盈亏的净值:{i[3]}\n初始净值:{yuzong}\n盈亏:{profit}\n\n"
                elif '外部资费套利团队' in i[1]:
                    profit = i[2] - zifei
                    all_mess += f"{i[1]}\n{date}\n净值:{i[2]}\n包含未实现盈亏的净值:{i[3]}\n初始净值:{zifei}\n盈亏:{profit}\n"
            print(all_mess)
        except:
            mess = f"{datetime.datetime.now()} 发送对冲账户权益到tg群失败,报错:{traceback.format_exc()}"
            self.log.warning(mess)
            tb.warning(mess, 'risk')
            
        if all_mess != '':
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=all_mess)
        self.db.close()   # 关闭数据库连接
        
        
    
if __name__ == '__main__':
    Strategy().run()






