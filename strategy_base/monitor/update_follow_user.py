'''
    跟单用户更新
    版本信息:v1.0.0
    日期:2025-11-11
    作者:sky

    利用wss接口推送的跟单用户仓位变化,记录跟单用户id
    保存到mysql的follow_users表中
'''

import os
import sys
import datetime
import traceback
import asyncio
import mysql.connector
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class fund(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()         # 读取配置
        self.db = 1
        self.rc_task = rc.RestClient()

    def _load_config(self):
        pass

    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(test=False)
        self.redis_conn = await self.redis_pool.open()
        await asyncio.sleep(0.3)
        # 订阅外盘一档数据
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(0.3)
        await self.redis_conn.sub_channel(f"market.documentary.hold")  # 保本跟单对冲用户推送（特殊标签推送）
        self.log.info(f"策略初始化完成")
        
    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.main, CronTrigger(minute="*/5"))         # 判断是否需要调整资费
    
    ''' ============================================================================'''
    ''' =================================== mysql =================================='''
    ''' ============================================================================'''
    # 连接数据库
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="10.0.208.249",
            user="lh_sky",
            password="sky_123456",
            database="contract_dws",
            port=33306  # 默认端口
        )
        self.cursor = self.db.cursor()
    
    async def del_mysql(self):
        await self.connect_db()
        self.cursor.execute("DROP TABLE fund_cost")
        self.db.commit()
        self.db.close()
    
    async def load_mysql(self):
        await self.connect_db()
        try:
            self.cursor.execute(f"SELECT * FROM fund_cost")
            data = self.cursor.fetchall()
            for i in data:
                print(i)
        except:
            print("没有fund_cost表")
        self.db.close()

    async def create_table(self):
        await self.connect_db()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `fund_cost` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `date` VARCHAR(50) NOT NULL COMMENT '时间',
                `symbol` VARCHAR(50) NOT NULL COMMENT '交易对',
                `mm_cost` DECIMAL(8, 4) NOT NULL COMMENT '做市账户资费金额',
                `user_cost` DECIMAL(8, 4) NOT NULL COMMENT '普通用户资费金额',
                `sim_cost` DECIMAL(8, 4) NOT NULL COMMENT '模拟金用户资费金额',
                `capital_rate` VARCHAR(50) NOT NULL COMMENT '资金费率'
            )
            """)
    
    # 保存到mysql数据库
    async def save_mysql(self, save_sql_data, save_sql_filter_data):
        try:
            await self.connect_db()
            insert_sql = "INSERT INTO crypto_funding_rates (pair_name, exchange, fund_rate, fund_value, create_ts, timestamp) VALUES (%s, %s, %s, %s, %s, %s)"
            self.log.info(f"保存数据:{insert_sql}\n{save_sql_data}")
            # 批量插入
            self.cursor.executemany(insert_sql, save_sql_data)
            self.db.commit()  # 提交事务
        except:
            mess = f"{datetime.datetime.now()} 保存mysql资金费率数据失败,报错:{traceback.format_exc()}"
            self.log.info(mess)
            tb.warning(mess, 'risk')
            tb.sendmail(f'合约调整资金费率策略保存mysql失败', mess)
        self.db.close()  # 关闭数据库连接
    
    ''' ============================================================================'''
    ''' ==================================== wss ==================================='''
    ''' ============================================================================'''
    async def on_message(self, channel: str, item: dict):
        self.log.info(f"{channel} {item}")
        
    
    ''' ============================================================================'''
    ''' ================================== function ================================'''
    ''' ============================================================================'''
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
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        pass


def main():
    fund().run()

if __name__ == '__main__':
    main()












