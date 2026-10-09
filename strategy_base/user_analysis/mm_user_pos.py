'''
    统计所有做市账户每个交易对的净持仓
    版本信息:v1.0.0
    日期:2025-10-09
    作者:sky

    通过holding2查询做市账户+所有普通用户的持仓
    保存到mysql
    用grafana展示
'''

import os
import sys
import datetime
import traceback
import asyncio
import pytz
import mysql.connector
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
# from utils.aio_redis import MyAioredis, MyAioredisFunctools
# from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from client.env_pro.rest.websea.contract import WebseaContract as old_contract
# from crypto_center.client.rest.websea import contract_quan
# from crypto_center.client.rest.binance import u_contract as bn_rest
# from client.env_dev.rest.websea_contract import WebseaContract as ws_contract_rest    # 测试环境
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class fund(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()          # 读取配置
        self.symbols_markprice = {}  # 交易对最新标记价格
        self.symbol_unit = {}        # 交易对单位
        self.rc_task = rc.RestClient()

    def _load_config(self):
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        self.old_rest = old_contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')       # 近盘口
        self.rest1 = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp',dev=False)    # 近盘口 19
        self.rest2 = Contract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1',dev=False)    # 远盘口 20
        self.rest3 = Contract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k',dev=False)    # 补挡位 
        self.rest4 = Contract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr',dev=False)    # 刷量策略 18
        self.rest5 = Contract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz',dev=False)    # 新策略 22
        self.rest6 = Contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4',dev=False) # 压盘口策略 
        self.rest7 = Contract('5accd1fb1d03e26f299a4297d9q52646135','nyuc69aljff4g297uj99',dev=False) # MH合约 
        self.rest8 = Contract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3',dev=False)    # 防守策略 23
        self.rest9 = Contract('fd0bc370e0f8dc47a923f54fca31d3d2','zbihesbcp6yz5j0awlqv',dev=False)    # 新铺单策略-2 
        self.rest10 = Contract('d6678ea254de541f67594668b8ee7937','tz1rtf00gudmgch3jchl',dev=False)   # 新铺单策略-3
        self.rest11 = Contract('57b49384ee4e0b236f0f38a45fj50733190','bu8qqmoyaq0o10xeepdd',dev=False)
        # self.bn_rest = bn_rest.UBinanceContract('AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi','FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj')
    
    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        await self.get_symbol_unit()        # 内盘合约单位
        await self.del_mysql()      # 删除表
        # await self.load_mysql()     # 查看表
        await self.create_table()   # 创建表
        # 查询初始数据
        self.log.info(f"策略初始化完成")
        await self.main()
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="*/15"))
    
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
        self.cursor.execute("DROP TABLE mm_user_pos")
        self.db.commit()
        self.db.close()
    
    async def load_mysql(self):
        await self.connect_db()
        self.cursor.execute(f"SELECT * FROM mm_user_pos")
        data = self.cursor.fetchall()
        for i in data:
            print(i)

    async def create_table(self):
        await self.connect_db()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `mm_user_pos` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `date` DATETIME(3) NOT NULL COMMENT '时间',
                `mm_user` VARCHAR(36) NOT NULL COMMENT '做市账号',
                `symbol` VARCHAR(36) NOT NULL COMMENT '交易对',
                `mm_amt` DECIMAL(20,4) NOT NULL COMMENT '做市商账户净持仓',
                `mm_pos` DECIMAL(20,4) NOT NULL COMMENT '做市商账户净持仓',
                `long_pos` DECIMAL(20,4) NOT NULL COMMENT '做市商账户多头持仓',
                `short_pos` DECIMAL(20,4) NOT NULL COMMENT '做市商账户空头持仓'
            )
            """)
    
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
        
    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                res = await self.rest1.fetch_symbol_info()
                for s, v in res.items():
                    self.symbol_unit[s] = v['contractSize']
                self.log.info(f"初始化合约单位:{self.symbol_unit}")
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)
            
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self, tag=None):
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 统计所有做市账户持仓
        mm_symbol_pos = {}    # 保存所有做市账户持仓
        for uid in [18,19,20,21,22,23,24,25,476515,478614,532500,532502]:
            temp_hold_list = []
            last_page = 0
            while 1:
                try:
                    if last_page == 0: 
                        data = await self.rest.fetch_hold_list(user_id=uid, page_size=100)
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
                    for i in temp_hold_list:
                        symbol = i['symbol']
                        pos = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
                        pos *= self.symbol_unit.get(symbol, 1)
                        mark_price = float(i['mark_price'])
                        if uid not in mm_symbol_pos:
                            mm_symbol_pos[uid] = {}
                        if symbol not in mm_symbol_pos[uid]:
                            mm_symbol_pos[uid][symbol] = [0, 0, 0, 0]
                        mm_symbol_pos[uid][symbol][0] += pos
                        mm_symbol_pos[uid][symbol][1] += mark_price * pos
                        if pos > 0:
                            mm_symbol_pos[uid][symbol][2] = pos
                        elif pos < 0:
                            mm_symbol_pos[uid][symbol][3] = pos
                    break
                except:
                    self.log.info(traceback.format_exc())
                    await asyncio.sleep(5)
                        
        mess = ""
        diff_mess = ""
        save_sql_data = []
        for uid, data in mm_symbol_pos.items():
            for s, pos in data.items():
                mm_pos = round(pos[0], 4)
                mm_amt = round(pos[1], 1)
                long_pos = round(pos[2], 4)
                short_pos = round(pos[3], 4)
                save_sql_data.append((date, uid, s, mm_amt, mm_pos, long_pos, short_pos))
                mess += f"{uid}账户 {s} mm_pos:{mm_pos}, long_pos:{long_pos}, short_pos:{short_pos}\n"
            self.log.info(mess)
        
        # 把分析好的结果存入mysql数据库中,用于grafana分析用
        await self.connect_db()     # 连接数据库
        try:
            # 先清空table
            sql = "TRUNCATE TABLE mm_user_pos"
            self.cursor.execute(sql)
            self.db.commit()  # 提交事务

            # 批量插入
            insert_sql = f"INSERT INTO mm_user_pos (date, mm_user, symbol, mm_amt, mm_pos, long_pos, short_pos) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            self.cursor.executemany(insert_sql, save_sql_data)
            self.db.commit()  # 提交事务
            self.log.info(f"保存数据:{insert_sql}\n{save_sql_data}")
        except:
            mess = f"{datetime.datetime.now()} 用户分析数据保存mysql失败,报错:{traceback.format_exc()}"
            self.log.warning(mess)
            tb.warning(mess, 'risk')
            tb.sendmail(f'用户分析数据保存mysql失败', mess)
        self.cursor.close()   # 关闭游标
        self.db.close()       # 关闭数据库连接

def main():
    fund().run()

if __name__ == '__main__':
    main()





