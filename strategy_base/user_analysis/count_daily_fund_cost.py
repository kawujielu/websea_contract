'''
    单独计算每日资金费用收取情况
    版本信息:v1.0.0
    日期:2025-11-05
    作者:sky
'''
import os
import sys
import datetime
import traceback
import asyncio
import importlib
import calendar
import mysql.connector

sys.path.append("../..")
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
    
    
    def _load_config(self, config):
        """读取配置文件
        """
        self.init_config = config
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        # config = config.config
        # 配置账号
        # self.rest = Contract(config['base_token'], config['base_secret'])
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        
    def _initParams(self):
        """初始化参数
        """
        self.symbol_cost = {}       # 保存分币对资费
        pass
    
    async def on_first(self):
        self.log.info("=======first======")
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        # await self.del_mysql()        # 删除表
        # await self.load_mysql()       # 查询表
        # await self.create_table()     # 创建表
        # await self.update_tag_user()    # 查询模拟金用户
        self.log.info(f"初始化完成")
        # await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")                                                     
        self.schedule.add_job(self.update_tag_user, CronTrigger(hour="0", minute="0"))   # 更新模拟金用户
        self.schedule.add_job(self.main, CronTrigger(hour=f"0", minute="1"))             # 主策略

    
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
        try:
            await self.connect_db()
            self.cursor.execute("DROP TABLE daily_fund_cost")
            self.cursor.execute("DROP TABLE daily_symbol_fund_cost")
        except:
            self.log.warning(f"del_mysql报错:{traceback.format_exc()}")
        self.db.commit()
        self.db.close()
    
    async def load_mysql(self):
        try:
            await self.connect_db()
            self.cursor.execute(f"SELECT * FROM daily_fund_cost")
            data = self.cursor.fetchall()
            # print(data)
            for i in data:
                print(i)
            print('==============================================')
            self.cursor.execute(f"SELECT * FROM daily_symbol_fund_cost")
            data = self.cursor.fetchall()
            # print(data)
            for i in data:
                print(i)
        except:
            self.log.warning("没有daily_fund_cost表")
        self.db.close()

    async def create_table(self):
        try:
            await self.connect_db()
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `daily_fund_cost` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `date` VARCHAR(50) NOT NULL COMMENT '时间',
                    `user_cost` DECIMAL(20, 8) NOT NULL COMMENT '普通用户资费金额',
                    `sim_cost` DECIMAL(20, 8) NOT NULL COMMENT '模拟金用户资费金额'
                )
                """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS `daily_symbol_fund_cost` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `date` VARCHAR(50) NOT NULL COMMENT '时间',
                    `symbol` VARCHAR(50) NOT NULL COMMENT '交易对',
                    `user_cost` DECIMAL(20, 8) NOT NULL COMMENT '普通用户资费金额',
                    `sim_cost` DECIMAL(20, 8) NOT NULL COMMENT '模拟金用户资费金额'
                )
                """)
        except:
            self.log.warning(f"create_table报错:{traceback.format_exc()}")
        self.db.commit()
        self.db.close()
        
        
    ''' ==========================================================================='''
    ''' =================================== funcation ============================='''
    ''' ==========================================================================='''
    # 重新加载配置文件
    async def reload_config(self):
        try:
            importlib.reload(self.init_config)
            [setattr(self, k, v) for k, v in vars(self.init_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"reload_config报错{traceback.format_exc()}")
            except:
                pass
            
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
    
    # 更新模拟金用户
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
        self.log.info(f"E组用户:{tag_users}")
        self.filter_users = []      # 标记组用户列表
        for tag, users in tag_users.items():
            self.filter_users += users
        self.log.info(f"E组用户:{self.filter_users}")

    # 查询资费
    async def get_cost(self, user_type, begin_ts, end_ts):
        last_page = 0
        cost_data = []
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_treaty_cost(page_size=self.page_size, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    self.log.info(f"{data['data'][0]['create_time_text']}数据量:{data['pager']}")
                    # 'pager': {'item_count': '36', 'page': '1', 'page_size': '1000', 'total_page': 1}}
                    total_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, total_page+1):
                    last_page = n
                    data = await self.rest.fetch_treaty_cost(page=n, page_size=self.page_size, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                    await asyncio.sleep(0.2)
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
        return cost_data
    
    # 计算当日资费总和&分币对资费
    async def count_cost(self, mm_cost):
        # 计算每日资费总和
        user_cost_sum = 0
        sim_cost_sum = 0
        user_symbol_cost = {}
        sim_symbol_cost = {}
        for i in mm_cost:
            uid = i['user_id']
            symbol = i['symbol_name']
            cost = float(i['settle_cost'])
            if uid in self.filter_users:
                sim_cost_sum += cost
                if symbol in sim_symbol_cost:
                    sim_symbol_cost[symbol] += cost
                else:
                    sim_symbol_cost[symbol] = cost
            else:
                user_cost_sum += cost
                if symbol in user_symbol_cost:
                    user_symbol_cost[symbol] += cost
                else:
                    user_symbol_cost[symbol] = cost
        return user_cost_sum, sim_cost_sum, user_symbol_cost, sim_symbol_cost
    
    # 将数据转换为存mysql的格式
    async def count_sql_data(self, date, user_cost_sum, sim_cost_sum, user_symbol_cost, sim_symbol_cost):
        save_daily_cost, save_daily_symbol_cost = [], []
        save_daily_cost.append((date, round(user_cost_sum, 4), round(sim_cost_sum, 4)))
        for symbol, user_pos in user_symbol_cost.items():
            sim_pos = sim_symbol_cost.get(symbol, 0)
            save_daily_symbol_cost.append((date, symbol, round(user_pos, 4), round(sim_pos, 4)))
        return save_daily_cost, save_daily_symbol_cost
    
    # 保存mysql
    async def save_mysql(self, save_sql_data, save_sql_data2):
        try:
            await self.connect_db()
            insert_sql = "INSERT INTO daily_fund_cost (date, user_cost, sim_cost) VALUES (%s, %s, %s)"
            self.log.info(f"保存数据:{insert_sql}\n{save_sql_data}")
            self.cursor.executemany(insert_sql, save_sql_data)
            self.db.commit()  # 提交事务
        except:
            mess = f"保存资费金额策略 保存mysql数据失败,报错:{traceback.format_exc()}"
            self.log.error(mess)
            tb.warning(mess, 'risk')
        try:
            insert_sql = "INSERT INTO daily_symbol_fund_cost (date, symbol, user_cost, sim_cost) VALUES (%s, %s, %s, %s)"
            self.log.info(f"保存数据:{insert_sql}\n{save_sql_data2}")
            self.cursor.executemany(insert_sql, save_sql_data2)
            self.db.commit()  # 提交事务
        except:
            mess = f"保存资费金额策略 保存mysql数据失败,报错:{traceback.format_exc()}"
            self.log.error(mess)
            tb.warning(mess, 'risk')
        self.cursor.close()
        self.db.close()
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        # user_type = 1   # 0(全部用户) | 1(仅量化用户) | 2(仅普通用户)
        # 计算统计区间时间戳,统计最近8小时资费收取情况
        # 规则:开始时间是0点,会包含0点结算数据;结束时间是8点,不会包含8点数据
        # now_utc_8 = datetime.datetime.now() + datetime.timedelta(hours=8)
        # eight_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=self.hour_interval)
        # begin_time = eight_hours_ago.replace(minute=0, second=10, microsecond=0)
        # begin_ts = int(begin_time.timestamp())
        # end_ts = int(datetime.datetime.now().timestamp())

        # 计算本月1号0点开始的时间戳到2号0点的时间戳
        date_ts = []
        month = datetime.datetime.now().month
        year = datetime.datetime.now().year
        today = datetime.datetime.now().day
        begin_year = year
        begin_month = month
        if today == 1:
            if month == 1:
                begin_month = 12
                begin_year = int(year-1)
            else:
                begin_month = int(month-1)
            begin_day = calendar.monthrange(begin_year, begin_month)[1]   # 本月最后一天的日期
        else:
            begin_day = int(today-1)
        begin_time = datetime.datetime(begin_year, begin_month, begin_day, 0, 0, 0)
        begin_ts = int(begin_time.timestamp())
        end_time = datetime.datetime(year, month, today, 0, 0, 0)
        end_ts = int(end_time.timestamp())
        date_ts.append([begin_ts, end_ts])
        self.log.info(f"开始时间:{begin_time}——结束时间:{end_time} {begin_ts}——{end_ts}")
        
        # 查询所有普通用户资费
        save_daily_cost = []
        save_daily_symbol_cost = []
        mess = ""
        mess2 = ""
        for ts in date_ts:
            mm_cost = await self.get_cost(user_type=2, begin_ts=ts[0], end_ts=ts[1])
            user_cost_sum, sim_cost_sum, user_symbol_cost, sim_symbol_cost = await self.count_cost(mm_cost)
            # 10位时间戳转换成日期格式
            date = datetime.datetime.fromtimestamp(ts[0]).strftime('%Y-%m-%d')
            mess += f"{date}每日普通用户资费总金额:{user_cost_sum} 模拟金:{sim_cost_sum}\n"
            mess2 += f"{date}每日普通用户分币对资费金额:{user_symbol_cost} 模拟金:{sim_symbol_cost}"
            temp_save_daily_cost, temp_save_symbol_daily_cost = await self.count_sql_data(date, user_cost_sum, sim_cost_sum, user_symbol_cost, sim_symbol_cost)
            save_daily_cost += temp_save_daily_cost
            save_daily_symbol_cost += temp_save_symbol_daily_cost
        self.log.info(mess)
        self.log.info(mess2)
        
        # 存mysql数据库
        await self.save_mysql(save_daily_cost, save_daily_symbol_cost)
        
        

def main(path='fund_cost_monitor_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()


if __name__ == '__main__':
    main()




