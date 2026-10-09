'''
    月度交易数据汇总分析
    版本信息:v1.0.0
    日期:2026-02-25
    作者:sky

    每月1号获取上一个月的交易数据
    1、成交量排名前10的用户
    2、资费收取和支出排名前10的用户
    3、每日整体资费营收
    4、每月现货/合约交易盈亏数据
    5、当月对冲盈亏数据
'''
import os
import sys
import time
import datetime
import traceback
import asyncio
import numpy as np
import pandas as pd
import motor.motor_asyncio
import mysql.connector
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "../../.."))
sys.path.append(PROJECT_ROOT)
from utils.ToolBoxNew import ToolBox as tbn
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract
from crypto_center.client.rest.okex import contract as okx_rest

from zoneinfo import ZoneInfo
# 设置北京时间
tz = ZoneInfo("Asia/Shanghai")


buy_sell_side = {'1':'开多', '2':'开空', '3':'平多', '4':'平空'}
OPEN_ACTIONS = {"开多", "开空"}
CLOSE_ACTIONS = {"平多", "平空"}


# 当前时间
now = datetime.datetime.now(tz)

# 格式化函数
fmt = "%Y-%m-%d %H:%M:%S"

# 1️⃣ 当前时间
current_time = now.strftime(fmt)

# 2️⃣ 上一天 0 点
yesterday_0 = (now - datetime.timedelta(days=1)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
yesterday_0 = yesterday_0.strftime(fmt)

# 上一天24点
yesterday_24 = (now - datetime.timedelta(days=1)).replace(
    hour=23, minute=59, second=59, microsecond=0
)
yesterday_24 = yesterday_24.strftime(fmt)

# 3️⃣ 上个周一 0 点
# weekday(): 周一=0, 周日=6
this_week_monday = now - datetime.timedelta(days=now.weekday())
last_week_monday = (this_week_monday - datetime.timedelta(days=7)).replace(
    hour=0, minute=0, second=0, microsecond=0
)
last_week_monday = last_week_monday.strftime(fmt)

# 4️⃣ 上个月 1 号 0 点
if now.month == 1:
    last_month_first = now.replace(
        year=now.year - 1,
        month=12,
        day=1,
        hour=0, minute=0, second=0, microsecond=0
    )
else:
    last_month_first = now.replace(
        month=now.month - 1,
        day=1,
        hour=0, minute=0, second=0, microsecond=0
    )
last_month_first = last_month_first.strftime(fmt)

# 输出
print("当前时间:", current_time)
print("上一天0点:", yesterday_0)
print("上一天24点:", yesterday_24)
print("上个周一0点:", last_week_monday)
print("上个月1号0点:", last_month_first)
BEGIN_DATE = last_week_monday.split(' ')[0]
END_DATE = yesterday_24.split(' ')[0]


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()

    async def on_first(self):
        task = tbn()
        self.mm_id = list(task.load_account().keys())
        print(f"做市账户:{self.mm_id}")
        self.sim_user = await task.get_sim_user()
        print(f"模拟金用户:{self.sim_user}")
        # 配置账号
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.rest.DEBUG = False
        self.okx_rest.DEBUG = False
        self.rest.rest_timeout = 60
        self.okx_rest.rest_timeout = 60
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
    
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
    
    async def connect_db2(self):
        self.db2 = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="admin",
            password="A(?xvw8~v(ke0(O,=Se!W(!UGBujuh(XkBHuQTRu2",
            database="fund", # 'hedge'
            port=3306  # 默认端口
        )
        self.cursor2 = self.db2.cursor()

    async def test(self):
        try:
            await self.connect_db()
            # 查询表信息
            self.cursor.execute(f"SELECT * FROM user_fund_rank LIMIT 10")
            for row in self.cursor.fetchall():
                print(row)
        except:
            mess = f"{datetime.datetime.now()} 对冲账户权益数据保存mysql失败,报错:{traceback.format_exc()}"
            print(mess)
        finally:
            self.cursor.close()   # 关闭游标
            self.db.close()

    async def load_mysql_deal_data(self):
        user_vol = {}
        try:
            await self.connect_db()
            # 查询表信息
            sql = """
                    SELECT user_id,SUM(vol) AS total_vol 
                    FROM user_vol_rank 
                    WHERE date BETWEEN %s AND %s 
                    GROUP BY user_id 
                    ORDER BY total_vol DESC 
                    LIMIT 31
                  """
            self.cursor.execute(sql, (BEGIN_DATE, END_DATE))
            for row in self.cursor.fetchall():
                if row[0] not in self.mm_id and row[0] not in self.sim_user:
                    user_vol[row[0]] = row[1]
        except:
            mess = f"{datetime.datetime.now()} load_mysql_deal_data,报错:{traceback.format_exc()}"
            print(mess)
        finally:
            self.cursor.close()   # 关闭游标
            self.db.close()
        return user_vol
    
    async def load_mysql_fund_data(self):
        fund_data = {}
        try:
            await self.connect_db()
            # 查询每天资费
            sql = """
                    SELECT * FROM contract_dws.daily_fund_cost 
                    WHERE date BETWEEN %s AND %s 
                    ORDER BY date ASC
                  """
            self.cursor.execute(sql, (BEGIN_DATE, END_DATE))
            for row in self.cursor.fetchall():
                fund_data[row[0]] = row[1:]
        except:
            mess = f"{datetime.datetime.now()} load_mysql_fund_data,报错:{traceback.format_exc()}"
            print(mess)
        finally:
            self.cursor.close()   # 关闭游标
            self.db.close()
        return fund_data
    
    async def load_mysql(self):
        data = ""
        try:
            await self.connect_db()
            # self.cursor.execute(f"DESC user_fund_rank")
            # 查询用户资费
            sql = """
                    SELECT user_id, SUM(fund_cost) AS total_fund_cost 
                    FROM user_fund_rank 
                    WHERE date >= %s 
                    AND date <= %s 
                    GROUP BY user_id 
                    ORDER BY total_fund_cost DESC 
                    LIMIT 10
                  """
            self.cursor.execute(sql, (BEGIN_DATE, END_DATE))
            data = self.cursor.fetchall()
        except:
            print("没有fund_cost表")
        finally:
            self.cursor.close()   # 关闭游标
            self.db.close()       # 关闭数据库连接
        return data

    async def load_mysql2(self):
        data = ""
        try:
            await self.connect_db()
            # 查询用户资费
            sql = """
                    SELECT user_id, SUM(fund_cost) AS total_fund_cost 
                    FROM user_fund_rank 
                    WHERE date >= %s 
                    AND date <= %s 
                    GROUP BY user_id 
                    ORDER BY total_fund_cost ASC 
                    LIMIT 10
                """ 
            self.cursor.execute(sql, (BEGIN_DATE, END_DATE))
            data = self.cursor.fetchall()
        except:
            print("没有fund_cost表")
        finally:
            self.cursor.close()   # 关闭游标
            self.db.close()       # 关闭数据库连接
        return data

    async def load_mysql_mm_profit(self):
        data = ""
        try:
            await self.connect_db2()
            sql = """
                    SELECT * FROM spot_contract_pnl 
                    WHERE create_time <= %s 
                    ORDER BY id DESC LIMIT 2
                  """
            self.cursor2.execute(sql, (yesterday_24,))
            data = self.cursor2.fetchall()
        except:
            print("没有fund_cost表")
        finally:
            self.cursor2.close()   # 关闭游标
            self.db2.close()       # 关闭数据库连接
        return data
    
    async def do_count2(self, user_ids, uid_profit_list):
        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
        db = client.exchange
        collection = db.real_contract_deal

        profit_list = []
        for usr_id in user_ids:
            # 查找takerUser或makerUser至少一个字段包含468958值。
            query = {
                "$or": [
                    {"takerUser": {"$regex": usr_id}},
                    {"makerUser": {"$regex": usr_id}}
                ]
            }
            result = collection.find(query).sort("dealId", -1)  #.limit(1000)
            deal_num = 0    # 逐笔成交次数
            profit_sum = 0
            symbol_profit = {}
            async for deal in result:
                symbol = deal['symbol']
                date = datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
                temp_date = date.split(' ')[0]
                if temp_date < BEGIN_DATE or temp_date > END_DATE:
                    continue
                deal_num += 1
                if deal['takerUser'] == usr_id:
                    profit = deal['takerProfitLoss']
                if deal['makerUser'] == usr_id:
                    profit = deal['makerProfitLoss']
                profit_sum += float(profit)
                if symbol in symbol_profit:
                    symbol_profit[symbol] = round(symbol_profit[symbol]+float(profit), 2)
                else:
                    symbol_profit[symbol] = round(float(profit), 2)
            symbol_profit_mess = ",\n".join(
                f"{s}:{p}U" for s, p in symbol_profit.items()
            )
            profit_list.append(f"总盈亏:{round(profit_sum, 2)}U\n{symbol_profit_mess}")
            print(f"用户{usr_id}:总盈亏{round(profit_sum, 2)}U\n分币对盈亏:{symbol_profit_mess}")
        df = pd.DataFrame({
            'ID': user_ids,
            '成交金额': uid_profit_list,
            '交易币对（盈亏）': profit_list
        })
        # df.to_csv('成交量top10用户信息.csv', index=False, encoding='utf-8-sig')
        return df
    
    async def do_count3(self):
        fund_collect = await self.load_mysql()
        fund_pay = await self.load_mysql2()
        collect_user = [uid for uid, total in fund_collect]
        collect_fund = [float(total) for uid, total in fund_collect]
        pay_user = [uid for uid, total in fund_pay]
        pay_fund = [float(total) for uid, total in fund_pay]
        fund_collect_df = pd.DataFrame({
            'ID': collect_user,
            '资费金额': collect_fund
        })
        fund_pay_df = pd.DataFrame({
            'ID': pay_user,
            '资费金额': pay_fund
        })
        return fund_collect_df, fund_pay_df
    
    async def do_count4(self, fund_data):
        date_list = [d[0] for d in fund_data.values()]
        user_fund_list = [float(d[1]) for d in fund_data.values()]
        sim_fund_list = [float(d[2]) for d in fund_data.values()]
        df = pd.DataFrame({
                '日期': date_list,
                '普通用户资费': user_fund_list,
                '模拟金用户资费': sim_fund_list
            })
        return df
        
    async def do_count5(self, data):
        create_time = []
        detail = []
        for i in data:
            create_time.append(i[-1])
            detail.append(i[4])
        df = pd.DataFrame({
            '时间': create_time,
            '盈亏': detail
        })
        return df
    
    async def do_count6(self):
        hedge_profit_sum = 0
        # 修改日期为13位时间戳
        begin_ts = int(time.mktime(time.strptime(yesterday_24, "%Y-%m-%d %H:%M:%S"))) * 1000
        end_ts = int(time.mktime(time.strptime(last_week_monday, "%Y-%m-%d %H:%M:%S"))) * 1000
        start_ts = min(begin_ts, end_ts)
        stop_ts = max(begin_ts, end_ts)
        ten_days_ms = 10 * 24 * 60 * 60 * 1000
        all_res = []
        current_ts = start_ts
        while current_ts < stop_ts:
            next_ts = min(current_ts + ten_days_ms, stop_ts)
            # 保持接口参数方向和原有代码一致（after为较新时间，before为较旧时间）
            batch = await self.okx_rest.fetch_position_history(limit=100, after=next_ts, before=current_ts)
            for i in batch[::-1]:
                all_res.append(i)
            current_ts = next_ts
            await asyncio.sleep(0.5)
        for i in all_res[::-1]:
            hedge_profit_sum += round(float(i['realizedPnl']), 2)
            hedge_profit_sum += round(float(i['fee']), 2)    
        print(f'对冲总盈亏:{hedge_profit_sum}')

        # 内盘用户盈亏
        import importlib.util
        path = "/home/ubuntu/strategy_base/strategy/follow_hedge2/follow_hedge_config.py"
        spec = importlib.util.spec_from_file_location("follow_hedge_config", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.usr_id = str(list(module.leads.keys())[0])

        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
        db = client.exchange
        collection = db.real_contract_deal
        query = {
            "$or": [
                {"takerUser": {"$regex": self.usr_id}},
                {"makerUser": {"$regex": self.usr_id}}
            ]
        }
        result = collection.find(query).sort("dealId", -1)  #.limit(1000)
        deal_num = 0    # 逐笔成交次数
        profit_sum = 0
        async for deal in result:
            date = datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
            temp_date = date.split(' ')[0]
            if temp_date < BEGIN_DATE or temp_date > END_DATE:
                continue
            deal_num += 1
            if deal['takerUser'] == self.usr_id:
                profit = deal['takerProfitLoss']
            if deal['makerUser'] == self.usr_id:
                profit = deal['makerProfitLoss']
            profit_sum += float(profit)
        print(f"{self.usr_id}用户共{deal_num}条数据, 内盘总盈亏:{int(profit_sum)}")

        df = pd.DataFrame({
            '内盘总盈亏': [int(profit_sum)],
            '对冲总盈亏': [int(hedge_profit_sum)]
        })
        return df

    async def main(self):
        # await self.test()
        # exit()

        # 准备数据
        user_vol = await self.load_mysql_deal_data()
        uid_list = list(user_vol.keys())[:10]
        uid_profit_list = list(user_vol.values())[:10]
        fund_data = await self.load_mysql_fund_data()
        mm_profit = await self.load_mysql_mm_profit()

        # 并行计算
        results = await asyncio.gather(
            self.do_count2(uid_list, uid_profit_list),  # 成交量
            self.do_count3(),                           # 用户资费
            self.do_count4(fund_data),                  # 每日资费
            self.do_count5(mm_profit)                  # 现货&合约盈亏
            #self.do_count6()                            # 计算合约对冲盈亏
        )

        # 从返回的列表中解包结果
        vol_df = results[0]
        fund_collect_df, fund_pay_df = results[1]
        fund_df = results[2]
        mm_profit_df = results[3]
        # hedge_profit_df = results[4]

        # 行数对齐
        # max_rows = max(len(vol_df), len(fund_collect_df), len(fund_pay_df), len(fund_df), len(mm_profit_df), len(hedge_profit_df))
        max_rows = max(len(vol_df), len(fund_collect_df), len(fund_pay_df), len(fund_df), len(mm_profit_df))
        vol_df = vol_df.reindex(range(max_rows))
        fund_collect_df = fund_collect_df.reindex(range(max_rows))
        fund_pay_df = fund_pay_df.reindex(range(max_rows))
        fund_df = fund_df.reindex(range(max_rows))
        mm_profit_df = mm_profit_df.reindex(range(max_rows))
        # hedge_profit_df = hedge_profit_df.reindex(range(max_rows))

        # 插入一个空列（作为分隔列）
        separator = pd.DataFrame({"": [np.nan] * max_rows})
        # 横向拼接
        # df_final = pd.concat([mm_profit_df, separator, fund_df, separator, hedge_profit_df, separator, 
        #                       vol_df, separator, fund_collect_df, separator, fund_pay_df], axis=1)
        df_final = pd.concat([mm_profit_df, separator, fund_df, separator, separator, 
                              vol_df, separator, fund_collect_df, separator, fund_pay_df], axis=1)
        
        # 保存数据
        with open(f'{BEGIN_DATE}-{END_DATE}周数据分析.csv', "w", encoding="utf-8-sig") as f:
            # 写入标题行（后面补两个逗号，占3列）
            f.write('上周做市盈亏,,,' + '上周资费收入,,,,' + '对冲盈亏,,,' + '成交量排名,,,,' + '资费收取用户排名,,,' + '资费支出用户排名,,,\n')
            df_final.to_csv(f, index=False)
        # 发送tg
        import requests
        BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
        CHAT_ID = "-4893618309"  #"-4893618309"
        FILE_PATH = f'{BEGIN_DATE}-{END_DATE}周数据分析.csv'
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
        with open(FILE_PATH, "rb") as f:
            files = {"document": f}
            data = {"chat_id": CHAT_ID}
            r = requests.post(url, data=data, files=files)
        print(r.json())
        self.loop.stop()


def main():
    Strategy().run()


if __name__ == '__main__':
    main()



