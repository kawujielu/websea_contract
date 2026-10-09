'''
    定时分析以实现盈亏达标用户交易数据
    版本信息:v1.0.0
    日期:2025-10-09
    作者:sky

    用户交易行为分析:
    1、获得根据规则筛选出来的用户id
    2、按照用户id分析近期成交表现
'''

import os
import sys
import time
import datetime
import numpy as np
import pandas as pd
import traceback
import mysql.connector
import motor.motor_asyncio

sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


MM_ID = ['18','19','20','21','22','23','24','25','476515','478614','532500','532502']
BUY_SELL_SIDE = {'1': '开多', '2': '开空', '3': '平多', '4': '平空'}
ORDER_SOURCE = {1: '普通', 2: '爆仓', 3: '止盈', 4: '止损', 5: '反手', 6: '限价止盈止损触发', }
PROFITLOSS = 1000
VOL = 100
MINUTES = 10

class user_analysis(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.rc_task = rc.RestClient()
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef', '', dev=False)
        self.today_user = []
        self.today = datetime.datetime.now().strftime("%Y-%m-%d")
        self.yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    async def on_first(self):
        # 日志
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        # await self.del_mysql()    # 删除表
        # await self.load_mysql()     # 查看表
        await self.create_table()     # 创建表
        self.log.info(f"策略初始化完成")
        #await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute=f"*/{MINUTES}"))   # 判断是否需要调整资费
        
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
        self.cursor.execute("DROP TABLE user_profit_analysis")
        self.db.commit()
        self.db.close()
    
    async def load_mysql(self):
        await self.connect_db()
        self.cursor.execute(f"SELECT * FROM user_profit_analysis")
        data = self.cursor.fetchall()
        print(data)
        for i in data:
            print(i)
        # 删除表
        # self.cursor.execute("DROP TABLE okx_hedge_balance3")
        # self.db.commit()
        # self.db.close()

    async def create_table(self):
        await self.connect_db()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `user_profit_analysis` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `user_id` VARCHAR(36) NOT NULL COMMENT '用户id',
                `asalysis_info` VARCHAR(36) NOT NULL COMMENT '分析内容',
                `trade_num` VARCHAR(36) NOT NULL COMMENT '交易笔数',
                `open_close_num` VARCHAR(36) NOT NULL COMMENT '开平仓次数',
                `profit` VARCHAR(36) NOT NULL COMMENT '总盈亏',
                `ave_profit` VARCHAR(36) NOT NULL COMMENT '平均每笔盈亏',
                `max_profit` VARCHAR(36) NOT NULL COMMENT '最大盈利',
                `max_loss` VARCHAR(36) NOT NULL COMMENT '最大亏损',
                `profit_ratio` VARCHAR(36) NOT NULL COMMENT '胜率',
                `profit_loss_ratio` VARCHAR(36) NOT NULL COMMENT '盈亏比',
                `max_holding_time` VARCHAR(36) NOT NULL COMMENT '最大持仓时间',
                `min_holding_time` VARCHAR(36) NOT NULL COMMENT '最短持仓时间',
                `max_win_num` VARCHAR(36) NOT NULL COMMENT '最大连续盈利次数',
                `max_loss_num` VARCHAR(36) NOT NULL COMMENT '最大连续亏损次数',
                `max_deal_amt` VARCHAR(36) NOT NULL COMMENT '最大单笔交易金额',
                `ave_holding_time` VARCHAR(36) NOT NULL COMMENT '平均持仓时间',
                `ave_deal_num` VARCHAR(128) NOT NULL COMMENT '平均交易次数',
                `leverage` VARCHAR(36) NOT NULL COMMENT '杠杆',
                `expected_return` VARCHAR(36) NOT NULL COMMENT '期望收益'
            )
            """)
    
    # 判断今日是否已经分析过这批用户,用UTC时间
    async def filter_user(self, usr_ids):
        users = []
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        if today != self.today:
            self.today = today
            self.today_user = []
        for id in usr_ids:
            if id not in self.today_user:
                users.append(id)
                self.today_user.append(id)
        return users
        
    async def main(self):
        # 每30分钟更新一次盈利用户数据
        usr_ids = await self.get_user_ids()
        
        # 当天分析过的不再重复分析
        users = await self.filter_user(usr_ids)
        if users == []:
            self.log.info("无新增用户")
            return
        else:
            self.log.info(f"新增分析用户:{users}")
        
        # 查询此时间后的数据
        begin_date = await self.get_begin_date()
        
        await self.do_count(users, begin_date)
    
    # 每30分钟更新一次盈利用户数据
    async def get_user_ids(self):
        # AsyncIOMotorClient 代表一个mongod进程，或者它们的一个集群。显式创建这些客户端对象之一，将其连接到正在运行的 mongod，并在应用程序的整个生命周期中使用它。
        client = motor.motor_asyncio.AsyncIOMotorClient(
            'mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
        # AsyncIOMotorDatabase：每个 mongod 都有一组数据库（磁盘上不同的数据文件集）。可以从客户端获取对数据库的引用。
        db = client.exchange

        info = await db.list_collection_names()
        # print(f"集合列表:{info}")
        
        # AsyncIOMotorCollection：一个数据库有一组集合，其中包含文档；从数据库中获得对集合的引用。
        collection = db.real_contract_deal
        # 昨日时间
        # yesterday_ts = int(datetime.datetime.combine(datetime.datetime.today() - datetime.timedelta(days=1), datetime.datetime.min.time()).timestamp())
        # one_hour_ago = int((datetime.datetime.now() - datetime.timedelta(hours=1)).timestamp())
        cutoff_ts = int((datetime.datetime.now() - datetime.timedelta(minutes=MINUTES)).timestamp())
        
        # for usr_id in usr_ids:
        query = {
                'ts': {'$gt': cutoff_ts}  # 大于等于begin_date前的时间
                # "$or": [
                #     {"takerUser": {"$regex": usr_ids[0]}},
                #     {"makerUser": {"$regex": usr_ids[0]}}
                # ]
            }
        res = collection.find(query).sort("dealId", -1).allow_disk_use(True)
        
        orders = []
        async for re in res:
            T = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(re['ts']))
            symbol = re['symbol']
            object_id = str(re['_id'])
            dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(re['ts']))
            price = float(re['price'])
            amount = float(re['amount'])  # 张数
            takerUser = re['takerUser']
            makerUser = re['makerUser']
            takerOrder = re['takerOrder']
            makerOrder = re['makerOrder']
            takerFee = float(re['takerFee'])
            makerFee = float(re['makerFee'])
            takerIsFull = True if re['takerIsFull'] == 1 else False
            makerIsFull = True if re['makerIsFull'] == 1 else False
            takerBuyOrSell = BUY_SELL_SIDE.get(re['takerBuyOrSell'], re['takerBuyOrSell'])
            makerBuyOrSell = BUY_SELL_SIDE.get(re['makerBuyOrSell'], re['makerBuyOrSell'])
            takerOpenAvgPrice = float(re['takerOpenAvgPrice']) if re['takerOpenAvgPrice'] else 0.0
            makerOpenAvgPrice = float(re['makerOpenAvgPrice']) if re['makerOpenAvgPrice'] else 0.0
            takerProfitLoss = float(re['takerProfitLoss']) if re['takerProfitLoss'] else 0.0
            makerProfitLoss = float(re['makerProfitLoss']) if re['makerProfitLoss'] else 0.0
            takerFaceValue = float(re['takerFaceValue'])  # 面值
            makerFaceValue = float(re['makerFaceValue'])
            takerMultiple = float(re['takerMultiple'])
            makerMultiple = float(re['makerMultiple'])
            takerOrderSource = ORDER_SOURCE.get(re['takerOrderSource'], re['takerOrderSource'])
            makerOrderSource = ORDER_SOURCE.get(re['makerOrderSource'], re['makerOrderSource'])
            
            taker = [object_id, dt, symbol, takerOrder, takerUser, takerIsFull, takerMultiple, takerBuyOrSell, amount, price, takerFee, takerOpenAvgPrice, takerProfitLoss, takerFaceValue, takerOrderSource]
            maker = [object_id, dt, symbol, makerOrder, makerUser, makerIsFull, makerMultiple, makerBuyOrSell, amount, price, makerFee, makerOpenAvgPrice, makerProfitLoss, makerFaceValue, makerOrderSource]
            orders.append(taker)
            orders.append(maker)
        # ['68e926a56990ca86be88e091', '2025-10-10 15:30:43', 'ETH-USDT', 'SM4851421759992408201PY4CQ9', '485142', False, 100.0, '平多', 1639.0, 4127.62, 37.20843049, 4326.34345629966, -3257.0774487514186, 0.01, '止损']
        df1 = pd.DataFrame(orders, columns=[
                                'object_id', 'tsText', 'symbol', 'takerOrder', 'user_id', 'takerIsFull', 'takerMultiple', 'takerBuyOrSell', 'vol', 'price', 'fee', 'takerOpenAvgPrice', 'profitLoss', 'takerFaceValue', 'takerOrderSource'])
        
        user_list = []
        for j in range(len(df1)):
            s = df1['symbol'].iloc[j]
            fee_s = df1['fee'].iloc[j]
            vol_s = df1['vol'].iloc[j]
            profitLoss_s = df1['profitLoss'].iloc[j]
            df2 = df1[df1['symbol'] == s]
            df2 = df2.groupby(['user_id'])[['fee', 'profitLoss', 'vol']].sum().reset_index()
            df2 = df2.sort_values(by=["profitLoss"], ascending=True)
            user_s = df2['user_id'].unique()
            df2 = df2[(abs(df2['profitLoss']) >= PROFITLOSS) | (df2['vol'] >= VOL)]
            msg_d = ""
            if not df2.empty:
                msg_d += f"【{s}】盈亏:{int(profitLoss_s)}U 手续费:{round(fee_s, 2)}U 量:{int(vol_s)}U 人数:{len(user_s)}\n"
                for i in range(len(df2)):
                    user_id = df2['user_id'].iloc[i]
                    if user_id in MM_ID:
                        continue
                    fee = df2['fee'].iloc[i]
                    profitLoss = df2['profitLoss'].iloc[i]
                    vol = df2['vol'].iloc[i]
                    if abs(profitLoss) >= PROFITLOSS:
                        user_list.append(user_id)
                        msg_d += f"         -  id:{user_id}:盈亏:{int(profitLoss)}U 手续费:{round(fee, 2)}U 量:{int(vol)}U \n"
        self.log.info(f"{msg_d}")
        self.log.info(f"分析用户:{user_list}")
        return user_list
    
    # 计算查询时间
    async def get_begin_date(self):
        yesterday = (datetime.datetime.now() -
                     datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        last_week = (datetime.datetime.now() -
                     datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        last_month = (datetime.datetime.now() -
                      datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        return [yesterday, last_week, last_month]

    async def do_count(self, usr_ids, begin_date):
        for usr_id in usr_ids:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                'mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
            db = client.exchange
            collection = db.real_contract_deal
            # 查找takerUser或makerUser至少一个字段包含user_id值
            query = {
                "$or": [
                    {"takerUser": {"$regex": usr_id}},
                    {"makerUser": {"$regex": usr_id}}
                ],
                # 'ts': {'$gte': begin_ts}  # 大于等于begin_date前的时间
                # "symbol": "CFX-USDT",
            }
            # find()查找所有内容, sort()按照dealId字段-1降序排序, limit()限制返回条数
            result = collection.find(query).sort("dealId", -1)  # .limit(1000)
            # result = await collection.find_one()  # 查询最新一条数据

            temp_data = []
            async for deal in result:
                temp_data.append(deal)
            for n, filter_date in enumerate(begin_date):
                date_info = f"近1天" if n == 0 else f"近1周" if n == 1 else f"近1月"
                num = 0
                filter_num = 0
                profit_sum = 0
                user_list = []
                for deal in temp_data:
                    num += 1
                    date = datetime.datetime.utcfromtimestamp(
                        deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
                    temp_date = date.split(' ')[0]
                    if temp_date < filter_date:
                        filter_num += 1
                        continue
                    taker = f"taker:{deal['takerUser']}"
                    maker = f"maker:{deal['makerUser']}"
                    price = float(deal['price'])
                    amount = float(deal['amount'])
                    face_value = deal['takerFaceValue']
                    if deal['takerUser'] == usr_id:
                        side_type = deal['takerBuyOrSell']
                        fee = deal['takerFee']
                        profit = deal['takerProfitLoss']
                        multiple = deal['takerMultiple']
                    if deal['makerUser'] == usr_id:
                        side_type = deal['makerBuyOrSell']
                        fee = deal['makerFee']
                        profit = deal['makerProfitLoss']
                        multiple = deal['makerMultiple']
                    side = BUY_SELL_SIDE[str(side_type)]
                    taker_side = deal['takerBuyOrSell']
                    maker_side = deal['makerBuyOrSell']
                    profit_sum += float(profit)
                    # print(f"{datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')} {deal['symbol']} {side} 成交价:{price} 数量:{amount} 盈亏:{round(float(profit), 2)} 手续费:{round(float(fee), 2)} {taker} {maker}")
                    ll = [usr_id, date, side, float(profit), float(
                        price), float(amount), float(face_value), int(multiple)]
                    user_list.append(ll)
                print(
                    f"{usr_id}用户,统计时间:{filter_date}至今,共{num}条数据,过滤{filter_num}条,总盈亏:{int(profit_sum)}")

                # 生成dataframe
                df = pd.DataFrame(user_list, columns=[
                                  'userid', 'tsText', 'buy_sell', 'profit_loss', 'price', 'amount', 'face_value', 'multiple'])
                await self.fenxi(df, date_info)

    async def fenxi(self, df, date_info):
        if df.shape[0] == 0:
            print(f"{date_info}无成交记录")
            return
        # 时间列处理 & 排序
        df['tsText'] = pd.to_datetime(df['tsText'])
        df = df.sort_values('tsText').reset_index(drop=True)
        # 选择tsText字段大于2025-07-12号的
        # df = df[df['tsText'] > '2025-07-12']

        # 初始化队列和结果
        long_opens = []   # 存储开多时间
        short_opens = []  # 存储开空时间
        durations = []    # 存储持仓时长
        profit_loss_sum = []   # 盈亏加总
        deal_amt_list = []   # 成交金额

        # 遍历处理每一行
        last_action = ''
        temp_profit_sum = 0
        for idx, row in df.iterrows():
            action = row['buy_sell']
            ts = row['tsText']
            if action == '开多':
                if ts not in long_opens and not long_opens:
                    long_opens.append(ts)
                if '平多' in last_action:
                    profit_loss_sum.append(int(temp_profit_sum))
                    deal_amt_list.append(
                        row['price'] * row['amount'] * row['face_value'])
                    temp_profit_sum = 0
                last_action = action
            elif action == '平多':
                temp_profit_sum += row['profit_loss']
                if long_opens:
                    open_ts = long_opens.pop(0)
                    durations.append(ts - open_ts)
                last_action = action
            elif action == '开空':
                if ts not in short_opens and not short_opens:
                    short_opens.append(ts)
                if '平' in last_action:
                    profit_loss_sum.append(int(temp_profit_sum))
                    deal_amt_list.append(
                        row['price'] * row['amount'] * row['face_value'])
                    temp_profit_sum = 0
                last_action = action
            elif action == '平空':
                temp_profit_sum += row['profit_loss']
                if short_opens:
                    open_ts = short_opens.pop(0)
                    durations.append(ts - open_ts)
                last_action = action
        profit_loss_sum.append(int(temp_profit_sum))
        # deal_amt_list.append(df.iloc[-1]['price'] * df.iloc[-1]['amount'] * df.iloc[-1]['face_value'])
        
        # 结果统计
        trade_num = df.shape[0]     # 交易笔数
        # 开平仓次数   # len(durations) 这里差别很大，需要优化算法
        open_close_num = len(profit_loss_sum)
        user_id = df.iloc[0]['userid']
        if open_close_num == 0:
            print("没有成交记录")
        else:
            # 统计周期内的总盈利
            begin_time = str(df.iloc[0]['tsText'])
            end_time = str(df.iloc[-1]['tsText'])
            print(f"{user_id}用户{date_info}成交数据")
            print(f"时间范围{begin_time}——{end_time}")
            print(f"成交笔数:{trade_num}")
            print(f"开平仓次数: {open_close_num}")
            print(f"总盈亏: {sum(profit_loss_sum)}")
            print(f"平均每笔盈亏: {round(sum(profit_loss_sum) / open_close_num, 2)}")
            print(f"最大盈利: {max(profit_loss_sum)}")
            print(f"最大亏损: {min(profit_loss_sum)}")
            # 计算胜率
            profit_num = sum(pl > 0 for pl in profit_loss_sum)
            loss_num = sum(pl <= 0 for pl in profit_loss_sum)
            win_rate = round(profit_num / open_close_num * 100, 2)
            print(f"胜率: {win_rate}%")
            # 计算盈亏比
            profit_sum = sum(pl for pl in profit_loss_sum if pl > 0)
            loss_sum = sum(pl for pl in profit_loss_sum if pl < 0)
            try:
                profit_loss_ratio = round(
                    (profit_sum / profit_num) / abs(loss_sum / loss_num), 2)
            except:
                profit_loss_ratio = 0
            print(f"盈亏比: {profit_loss_ratio}")
            # 计算最大持仓时长
            max_duration = max(durations)
            print(f"最大持仓时长: {max_duration}")
            # 计算最短持仓时长
            min_duration = min(durations)
            print(f"最短持仓时长: {min_duration}")
            # 计算最大连续盈利笔数
            max_win_num = 0
            cur_win_num = 0
            for pl in profit_loss_sum:
                if pl > 0:
                    cur_win_num += 1
                    if cur_win_num > max_win_num:
                        max_win_num = cur_win_num
                else:
                    cur_win_num = 0
            print(f"最大连续盈利笔数: {max_win_num}")
            # 计算最大连续亏损笔数
            max_loss_num = 0
            cur_loss_num = 0
            for pl in profit_loss_sum:
                if pl < 0:
                    cur_loss_num += 1
                    if cur_loss_num > max_loss_num:
                        max_loss_num = cur_loss_num
                else:
                    cur_loss_num = 0
            print(f"最大连续亏损笔数: {max_loss_num}")
            # 计算单笔最大下单金额
            df['deal_amt'] = df['price'] * df['amount'] * df['face_value']
            max_deal_amt = df['deal_amt'].max()
            print(f"单笔最大下单金额: {int(max_deal_amt)}")
            # 计算平均时长（转换为总秒数再计算）
            total_seconds = sum(d.total_seconds() for d in durations)
            avg_seconds = total_seconds / open_close_num
            avg_duration = pd.Timedelta(seconds=avg_seconds)
            print(f"平均持仓时长: {avg_duration}")

            # 计算交易频率 日均/周均/月均交易次数
            date_end = datetime.datetime.strptime(
                end_time, "%Y-%m-%d %H:%M:%S")
            date_begin = datetime.datetime.strptime(
                begin_time, "%Y-%m-%d %H:%M:%S")
            days = (date_end-date_begin).days
            days = days if days > 0 else 1
            weeks = int(days/7) if int(days/7) > 0 else 1
            months = int(days/30) if int(days/30) > 0 else 1
            total_trades = len(profit_loss_sum)
            daily_trades = round(total_trades/days, 2)        # 日均
            weekly_trades = daily_trades*7
            monthly_trades = daily_trades*30
            # weekly_trades = int(total_trades/weeks)      # 周均
            # monthly_trades = int(total_trades/months)    # 月均
            print(
                f"日均交易次数: {daily_trades} 周均:{weekly_trades} 月均:{monthly_trades}")

            # 杠杆使用率： (合约用户) 平均或最大使用的杠杆倍数
            avg_leverage = round(df['multiple'].mean(), 2)
            max_leverage = df['multiple'].max()
            print(f"平均杠杆倍数: {avg_leverage} 最大杠杆倍数: {max_leverage}")

            # 夏普比率： (平均收益率 - 无风险利率) / 收益率标准差 (衡量风险调整后收益，需要计算一段时间内的收益率序列)
            # risk_free_rate = 0.02 / 252  # 年化2%，换算为日收益率（252个交易日）
            # return_list = [profit_loss_sum[i]/deal_amt_list[i]
            #                for i in range(len(profit_loss_sum))]

            # df_pro = df[df['profit_loss'] != 0]
            # df_pro['return'] = df_pro['profit_loss'] / df_pro['deal_amt']
            # mean_return = statistics.mean(return_list)
            # std_return = statistics.stdev(return_list)
            # sharpe_ratio = (mean_return - risk_free_rate) / std_return if std_return != 0 else None
            # print(f"夏普比率: {round(sharpe_ratio, 2)}")

            # 胜率盈亏比综合指标： 期望收益 = 胜率 * 平均盈利 - (1 - 胜率) * 平均亏损
            # 算法一
            # wins = df[df['profit_loss'] > 0]
            # losses = df[df['profit_loss'] < 0]
            # win_rate = len(wins) / len(df)
            # avg_profit = wins['profit_loss'].mean() if not wins.empty else 0
            # avg_loss = abs(losses['profit_loss'].mean()) if not losses.empty else 0
            # 算法二
            wins = np.array([pl for pl in profit_loss_sum if pl > 0])
            losses = np.array([pl for pl in profit_loss_sum if pl < 0])
            avg_profit = wins.mean() if wins.any() else 0
            avg_loss = abs(losses.mean()) if losses.any() else 0
            # print(win_rate, avg_profit, avg_loss)
            expected_return = win_rate/100 * \
                avg_profit - (1 - win_rate/100) * avg_loss
            if not expected_return:
                expected_return = 0
            print(f"期望收益:{round(expected_return, 2)}\n\n")

            # 最大回撤： 在选定周期内，账户净值从最高点到最低点的最大跌幅百分比
            # 仓位大小： 单笔交易金额占总资产的比例（平均或最大值）
            # 没有用户账户余额无法计算
            # 手续费占比： 总手续费 / 总交易额 * 100%
            
            # 把分析好的结果存入mysql数据库中,用于grafana分析用
            save_sql_data = [(user_id, date_info, trade_num, open_close_num, sum(profit_loss_sum), round(sum(profit_loss_sum) / open_close_num, 2), max(profit_loss_sum), min(profit_loss_sum), str(win_rate)+"%", profit_loss_ratio, str(max_duration), str(min_duration), 
                              max_win_num, max_loss_num, int(max_deal_amt), str(avg_duration), f"日均交易次数: {daily_trades} 周均:{weekly_trades} 月均:{monthly_trades}", f"平均杠杆倍数: {avg_leverage} 最大杠杆倍数: {max_leverage}", round(expected_return, 2))]
            await self.connect_db()
            try:
                insert_sql = f"INSERT INTO user_profit_analysis (user_id, asalysis_info, trade_num, open_close_num, profit, ave_profit, max_profit, max_loss, profit_ratio, profit_loss_ratio, max_holding_time, min_holding_time, max_win_num, max_loss_num, max_deal_amt, ave_holding_time, ave_deal_num, leverage, expected_return) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                print(f"保存数据:{insert_sql}\n{save_sql_data}")
                # 批量插入
                self.cursor.executemany(insert_sql, save_sql_data)
                self.db.commit()  # 提交事务
            except:
                mess = f"{datetime.datetime.now()} 用户分析数据保存mysql失败,报错:{traceback.format_exc()}"
                self.log.warning(mess)
                tb.warning(mess, 'risk')
                tb.sendmail(f'用户分析数据保存mysql失败', mess)
            self.db.close()   # 关闭数据库连接


def main():
    user_analysis().run()


if __name__ == '__main__':
    main()




