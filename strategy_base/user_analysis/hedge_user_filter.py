'''
    自动分析指定用户列表中是否有值得对冲的用户
    版本信息:v1.0.0
    日期:2025-10-09
    作者:sky

    给定用户列表
    根据条件筛选用户
'''

import os
import sys
import json
import datetime
import numpy as np
import pandas as pd
import traceback
import asyncio
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


class user_analysis(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self._load_config(config)  # 读取配置
        self.rc_task = rc.RestClient()
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef', '', dev=False)
        self.every_day_user = self.white_user    # 保存每日盈利达标用户
        self.every_week_user = self.white_user   # 保存每周盈利达标用户
        self.every_month_user = self.white_user  # 保存每月盈利达标用户
        self.filter_users = []                   # 保存所有E组用户
        self.all_hedge_mess = ""                 # 保存所有对冲的结果信息
        self.all_other_mess = ""                 # 保存所有过滤掉的结果信息
        
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]   # 批量生成所有参数

    async def on_first(self):
        # 日志
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        # await self.create_table()
        # await self.init_load_mysql()  # 查看表
        await self.update_tag_user()    # 更新标记用户
        self.log.info(f"策略初始化完成")
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(hour=f"*/8", minute="1"))   # 判断是否需要调整资费
        self.schedule.add_job(self.update_tag_user, CronTrigger(hour=f"*/8"))    # 判断是否需要调整资费
    
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
    
    async def init_load_mysql(self):
        await self.connect_db()
        try:
            self.cursor.execute(f"SELECT * FROM time_analysis_users")
            data = self.cursor.fetchall()
            print(data)
            for i in data:
                print(i)
        except:
            print("没有time_analysis_users表")
        self.db.close()
    
    async def load_mysql(self, datetime_info):
        await self.connect_db()
        self.cursor.execute("SELECT user_list FROM time_analysis_users WHERE datetime_info = %s", (datetime_info,))
        result = self.cursor.fetchone()
        self.log.info(f"{datetime_info}查询结果:{result}")
        if result:
            if datetime_info.endswith('day'):
                self.every_day_user = json.loads(result[0])
            elif datetime_info.endswith('week'):
                self.every_week_user = json.loads(result[0])
            elif datetime_info.endswith('month'):
                self.every_month_user = json.loads(result[0])
        else:
            self.log.info(f"{datetime_info}无数据")
        self.db.close()
        
    
    ''' ============================================================================'''
    ''' =============================== funcation =================================='''
    ''' ============================================================================'''
    # 发送tel
    async def send_tg(self, mess):
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4917988058, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4917988058, content=send_text)
    
    # 更新标记用户
    async def update_tag_user(self):
        temp_tag_list = []
        last_page = 0
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_tag_list(page=1, page_size=1000)
                    tag_list_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in temp_tag_list:
                            temp_tag_list.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, tag_list_page+1):
                    last_page = n
                    data = await self.rest.fetch_tag_list(page=n, page_size=1000)
                    for i in data['data']:
                        if i not in temp_tag_list:
                            temp_tag_list.append(i)
                    await asyncio.sleep(0.5)
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
                
        tag_users = {}
        for data in temp_tag_list:
            if data['tag'] in tag_users and data['user_id'] not in tag_users[data['tag']]:
                tag_users[data['tag']].append(data['user_id'])
            else:
                tag_users[data['tag']] = [data['user_id']]
        for tag, users in tag_users.items():
            if 'E' in tag:
                self.filter_users += users
        self.log.info(f"E组用户:{self.filter_users}")
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 计算时间
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        last_week = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        last_month = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        begin_date = [yesterday, last_week, last_month]
        
        # 从数据库读取分析用户
        # await self.load_mysql(f'{datetime.datetime.now().strftime("%Y-%m-%d")}_day')
        # await self.load_mysql(f'{self.last_monday.strftime("%Y-%m-%d")}_week')
        # await self.load_mysql(f'{self.last_month}_month')
        
        # 交易分析
        # await self.do_count(self.every_day_user, begin_date, "日度用户分析")
        # await self.do_count(self.every_week_user, begin_date, "周度用户分析")
        # await self.do_count(self.every_month_user, begin_date, "月度用户分析")
        
        # ETH近期盈利用户
        # user_list = ['583427','584070','584071','583425','538975','584597','588073','591711','588831','540519','559965','576326','587986','584307','237345','588524','583429','588912','559458','574807','532695','579101','588819','588955','588837','572638','573469','584949','583136','583605','587745','572317','551600','590060','572183','559430','592158','592322','588087','558940','587883','582633','579672','581380','592188','590795','580212','574248','588632''583427','584070','584071','583425','538975','584597','588073','591711','588831','540519','559965','576326','587986','584307','237345','588524','583429','588912','559458','574807','532695','579101','588819','588955','588837','572638','573469','584949','583136','583605','587745','572317','551600','590060','572183','559430','592158','592322','588087','558940','587883','582633','579672','581380','592188','590795','580212','574248','588632']
        # 每日分析脚本筛选出来的用户
        user_list = ['577744','540519','559430','572183','538401','588223','587745','574807','530352','581767','560175','487848','572024','575490','485087','591249','580212','557580','581380']
        user_list2 = [i for i in user_list if i not in self.filter_users]
        self.log.info(f"原始用户:{len(user_list)}名 筛选出非E组用户:{len(user_list2)}名")
        await self.do_count(user_list2, [last_month], "总体用户分析")
        
        tg_mess = "筛选对冲用户规则:\n1. 3个月总盈利超过1万usdt\n2. 开平仓次数超过30次\n3. 盈亏比大于0.5\n4. 胜率*盈亏比-(赔率)>2\n\n"
        self.log.info(f"被过滤掉的用户:\n{self.all_other_mess}\n\n\n需要对冲的用户:{self.all_hedge_mess}")
        tg_mess += f"被过滤掉的用户:\n{self.all_other_mess}\n\n\n需要对冲的用户:{self.all_hedge_mess}\n具体查看这些用户的交易情况,最终决定是否对冲"
        await self.send_tg(tg_mess)
    
    async def do_count(self, usr_ids, begin_date, title):
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
            user_mess = f"{title}\n"
            all_hedge_tag = True
            for n, filter_date in enumerate(begin_date):
                date_info = "近3月"   #f"近1天" if n == 0 else f"近1周" if n == 1 else f"近1月"
                num = 0
                filter_num = 0
                profit_sum = 0
                user_list = []
                for deal in temp_data:
                    symbol = deal['symbol']
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
                    ll = [usr_id, date, symbol, side, float(profit), float(
                        price), float(amount), float(face_value), int(multiple)]
                    user_list.append(ll)
                # print(
                #     f"{usr_id}用户,统计时间:{filter_date}至今,共{num}条数据,过滤{filter_num}条,总盈亏:{int(profit_sum)}")

                # 生成dataframe
                df = pd.DataFrame(user_list, columns=[
                                  'userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'face_value', 'multiple'])
                await self.fenxi(usr_id, df, date_info)
            #     user_mess += f"{mess}"
            #     if not hedge_tag:
            #         all_hedge_tag = False
            # if all_hedge_tag:
            #     self.log.info(f"{usr_id}用户需要对冲")
            #     await self.send_tg(user_mess)

    async def fenxi(self, usr_id, df, date_info):
        info = self.user_info.get(usr_id, '普通用户')
        if df.shape[0] == 0:
            self.log.info(f"{usr_id}{info} {date_info}无成交记录")
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
                    profit_loss_sum.append([row['symbol'], int(temp_profit_sum)])
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
                    profit_loss_sum.append([row['symbol'], int(temp_profit_sum)])
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
        
        # 结果统计
        trade_num = df.shape[0]     # 交易笔数
        # 开平仓次数   # len(durations) 这里差别很大，需要优化算法
        open_close_num = len(profit_loss_sum)
        user_id = df.iloc[0]['userid']
        if open_close_num == 0:
            self.log.info(f"{user_id}用户{date_info}没有平仓记录")
        
        # 统计周期内的总盈利
        mess = ""
        begin_time = str(df.iloc[0]['tsText'])
        end_time = str(df.iloc[-1]['tsText'])
        mess += (f"{user_id}用户{date_info}成交数据\n")
        mess += (f"时间范围{begin_time}——{end_time}\n")
        mess += (f"成交笔数:{trade_num}\n")
        mess += (f"开平仓次数: {open_close_num}\n")
        temp_profit = [i[1] for i in profit_loss_sum]
        mess += (f"总盈亏: {sum(temp_profit)}\n")
        # 分币对盈亏
        profit_loss_dict = {}
        for i in profit_loss_sum:
            if i[0] in profit_loss_dict:
                profit_loss_dict[i[0]] += i[1]
            else:
                profit_loss_dict[i[0]] = i[1]
        mess += (f"分币对盈亏:\n")
        for i in profit_loss_dict:
            mess += f"   {i}: {profit_loss_dict[i]}\n"
        profit_loss_sum = temp_profit
        ave_profit = round(sum(profit_loss_sum) / open_close_num, 2) if open_close_num else 0
        mess += (f"平均每笔盈亏: {ave_profit}\n")
        max_profit = max(profit_loss_sum) if profit_loss_sum else 0
        min_profit = min(profit_loss_sum) if profit_loss_sum else 0
        mess += (f"最大盈利: {max_profit}\n")
        mess += (f"最大亏损: {min_profit}\n")
        
        # 计算胜率
        profit_num = sum(pl > 0 for pl in profit_loss_sum)
        loss_num = sum(pl <= 0 for pl in profit_loss_sum)
        try:
            win_rate = round(profit_num / open_close_num * 100, 2)
        except:
            win_rate = 0
        mess += (f"胜率: {win_rate}%\n")
        
        # 计算盈亏比
        profit_sum = sum(pl for pl in profit_loss_sum if pl > 0)
        loss_sum = sum(pl for pl in profit_loss_sum if pl < 0)
        try:
            profit_loss_ratio = round(
                (profit_sum / profit_num) / abs(loss_sum / loss_num), 2)
        except:
            profit_loss_ratio = 0
        mess += (f"盈亏比: {profit_loss_ratio}\n")
        
        # 计算最大持仓时长
        max_duration = max(durations) if durations else 0
        mess += (f"最大持仓时长: {max_duration}\n")
        
        # 计算最短持仓时长
        min_duration = min(durations) if durations else 0
        mess += (f"最短持仓时长: {min_duration}\n")
        
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
        mess += (f"最大连续盈利笔数: {max_win_num}\n")
        
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
        mess += (f"最大连续亏损笔数: {max_loss_num}\n")
        
        # 计算单笔最大下单金额
        df['deal_amt'] = df['price'] * df['amount'] * df['face_value']
        max_deal_amt = df['deal_amt'].max()
        mess += (f"单笔最大下单金额: {int(max_deal_amt)}\n")
        
        # 计算平均时长（转换为总秒数再计算）
        total_seconds = sum(d.total_seconds() for d in durations)
        if open_close_num:
            avg_seconds = total_seconds / open_close_num
            avg_duration = pd.Timedelta(seconds=avg_seconds)
        else:
            avg_duration = 0
        mess += (f"平均持仓时长: {avg_duration}\n")

        # 计算交易频率 日均/周均/月均交易次数
        date_end = datetime.datetime.strptime(
            end_time, "%Y-%m-%d %H:%M:%S")
        date_begin = datetime.datetime.strptime(
            begin_time, "%Y-%m-%d %H:%M:%S")
        days = (date_end-date_begin).days
        days = days if days > 0 else 1
        total_trades = len(profit_loss_sum)
        daily_trades = round(total_trades/days, 2)  # 日均
        weekly_trades = round(daily_trades*7,1)     # 周均
        monthly_trades = round(daily_trades*30,1)   # 月均
        mess += (f"日均交易次数: {daily_trades} 周均:{weekly_trades} 月均:{monthly_trades}\n")

        # 杠杆使用率： (合约用户) 平均或最大使用的杠杆倍数
        avg_leverage = round(df['multiple'].mean(), 2)
        max_leverage = df['multiple'].max()
        mess += (f"平均杠杆倍数: {avg_leverage} 最大杠杆倍数: {max_leverage}\n")

        # 期望收益
        wins = np.array([pl for pl in profit_loss_sum if pl > 0])
        losses = np.array([pl for pl in profit_loss_sum if pl < 0])
        avg_profit = wins.mean() if wins.any() else 0
        avg_loss = abs(losses.mean()) if losses.any() else 0
        expected_return = win_rate/100 * \
            avg_profit - (1 - win_rate/100) * avg_loss
        if not expected_return:
            expected_return = 0
        mess += (f"期望收益:{round(expected_return, 2)}\n\n")
        # self.log.info(mess)
        
        # 对冲判断
        hedge_tg = False
        # 总盈亏大于10000
        con1 = sum(temp_profit) > 10000 if date_info == '近3月' else True
        con2 = win_rate*profit_loss_ratio-(100-win_rate) > 2   # 胜率*盈亏比-赔率 > 2
        # 开平仓次数大于30次
        con3 = open_close_num >= 30 if date_info == '近3月' else True        
        con4 = profit_loss_ratio > 0.5      # 盈亏比大于0.5
        if con1 and con2 and con3 and con4:
            hedge_tg = True
            hedge_mess = f"{user_id}用户需要对冲\n{mess}"
            self.all_hedge_mess += hedge_mess
        else:
            other_mess = (f"{user_id}用户{date_info}交易数据,不满足对冲条件。总盈利:{sum(temp_profit)} 胜率:{win_rate}% 盈亏比:{profit_loss_ratio} 开平仓次数:{open_close_num}\n")
            self.all_other_mess += other_mess
        
        # return mess, False
        

def main(path='time_analysis_user_config.py'):
    config = __import__(path.split(".py")[0])
    user_analysis(config).run()


if __name__ == '__main__':
    main()


