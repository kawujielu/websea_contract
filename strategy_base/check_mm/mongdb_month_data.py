'''
    用户交易行为分析:
    1、每次启动获取全量数据
    2、可以按照用户id查询分析
'''

import time
import datetime
import statistics
import numpy as np
import pandas as pd
import asyncio
import motor.motor_asyncio


mm_id = ['18','19','20','21','22','476515','465880']
buy_sell_side = {'1':'开多', '2':'开空', '3':'平多', '4':'平空'}

async def do_count():
    t1 = time.time()
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
    db = client.exchange
    collection = db.real_contract_deal
    await collection.create_index("ts")

    # 指定日期时间戳
    begin_ts = int(datetime.datetime(2025, 12, 30, 0, 0, 0).timestamp())
    end_ts = int(datetime.datetime(2025, 12, 31, 0, 0, 0).timestamp())
    query = {
            'ts': {
                '$gte': begin_ts,
                '$lt': end_ts
            }
        }
    print(begin_ts, end_ts)
    result = collection.find(query)
    t2 = time.time()
    print(f"查询耗时:{round(int(t2-t1)/60, 2)}min")
    num = 0
    profit_sum = 0
    user_profit_dict = {}
    async for deal in result:
        print(deal)
        return
        symbol = deal['symbol']
        date = datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
        temp_date = date.split(' ')[0]
        num += 1
        #continue
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
            order_id = deal['takerOrder']
        if deal['makerUser'] == usr_id:
            side_type = deal['makerBuyOrSell']
            fee = deal['makerFee']
            profit = deal['makerProfitLoss']
            multiple = deal['makerMultiple']
            order_id = deal['makerOrder']
        side = buy_sell_side[str(side_type)]
        taker_side = deal['takerBuyOrSell']
        maker_side = deal['makerBuyOrSell']
        profit_sum += float(profit)
        user_profit_dict[usr_id] = profit_sum

        # 按user_profit_dict的盈利排序
        sorted_user_profit_dict = dict(sorted(user_profit_dict.items(), key=lambda item: item[1], reverse=True))
        print(sorted_user_profit_dict)


def fenxi(df):
    
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
                deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
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
                deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
                temp_profit_sum = 0
            last_action = action
        elif action == '平空':
            temp_profit_sum += row['profit_loss']
            if short_opens:
                open_ts = short_opens.pop(0)
                durations.append(ts - open_ts)
            last_action = action
    # profit_loss_sum.append(int(temp_profit_sum))
    try:
        deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
    except:
        pass

    # 结果统计
    trade_num = df.shape[0]     # 交易笔数
    num_trades = len(profit_loss_sum)  # len(durations) 这里差别很大，需要优化算法
    if num_trades == 0:
        print("没有成交记录")
    else:
        # 统计周期内的总盈利
        begin_time = str(df.iloc[0]['tsText'])
        end_time = str(df.iloc[-1]['tsText'])
        trade_days = (df.iloc[-1]['tsText'] - df.iloc[0]['tsText']).days
        print(f"\n{df.iloc[0]['userid']}统计周期{begin_time}——{end_time},共{trade_days}天")
        print(f"成交笔数:{trade_num}")
        print(f"开平仓次数: {len(profit_loss_sum)}")   #  盈亏次数: {len(profit_loss_sum)}")
        # 分币对盈亏
        profit_loss_dict = {}
        for i in profit_loss_sum:
            if i[0] in profit_loss_dict:
                profit_loss_dict[i[0]] += i[1]
            else:
                profit_loss_dict[i[0]] = i[1]
        mess = ""
        for i in profit_loss_dict:
            mess += f"{i}: {profit_loss_dict[i]}\n"
        profit_loss_sum = [i[1] for i in profit_loss_sum]
        print(f"总盈亏: {sum(profit_loss_sum)}")
        print(f"分币对盈亏:\n{mess}")
        print(f"平均每笔盈亏: {round(sum(profit_loss_sum) / num_trades, 2)}")
        print(f"最大盈利: {max(profit_loss_sum)}")
        print(f"最大亏损: {min(profit_loss_sum)}")
        # 计算胜率
        profit_num = sum(pl > 0 for pl in profit_loss_sum)
        loss_num = sum(pl <= 0 for pl in profit_loss_sum)
        win_rate = round(profit_num / num_trades * 100, 2)
        print(f"胜率: {win_rate}%")
        # 计算盈亏比 
        profit_sum = sum(pl for pl in profit_loss_sum if pl > 0)
        loss_sum = sum(pl for pl in profit_loss_sum if pl < 0)
        try:
            profit_loss_ratio = round((profit_sum / profit_num) / abs(loss_sum / loss_num), 2)
        except:
            profit_loss_ratio = None
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
        total_deal_amt = df['deal_amt'].sum()
        print(f"统计区间内总交易金额:{int(total_deal_amt)}U")
        #print(f"统计区间内日均交易金额:{int(total_deal_amt/trade_days)}U")
        print(f"单笔最大下单金额: {int(max_deal_amt)}")
        print(f"盈利占比:{round(sum(profit_loss_sum)/total_deal_amt*100, 3)}%")
        # 计算平均时长（转换为总秒数再计算）
        total_seconds = sum(d.total_seconds() for d in durations)
        avg_seconds = total_seconds / num_trades
        avg_duration = pd.Timedelta(seconds=avg_seconds)
        print(f"平均持仓时长: {avg_duration}")
        print(f"中位数持仓时长: {statistics.median(durations)}")
        print(f"众数持仓时长:{statistics.mode(durations)}")
        
        # 计算交易频率 日均/周均/月均交易次数
        date_end = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        date_begin = datetime.datetime.strptime(begin_time, "%Y-%m-%d %H:%M:%S")
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
        print(f"日均交易次数: {daily_trades} 周均:{weekly_trades} 月均:{monthly_trades}")
        
        # 杠杆使用率： (合约用户) 平均或最大使用的杠杆倍数
        avg_leverage = round(df['multiple'].mean(), 2)
        max_leverage = df['multiple'].max()
        print(f"平均杠杆倍数: {avg_leverage} 最大杠杆倍数: {max_leverage}")
        
        # 夏普比率： (平均收益率 - 无风险利率) / 收益率标准差 (衡量风险调整后收益，需要计算一段时间内的收益率序列)
        risk_free_rate = 0.02 / 252  # 年化2%，换算为日收益率（252个交易日）
        return_list = [profit_loss_sum[i]/deal_amt_list[i] for i in range(len(profit_loss_sum))]
        
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
        avg_profit = wins.mean()
        avg_loss = abs(losses.mean())
        # print(win_rate, avg_profit, avg_loss)
        expected_return = win_rate/100 * avg_profit - (1 - win_rate/100) * avg_loss
        print(f"期望收益:{round(expected_return, 2)}")
        
        # 最大回撤： 在选定周期内，账户净值从最高点到最低点的最大跌幅百分比
        # 仓位大小： 单笔交易金额占总资产的比例（平均或最大值）
        # 没有用户账户余额无法计算
        # 手续费占比： 总手续费 / 总交易额 * 100%


loop = asyncio.get_event_loop()
df = loop.run_until_complete(do_count())






