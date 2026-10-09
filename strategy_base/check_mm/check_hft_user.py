'''
    用户交易行为分析:
    1、每次启动获取全量数据
    2、可以按照用户id查询分析
'''

import datetime
import statistics
import numpy as np
import pandas as pd
import asyncio
import motor.motor_asyncio


mm_id = ['18','19','20','21','22','476515','465880']
buy_sell_side = {'1':'开多', '2':'开空', '3':'平多', '4':'平空'}

async def do_count():
    # AsyncIOMotorClient 代表一个mongod进程，或者它们的一个集群。显式创建这些客户端对象之一，将其连接到正在运行的 mongod，并在应用程序的整个生命周期中使用它。
    client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
    # AsyncIOMotorDatabase：每个 mongod 都有一组数据库（磁盘上不同的数据文件集）。可以从客户端获取对数据库的引用。
    db = client.exchange

    info = await db.list_collection_names()
    print(f"集合列表:{info}")
    
    # AsyncIOMotorCollection：一个数据库有一组集合，其中包含文档；从数据库中获得对集合的引用。
    collection = db.real_contract_deal

    usr_ids = ['624265']  #["519242","534681","552752","577558","582711","583548"]  #['617185','617187','617188','617205','617213','617214']  #['602886','540519','608922','608240','607250','607254','571577','574458','605434','609534','587222','589177','557509','588673','607622','607514','589797']  # 11-24盈利用户['551107','572887','513735','462703','513735','579222','598461','603259','534407','462703','534407','462703','559667','538401','572183','572183']  # 11-01盈利用户['577744','540519','559430','572183','538401','588223','587745','574807','530352','581767','560175','487848','572024','575490','485087','591249','580212','557580','581380']
    begin_date = "2025-01-01"
    end_date = "2026-04-01"

    for usr_id in usr_ids:
        # 查找takerUser或makerUser至少一个字段包含468958值。
        query = {
            "$or": [
                {"takerUser": {"$regex": usr_id}},
                {"makerUser": {"$regex": usr_id}}
            ]
            #"symbol": "CFX-USDT",
        }

        # 查询某2个字段的值不同时在指定列表中的数据
        # query = {
        #     "$or": [
        #         {"takerUser": {"$nin": mm_id}},
        #         {"makerUser": {"$nin": mm_id}},
        #         # {"ts": {"$gt": 1745856000}},    # 过滤ts字段所有大于1743436800的数据
        #     ]
        # }
        
        # find()查找所有内容, sort()按照dealId字段-1降序排序, limit()限制返回条数
        result = collection.find(query).sort("dealId", -1)  #.limit(1000)
        # result = collection.aggregate(pipeline, allowDiskUse=True)    # pipeline必需是list
        # result = await collection.find_one()  # 查询最新一条数据
        num = 0
        profit_sum = 0
        user_list = []
        async for deal in result:
            #print(deal)
            #return
            symbol = deal['symbol']
            date = datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
            temp_date = date.split(' ')[0]
            if temp_date < begin_date or temp_date > end_date:
                continue
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
            if deal['makerUser'] == usr_id:
                side_type = deal['makerBuyOrSell']
                fee = deal['makerFee']
                profit = deal['makerProfitLoss']
                multiple = deal['makerMultiple']
            side = buy_sell_side[str(side_type)]
            taker_side = deal['takerBuyOrSell']
            maker_side = deal['makerBuyOrSell']
            profit_sum += float(profit)
            # print(taker, maker, price, amount, taker_side, maker_side)
            # print(f"{deal['takerUser']} takerOrderSource:{deal['takerOrderSource']} takerBuyOrSell:{deal['takerBuyOrSell']}\ndeal['makerUser'] makerOrderSource:{deal['makerOrderSource']} makerBuyOrSell:{deal['makerBuyOrSell']}")
            # if deal['symbol'] == 'COAI-USDT':
            try:
                last_side
            except:
                last_side = 1
            if side != last_side:
                print(f"{datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')} {deal['symbol']} {side} 成交价:{price} 数量:{amount} 盈亏:{round(float(profit), 2)} 手续费:{round(float(fee), 2)} {taker} {maker}")
            last_side = side
            ll = [usr_id, date, symbol, side, float(profit), float(price), float(amount), float(face_value), int(multiple)]
            user_list.append(ll)
            
            # {'_id': ObjectId('6810705b6990ca86be1844bf'), 'dealId': 1745907803673019, 'symbol': 'SUI-USDT', 
            # 'takerUser': '468958', 'makerUser': '22', 'takerOrder': 'BM46895817459078036625RH6BH', 
            # 'makerOrder': 'SL2217459078027397ZPRQJ', 'takerFee': '0.180132', 'makerFee': '0', 
            # 'price': '3.532', 'amount': '85', 'type': 'buy', 'ts': 1745907803, 'createTime': 0, 'updateTime': 0, 
            # 'updateUser': '', 'isDeleted': 0, 'tradeId': 40, 'settleId': 2, 'tradeToBase': '', 
            # 'sellteToBase': '', 'takerOrderSource': 1, 'makerOrderSource': 1, 'makerBood': '60.044', 
            # 'takerBood': '30.022', 'makerProfitLoss': '27.035552562474764405', 
            # 'takerProfitLoss': '0.085000000000000000', 'takerBuyOrSell': '4', 'makerBuyOrSell': '3', 
            # 'takerMultiple': '10', 'makerMultiple': '5', 'takerApiSource': '2', 'makerApiSource': '4', 
            # 'takerApiKey': '', 'makerApiKey': '', 'takerOpenAvgPrice': '3.533', 'makerOpenAvgPrice': '3.213934675735591007', 
            # 'takerUserRank': '', 'makerUserRank': '', 'takerDealRate': '', 'takerGearLadder': '', 
            # 'takerGearNum': '', 'makerDealRate': '', 'makerGearLadder': '', 'makerGearNum': '', 
            # 'needConver': '1', 'takerIsQuantUser': '0', 'makerIsQuantUser': '1', 'takerFaceValue': '1', 
            # 'makerFaceValue': '1', 'takerOrderBalance': '434', 'makerOrderBalance': '0', 
            # 'takerFeeBalance': '-0.5149656', 'makerFeeBalance': '0', 'takerBondBalance': '-85.8276', 
            # 'makerBondBalance': '-60.044', 'makerIsFull': 1, 'takerIsFull': 2, 
            # 'takerEntrustBondToHoldBond': '', 'makerEntrustBondToHoldBond': '', 'takerHoldBondToAvailable': '', 
            # 'makerHoldBondToAvailable': '', 'takerIndexSourceRateMark': '', 'makerIndexSourceRateMark': '', 
            # 'takerIndexComponent': '', 'makerIndexComponent': '', 'isIndex': 0, 'isExperience': 0, 
            # 'takerCouponId': 0, 'makerCouponId': 0, 'takerCouponNum': '', 'makerCouponNum': '', 
            # 'takerDeductionFee': '', 'makerDeductionFee': '', 'takerIsTrader': 0, 'makerIsTrader': 0, 
            # 'takerEntrustAmount': '677', 'makerEntrustAmount': '', 'takerShareProfit': '', 
            # 'makerShareProfit': '', 'takerPointCard': '', 'makerPointCard': '', 'takerPointCardRate': '', 
            # 'makerPointCardRate': '', 'takerPointCardCurrencyId': 0, 'makerPointCardCurrencyId': 0, 
            # 'takerPointCardName': '', 'makerPointCardName': '', 'takerResidueFee': '0.1801320', 
            # 'makerResidueFee': '0', 'takerIsExperience': 0, 'makerIsExperience': 0, 'takerExperienceId': 0, 
            # 'makerExperienceId': 0, 'takerExperienceExpireTime': 0, 'makerExperienceExpireTime': 0, 
            # 'makerTransLoss': '', 'takerTransLoss': '', 'takerFeeExpGold': '', 'makerFeeExpGold': '', 
            # 'takerLossExpGold': '', 'makerLossExpGold': '', 'takerResidueExpGold': '', 
            # 'makerResidueExpGold': '', 'takerPlatForm': '', 'makerPlatForm': '', 'takerPlatFormToU': '', 
            # 'makerPlatFormToU': '', 'takerPlatFormRate': '', 'makerPlatFormRate': '', 
            # 'takerPlatFormCurrencyId': 0, 'makerPlatFormCurrencyId': 0, 'takerPlatFormName': '', 
            # 'makerPlatFormName': '', 'takerPlatFormPrice': '', 'makerPlatFormPrice': '', 
            # 'takerIsPlatformDec': 0, 'makerIsPlatformDec': 0, 'takerIsProtected': 0, 'makerIsProtected': 0, 
            # 'takerSubId': 0, 'makerSubId': 0, 'takerCustomizeId': '', 'makerCustomizeId': '', 
            # 'takerAccount': '468958', 'makerAccount': '22', 'takerOpenRate': '', 'takerDocumentaryType': 0, 
            # 'takerDocumentaryAmount': '', 'makerOpenRate': '', 'makerDocumentaryType': 0, 
            # 'makerDocumentaryAmount': ''}

        print(f"{usr_id}用户共{num}条数据, 总盈亏:{int(profit_sum)}")
        
        # 生成dataframe
        df = pd.DataFrame(user_list, columns=['userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'face_value', 'multiple'])
        # print(df.shape)
        # print(df.head(3))
        # return df
        fenxi(df)


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
        trade_days = 1 if trade_days < 1 else int(trade_days)
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
        # 计算单笔最大下单金额
        df['deal_amt'] = df['price'] * df['amount'] * df['face_value']
        max_deal_amt = df['deal_amt'].max()
        total_deal_amt = df['deal_amt'].sum()
        print(f"统计区间内总交易金额:{int(total_deal_amt)}U")
        print(f"统计区间内日均交易金额:{int(total_deal_amt/trade_days)}U")
        print(f"单笔最大下单金额: {int(max_deal_amt)}")
        print(f"盈利占比:{round(sum(profit_loss_sum)/total_deal_amt*100, 3)}%")
        # 计算平均时长（转换为总秒数再计算）
        total_seconds = sum(d.total_seconds() for d in durations)
        avg_seconds = total_seconds / num_trades
        avg_duration = pd.Timedelta(seconds=avg_seconds)
        print(f"平均持仓时长: {avg_duration}")
        print(f"中位数持仓时长: {statistics.median(durations)}")
        print(f"众数持仓时长:{statistics.mode(durations)}")
        # 计算durations数据中小于5min的次数占全部数据的比例
        less_than_5_num = sum(1 for d in durations if d.total_seconds() < 60)
        less_than_5_ratio = round(less_than_5_num / len(durations) * 100, 2)
        print(f"持仓时长小于1min的次数占比: {less_than_5_ratio}%")
        less_than_5_num = sum(1 for d in durations if d.total_seconds() < 300)
        less_than_5_ratio = round(less_than_5_num / len(durations) * 100, 2)
        print(f"持仓时长小于5min的次数占比: {less_than_5_ratio}%")
        less_than_5_num = sum(1 for d in durations if d.total_seconds() < 600)
        less_than_5_ratio = round(less_than_5_num / len(durations) * 100, 2)
        print(f"持仓时长小于10min的次数占比: {less_than_5_ratio}%")
        duration_5min = [d for d in durations if d.total_seconds() < 300]
        if duration_5min:
            total_seconds = sum(d.total_seconds() for d in duration_5min)
            avg_seconds = total_seconds / len(duration_5min) if len(duration_5min) != 0 else None
            avg_duration = pd.Timedelta(seconds=avg_seconds) if avg_seconds is not None else None
            print(f"小于5min持仓的平均持仓时长: {avg_duration}")
            print(f"小于5min持仓的中位数持仓时长: {statistics.median(duration_5min)}")
            print(f"小于5min持仓的众数持仓时长:{statistics.mode(duration_5min)}")
        else:
            print(f"没有5min以下的持仓")

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
        

loop = asyncio.get_event_loop()
df = loop.run_until_complete(do_count())




