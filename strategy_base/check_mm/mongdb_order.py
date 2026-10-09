'''
    用户交易行为分析:
    1、每次启动获取全量数据
    2、可以按照用户id查询分析
'''

import datetime
import statistics

# Mongo 查询时间边界用北京时间，与下方成交展示（UTC+8）一致
_BJT = datetime.timezone(datetime.timedelta(hours=8))


def _bjt_date_range_to_ts(begin_date: str, end_date: str):
    """begin_date / end_date 为 'YYYY-MM-DD'，闭区间 [当日 00:00, 当日 23:59:59] BJT。"""
    lo = datetime.datetime.strptime(begin_date, "%Y-%m-%d").replace(tzinfo=_BJT)
    hi = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=_BJT
    )
    return int(lo.timestamp()), int(hi.timestamp())
import numpy as np
import pandas as pd
import asyncio
from collections import Counter, deque
import motor.motor_asyncio

buy_sell_side = {'1':'开多', '2':'开空', '3':'平多', '4':'平空'}
OPEN_ACTIONS = {"开多", "开空"}
CLOSE_ACTIONS = {"平多", "平空"}


async def do_count():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/")  #('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
    db = client.exchange
    collection = db.real_contract_deal

    usr_ids = ['554821']  #['1501615','558127','1481746','1493525','487848','1502813','1504495','1489777','1502806','1502809','1504078','1483840','611671','1503189','538386','1498135','617474','1502472','351087','605908']  #['535229',        '556300',    '557138',     '571775',      '573583',     '577877',     '580609',    '618546',     '632588',     '638112'] # Z2组用户   #['644818','670240','645543','645804','670019','632471','632439','633917','631939','632511','632469','632512','632506']  #['632588','621582','539308','556300','643053','591460','618378','638112','647069','538033','646515','641559','649984','580060','548089','645459','645429','620804','641168','640839','546375','639519','556374','612461','574305','639481','575822','588138','628459','457138','640053','571775','633213','623388','552445','578085','574458','632681','523679','573455','552522','593896','633241','640276','618413','628914','572522','640055','592256','590648']
    begin_date = "2026-9-01"  # 查询此时间后的数据（含当日，北京时间）
    end_date = "2026-10-01"

    ts_min, ts_max = _bjt_date_range_to_ts(begin_date, end_date)
    all_profit = 0
    for usr_id in usr_ids:
        uid = str(usr_id)
        # 库中 user 字段可能是 str 或 int，小列表 $in 仍可走索引
        uid_terms = [uid]
        if uid.isdigit():
            try:
                uid_terms.append(int(uid))
            except ValueError:
                pass
        # 时间下推到库端；用户条件用等值/$in（可走索引），避免 $regex 全表扫描。
        # 每个 $or 分支同时带 ts，便于 (takerUser, ts) / (makerUser, ts) 类复合索引。
        ts_rng = {"$gte": ts_min, "$lte": ts_max}
        query = {
            "$or": [
                {"takerUser": {"$in": uid_terms}, "ts": ts_rng},
                {"makerUser": {"$in": uid_terms}, "ts": ts_rng},
            ]
        }
        result = collection.find(query).sort("ts", 1)
        deal_num = 0    # 逐笔成交次数
        profit_sum = 0
        user_list = []
        deals_by_order_id = {}
        async for deal in result:
            #print(deal)
            #return
            symbol = deal['symbol']
            #if symbol not in ['BTC-USDT','ETH-USDT']:
            #    continue
            taker = deal['takerUser']
            maker = deal['makerUser']
            price = float(deal['price'])
            amount = float(deal['amount'])
            face_value = deal['takerFaceValue']   # 合约单位
            makerIsProtected = deal['makerIsProtected']  # maker订单是否为保本
            takerIsProtected = deal['takerIsProtected']  # taker订单是否为保本
            makerSubId = deal['makerSubId']  # 订阅ID
            takerSubId = deal['takerSubId']  # 订阅ID
            if str(taker) == uid:
                side_type = deal['takerBuyOrSell']
                fee = deal['takerFee']
                profit = deal['takerProfitLoss']
                multiple = deal['takerMultiple']
                order_id = deal['takerOrder']
                deal_is_protected = takerIsProtected
                deal_sub_id = takerSubId
            elif str(maker) == uid:
                side_type = deal['makerBuyOrSell']
                fee = deal['makerFee']
                profit = deal['makerProfitLoss']
                multiple = deal['makerMultiple']
                order_id = deal['makerOrder']
                deal_is_protected = makerIsProtected
                deal_sub_id = makerSubId
            else:
                continue
            deal_num += 1
            side = buy_sell_side[str(side_type)]
            # taker_side = deal['takerBuyOrSell']
            # maker_side = deal['makerBuyOrSell']
            profit_sum += float(profit)
            date = datetime.datetime.utcfromtimestamp(deal['ts']) + datetime.timedelta(hours=8)
            date = date.strftime('%Y-%m-%d %H:%M:%S')
            try:
                deals_by_order_id[order_id].append([date, deal['symbol'], side, price, amount, profit, fee, taker, maker, multiple, face_value, deal_is_protected, deal_sub_id])
            except:
                deals_by_order_id[order_id] = [[date, deal['symbol'], side, price, amount, profit, fee, taker, maker, multiple, face_value, deal_is_protected, deal_sub_id]]
            # last_side = side
            # ll = [usr_id, date, symbol, side, float(profit), float(price), float(amount), float(face_value), int(multiple)]
            # user_list.append(ll)
            
        trade_type_stats = Counter()
        for order_id, deals in deals_by_order_id.items():
            date = deals[0][0]
            symbol = deals[0][1]
            side = deals[0][2]
            multiple = deals[0][9]
            face_value = float(deals[0][10])
            amount = sum([float(i[4]) for i in deals])*face_value
            amt = sum([float(i[3])*float(i[4]) for i in deals])*face_value
            taker_amt = sum([float(i[3])*float(i[4]) for i in deals if str(i[7]) == uid])*face_value
            maker_amt = sum([float(i[3])*float(i[4]) for i in deals if str(i[8]) == uid])*face_value
            price = round(amt/amount, 6)
            profit = sum([float(i[5]) for i in deals])
            fee = sum([float(i[6]) for i in deals])
            taker_num = len([1 for i in deals if str(i[7]) == uid])
            maker_num = len([1 for i in deals if str(i[8]) == uid])
            for deal in deals:
                deal_is_protected, deal_sub_id = deal[11], deal[12]
                if deal_is_protected == 0 and deal_sub_id == 0:
                    trade_type_stats['自主交易'] += 1
                elif deal_is_protected != 0 and deal_sub_id != 0:
                    trade_type_stats['跟单交易'] += 1
                else:
                    trade_type_stats['其他'] += 1

            user_list.append([uid, date, symbol, side, float(profit), float(price), float(amount), amt, int(multiple), fee, taker_num, maker_num, taker_amt, maker_amt, trade_type_stats])
            print(f"{date} {symbol} {side} 成交均价:{price} 成交数量:{round(amount, 4)} 盈亏:{round(profit, 2)}, 总手续费:{round(fee, 4)} taker成交次数:{taker_num} maker成交次数:{maker_num}")
        print(f"{usr_id}用户共{deal_num}条数据, 总盈亏:{int(profit_sum)} 自主交易:{trade_type_stats['自主交易']} 跟单交易:{trade_type_stats['跟单交易']} 其他:{trade_type_stats['其他']}")
        all_profit += int(profit_sum)
        # user_list = [
        #     [537918, '2025-09-24 16:04:27', 'ETH-USDT', '开多', 0, 10000, 5, 50000, 3, 10, 1, 0],
        #     [537918, '2025-09-24 15:14:27', 'ETH-USDT', '平多', 300, 10000, 10, 50000, 3, 10, 1, 0],
        #     [537918, '2025-09-24 14:04:27', 'ETH-USDT', '开多', 0, 10000, 5, 50000, 3, 10, 1, 0],
        #     [537918, '2025-06-24 15:14:27', 'ETH-USDT', '平多', 100, 10000, 5, 100000, 2, 20, 1, 0],
        #     [537918, '2025-06-24 14:14:27', 'ETH-USDT', '平空', -100, 10000, 10, 200000, 5, 40, 1, 0],
        #     [537918, '2025-06-24 10:14:27', 'ETH-USDT', '开空', 0, 10000, 10, 200000, 5, 40, 1, 0],
        #     [537918, '2025-06-24 09:14:27', 'ETH-USDT', '开多', 0, 10000, 10, 100000, 2, 20, 1, 0],
        # ]
        
        df = pd.DataFrame(user_list, columns=['userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'amt', 'multiple', 'fee', 'taker_num', 'maker_num', 'taker_amt', 'maker_amt', 'trade_type_stats'])
        # 如果是空数据,则返回
        if df.empty:
            print(f"{usr_id}无数据")
        else:
            res = calc_trade_stats(df)
            for k, v in res.items():
                print(k, v)
            print()

    print(f"Z2组总盈利:{all_profit}")


def _ensure_amt(df: pd.DataFrame) -> pd.Series:
    if "amt" in df.columns and pd.api.types.is_numeric_dtype(df["amt"]):
        return df["amt"].astype(float)
    return (df["price"].astype(float) * df["amount"].astype(float))

def max_consecutive(arr):
    max_cnt = cnt = 0
    for x in arr:
        if x:
            cnt += 1
            max_cnt = max(max_cnt, cnt)
        else:
            cnt = 0
    return max_cnt

def calc_trade_stats(df: pd.DataFrame, userid=None) -> dict:
    """
    df: 包含字段 userid, tsText, symbol, buy_sell, profit_loss, price, amount, amt, multiple, fee...
    userid: 只算某个用户可传入；不传则按 df 原样（建议先过滤）
    """
    d = df.copy()

    positions = {'long': deque(), 'short': deque()}
    round_trip_count = {'long': 0, 'short': 0}
    in_position = {'long': False, 'short': False}
    holding_records = []

    # 时间
    d["ts"] = pd.to_datetime(d["tsText"])
    d = d.sort_values("ts").reset_index(drop=True)

    for _, r in d.iterrows():
        ts, side, amt = r['ts'], r['buy_sell'], float(r['amount'])
        if side == '开多':
            if not in_position['long']:
                in_position['long'] = True   # 进入一次新交易
                positions['long'].append({'open_time': ts, 'amount': amt})

        elif side == '开空':
            if not in_position['short']:
                in_position['short'] = True
                positions['short'].append({'open_time': ts, 'amount': amt})

        elif side in ('平多', '平空'):
            key = 'long' if side == '平多' else 'short'
            remain = amt

            while remain > 0 and positions[key]:
                pos = positions[key][0]
                close_qty = min(remain, pos['amount'])

                holding_records.append({
                    'direction': key,
                    'open_time': pos['open_time'],
                    'close_time': ts,
                    'amount': close_qty,
                    'holding_seconds': (ts - pos['open_time']).total_seconds()
                })

                pos['amount'] -= close_qty
                remain -= close_qty
                if pos['amount'] == 0:
                    positions[key].popleft()    # 把当前方向中「最早开的那一笔仓位分片」从队列里移除
            
            if side == '平多':
                if not positions['long'] and in_position['long']:
                    round_trip_count['long'] += 1
                    in_position['long'] = False
            if side == '平空':
                if not positions['short'] and in_position['short']:
                    round_trip_count['short'] += 1
                    in_position['short'] = False
    holding_df = pd.DataFrame(holding_records)

    if userid is not None and "userid" in d.columns:
        d = d[d["userid"].astype(str) == str(userid)].copy()

    # 金额与手续费
    d["amt_"] = _ensure_amt(d).astype(float)
    if "fee" in d.columns:
        d["fee_"] = pd.to_numeric(d["fee"], errors="coerce").fillna(0.0)
    else:
        d["fee_"] = 0.0

    # 1) 基础统计
    trade_type_stats = dict(d['trade_type_stats'][0])
    trade_count = len(d)
    open_count = d['buy_sell'].isin(['开多','开空']).sum()
    close_count = d['buy_sell'].isin(['平多','平空']).sum()
    total_round_trips = (
        round_trip_count['long'] +
        round_trip_count['short']
    )
    pnl_df = d[d['profit_loss'] != 0]
    total_profit = round(pnl_df['profit_loss'].sum(), 2)
    total_fee = round(d['fee'].sum(), 2)
    max_profit = round(pnl_df['profit_loss'].max(), 2)
    max_loss = round(pnl_df['profit_loss'].min(), 2)
    win_rate = str(round((pnl_df['profit_loss'] > 0).mean()*100, 1)) + '%'
    profit_loss_ratio = (
        pnl_df[pnl_df['profit_loss'] > 0]['profit_loss'].mean()
        / abs(pnl_df[pnl_df['profit_loss'] < 0]['profit_loss'].mean())
    )
    profit_loss_ratio = round(profit_loss_ratio, 2)
    expect_profit = round(pnl_df['profit_loss'].mean(), 2)
    total_trade_amount = round(d['amt'].sum(), 2)
    profit_trade_ratio = str(round(total_profit / total_trade_amount * 100, 4)) + '%'
    taker_num = d['taker_num'].sum()
    maker_num = d['maker_num'].sum()
    taker_amt = round(d['taker_amt'].sum(), 2)
    maker_amt = round(d['maker_amt'].sum(), 2)

    # 3) 持仓时长（按 symbol 的仓位区间）
    holding_df['holding_minutes'] = holding_df['holding_seconds'] / 60
    weighted_avg_holding = (
        holding_df['holding_seconds'] * holding_df['amount']
    ).sum() / holding_df['amount'].sum()
    max_holding = holding_df['holding_seconds'].max()
    min_holding = holding_df['holding_seconds'].min()
    median_holding = holding_df['holding_minutes'].median()
    mode_holding = holding_df['holding_minutes'].round().mode()[0]
    # 将s秒转换为0 days 00:44:09这样的时间格式
    max_holding = str(pd.Timedelta(seconds=max_holding))
    min_holding = str(pd.Timedelta(seconds=min_holding))
    median_holding = str(pd.Timedelta(minutes=median_holding))
    mode_holding = str(pd.Timedelta(minutes=mode_holding))
    # 求mode_holding众数的次数及占比
    # 先对数据取整（和你 mode 计算时一致）
    rounded_series = holding_df['holding_minutes'].round()
    # 众数（确保存在）
    if not rounded_series.mode().empty:
        mode_val = rounded_series.mode()[0]
    else:
        raise ValueError("No mode found.")
    # 众数出现的次数
    mode_count = (rounded_series == mode_val).sum()
    # 占比（比例）
    mode_ratio = str(round(mode_count / total_round_trips * 100, 2)) + '%'
    # 计算分位值
    quantiles = holding_df['holding_minutes'].quantile([0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0])

    weighted_avg_holding = str(pd.Timedelta(seconds=weighted_avg_holding))
    profits = pnl_df['profit_loss']
    max_win_streak = max_consecutive(profits > 0)
    max_loss_streak = max_consecutive(profits < 0)
    # 计算持仓时间小于5min的交易轮次
    sholding_count = (holding_df['holding_seconds'] < 300)
    holding_round_trips = sholding_count.sum()
    holding_round_trips_ratio = str(round(holding_round_trips/total_round_trips*100, 2)) + '%'

    symbol_profit = (
        d[d['profit_loss'] != 0]
        .groupby('symbol')['profit_loss']
        .sum()
        .sort_values(ascending=False)
    )
    symbol_profit_info = "\n"
    for symbol, profit in symbol_profit.items():
        symbol_profit_info += f"{symbol}:{round(profit, 2)}\n"

    # 5) 平均杠杆倍数
    avg_leverage = round(d['multiple'].mean(), 2)

    # 资金曲线 & 最大回撤（按时间累计 pnl）
    returns = pnl_df['profit_loss']
    sharpe = round(returns.mean() / returns.std() * np.sqrt(len(returns)), 2)
    cum_profit = returns.cumsum()
    drawdown = cum_profit - cum_profit.cummax()
    max_drawdown = round(drawdown.min(), 2)
    
    d['date'] = d['ts'].dt.date
    d['week'] = d['ts'].dt.to_period('W')
    d['month'] = d['ts'].dt.to_period('M')

    daily_freq = round(d.groupby('date').size().mean(), 2)
    weekly_freq = round(d.groupby('week').size().mean(), 2)
    monthly_freq = round(d.groupby('month').size().mean(), 2)

    return {
        #'成交笔数(按订单算)': trade_count,
        #'开仓次数(按订单算)': open_count,
        #'平仓次数(按订单算)': close_count,
        '总交易轮次(开平算一次)': total_round_trips,
        '交易性质': trade_type_stats,
        # 'taker笔数': taker_num,
        #'taker金额': taker_amt,
        #'taker金额占比': str(round(taker_amt / (taker_amt + maker_amt) * 100, 2)) + '%',
        # 'maker笔数': maker_num,
        #'maker金额': maker_amt,
        #'maker金额占比': str(round(maker_amt / (taker_amt + maker_amt) * 100, 2)) + '%',
        '总盈亏': total_profit,
        #'刨除手续费总盈亏': total_profit - total_fee,
        '总手续费': total_fee,
        '手续费占比': str(round(total_fee / total_profit * 100, 2)) + '%',
        #'分币对盈亏': symbol_profit_info,
        #'最大盈利': max_profit,
        #'最大亏损': max_loss,
        '胜率': win_rate,
        '盈亏比': profit_loss_ratio,
        # '最大回撤': max_drawdown,
        #'平均杠杆': avg_leverage,
        '夏普比率': sharpe,
        #'期望收益': expect_profit,
        #'总成交金额': total_trade_amount,
        #'盈利/成交额': profit_trade_ratio,
        #'最大持仓时长': max_holding,
        #'最短持仓时长': min_holding,
        #'平均持仓时长': weighted_avg_holding,
        '中位数持仓时长': median_holding,
        #'众数持仓时长(聚合到min)': mode_holding,
        #'众数持仓时长(出现次数)': mode_count,
        #'众数持仓时长(占比)': mode_ratio,
        #'分位数持仓时长': quantiles,
        # '持仓时间小于5min的交易轮次': holding_round_trips,
        '持仓时间小于5min的交易轮次占比': holding_round_trips_ratio,
        #'最大连续盈利': max_win_streak,
        #'最大连续亏损': max_loss_streak,
        #'日均交易次数': daily_freq,
        #'周均交易次数': weekly_freq,
        #'月均交易次数': monthly_freq,
    }


loop = asyncio.get_event_loop()
df = loop.run_until_complete(do_count())




