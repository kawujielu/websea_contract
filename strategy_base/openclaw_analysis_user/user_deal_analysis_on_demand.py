from __future__ import annotations

import argparse
import asyncio
import os
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import motor.motor_asyncio
import pandas as pd
from pymongo.errors import OperationFailure

from mongdb_order_stats import calc_trade_stats
from exclude_user_ids import get_excluded_user_ids_async

BJT = ZoneInfo('Asia/Shanghai')
BUY_SELL_SIDE = {'1': '开多', '2': '开空', '3': '平多', '4': '平空'}

# 活动观察窗口：仅允许查询该区间内（按北京时间日历日）的指定日/时段
BJT_CAMPAIGN_MIN_DATE = date(2026, 4, 1)
BJT_CAMPAIGN_MAX_DATE = date(2026, 4, 10)

MONGO_URI = os.environ.get(
    'UPM_MONGO_URI',
    'mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/',  #'mongodb://root:GD0wBplOGwxQTaUI@10.0.96.71:27020/',
)
DETAIL_DEFAULT_LIMIT = int(os.environ.get('UPM_DETAIL_DEFAULT_LIMIT', '100'))


def bjt_start_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0)


def bjt_end_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59)


def filter_df_window(df: pd.DataFrame, start_d: Optional[date], end_d: date) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    ts = pd.to_datetime(df['tsText'])
    end_dt = pd.Timestamp(bjt_end_of_day(end_d))
    if start_d is None:
        return df[ts <= end_dt].copy()
    start_dt = pd.Timestamp(bjt_start_of_day(start_d))
    return df[(ts >= start_dt) & (ts <= end_dt)].copy()


async def mongo_orders_to_dataframe(collection, usr_id: str, ts_min: int, ts_max: int) -> pd.DataFrame:
    query = {
        'ts': {'$gte': ts_min, '$lte': ts_max},
        '$or': [
            {'takerUser': usr_id},
            {'makerUser': usr_id},
        ],
    }
    projection = {
        '_id': 0,
        'ts': 1,
        'symbol': 1,
        'price': 1,
        'amount': 1,
        'takerUser': 1,
        'makerUser': 1,
        'takerBuyOrSell': 1,
        'makerBuyOrSell': 1,
        'takerFee': 1,
        'makerFee': 1,
        'takerProfitLoss': 1,
        'makerProfitLoss': 1,
        'takerMultiple': 1,
        'makerMultiple': 1,
        'takerOrder': 1,
        'makerOrder': 1,
        'takerFaceValue': 1,
        'takerIsProtected': 1,
        'makerIsProtected': 1,
        'takerSubId': 1,
        'makerSubId': 1,
    }
    deals_by_order_id = {}
    try:
        cursor = collection.find(query, projection=projection).sort('ts', 1).batch_size(2000)
        try:
            cursor = cursor.allow_disk_use(True)
        except Exception:
            pass
    except TypeError:
        cursor = collection.find(query, projection=projection).sort('ts', 1).batch_size(2000)

    try:
        async for deal in cursor:
            ts_i = int(deal['ts'])
            dt_str = datetime.fromtimestamp(ts_i, tz=BJT).strftime('%Y-%m-%d %H:%M:%S')
            symbol = deal['symbol']
            taker = deal['takerUser']
            maker = deal['makerUser']
            is_t = str(taker) == str(usr_id)
            is_m = str(maker) == str(usr_id)
            if is_t:
                side_type = deal['takerBuyOrSell']
                fee = deal['takerFee']
                profit = deal['takerProfitLoss']
                multiple = deal['takerMultiple']
                order_id = deal['takerOrder']
                deal_is_protected = int(deal.get('takerIsProtected') or 0)
                deal_sub_id = int(deal.get('takerSubId') or 0)
            elif is_m:
                side_type = deal['makerBuyOrSell']
                fee = deal['makerFee']
                profit = deal['makerProfitLoss']
                multiple = deal['makerMultiple']
                order_id = deal['makerOrder']
                deal_is_protected = int(deal.get('makerIsProtected') or 0)
                deal_sub_id = int(deal.get('makerSubId') or 0)
            else:
                continue
            side = BUY_SELL_SIDE[str(side_type)]
            row = [
                dt_str, symbol, side, float(deal['price']), float(deal['amount']), profit, fee,
                taker, maker, int(multiple), float(deal['takerFaceValue']),
                deal_is_protected, deal_sub_id,
            ]
            deals_by_order_id.setdefault(order_id, []).append(row)
    except OperationFailure as e:
        if getattr(e, 'code', None) == 292 or 'Sort exceeded memory limit' in str(e):
            # 回退无排序读取，避免 Mongo 侧排序内存限制
            cursor2 = collection.find(query, projection=projection).batch_size(2000)
            async for deal in cursor2:
                ts_i = int(deal['ts'])
                dt_str = datetime.fromtimestamp(ts_i, tz=BJT).strftime('%Y-%m-%d %H:%M:%S')
                symbol = deal['symbol']
                taker = deal['takerUser']
                maker = deal['makerUser']
                is_t = str(taker) == str(usr_id)
                is_m = str(maker) == str(usr_id)
                if is_t:
                    side_type = deal['takerBuyOrSell']
                    fee = deal['takerFee']
                    profit = deal['takerProfitLoss']
                    multiple = deal['takerMultiple']
                    order_id = deal['takerOrder']
                    deal_is_protected = int(deal.get('takerIsProtected') or 0)
                    deal_sub_id = int(deal.get('takerSubId') or 0)
                elif is_m:
                    side_type = deal['makerBuyOrSell']
                    fee = deal['makerFee']
                    profit = deal['makerProfitLoss']
                    multiple = deal['makerMultiple']
                    order_id = deal['makerOrder']
                    deal_is_protected = int(deal.get('makerIsProtected') or 0)
                    deal_sub_id = int(deal.get('makerSubId') or 0)
                else:
                    continue
                side = BUY_SELL_SIDE[str(side_type)]
                row = [
                    dt_str, symbol, side, float(deal['price']), float(deal['amount']), profit, fee,
                    taker, maker, int(multiple), float(deal['takerFaceValue']),
                    deal_is_protected, deal_sub_id,
                ]
                deals_by_order_id.setdefault(order_id, []).append(row)
        else:
            raise

    rows = []
    for _, deals in deals_by_order_id.items():
        date_s = deals[0][0]
        symbol = deals[0][1]
        side = deals[0][2]
        multiple = deals[0][9]
        face_value = float(deals[0][10])
        amount = sum(float(i[4]) for i in deals) * face_value
        amt = sum(float(i[3]) * float(i[4]) for i in deals) * face_value
        taker_amt = sum(float(i[3]) * float(i[4]) for i in deals if str(i[7]) == str(usr_id)) * face_value
        maker_amt = sum(float(i[3]) * float(i[4]) for i in deals if str(i[8]) == str(usr_id)) * face_value
        price = round(amt / amount, 6) if amount else 0.0
        profit = sum(float(i[5]) for i in deals)
        fee = sum(float(i[6]) for i in deals)
        taker_num = len([1 for i in deals if str(i[7]) == str(usr_id)])
        maker_num = len([1 for i in deals if str(i[8]) == str(usr_id)])
        order_trade_stats = Counter()
        for leg in deals:
            dip, sid = leg[11], leg[12]
            if dip == 0 and sid == 0:
                order_trade_stats['自主交易'] += 1
            elif dip != 0 and sid != 0:
                order_trade_stats['跟单交易'] += 1
            else:
                order_trade_stats['其他'] += 1
        rows.append([
            usr_id, date_s, symbol, side, float(profit), float(price), float(amount), amt,
            int(multiple), fee, taker_num, maker_num, taker_amt, maker_amt, dict(order_trade_stats),
        ])

    if not rows:
        return pd.DataFrame(columns=[
            'userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'amt',
            'multiple', 'fee', 'taker_num', 'maker_num', 'taker_amt', 'maker_amt', 'trade_type_stats',
        ])
    return pd.DataFrame(rows, columns=[
        'userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'amt',
        'multiple', 'fee', 'taker_num', 'maker_num', 'taker_amt', 'maker_amt', 'trade_type_stats',
    ])


def format_stats_report(user_id: str, label: str, stats: dict) -> str:
    lines = ['用户 {} 交易分析（{}）'.format(user_id, label)]
    for k, v in stats.items():
        lines.append('{}: {}'.format(k, v))
    return '\n'.join(lines)


def build_detail_lines(df: pd.DataFrame, limit: int) -> str:
    if df.shape[0] == 0:
        return '无交易明细数据'

    dff = df.copy()
    dff['tsSort'] = pd.to_datetime(dff['tsText'], errors='coerce')
    dff = dff.sort_values(['tsSort'], ascending=False).drop(columns=['tsSort'])
    total_cnt = dff.shape[0]
    lim = int(limit)
    if lim <= 0:
        lim = total_cnt
    show_df = dff.head(lim)

    lines = []
    for _, r in show_df.iterrows():
        lines.append(
            '{} {} {} 价:{} 量:{} 盈亏:{} fee:{} taker次数:{} maker次数:{}'.format(
                r['tsText'],
                r['symbol'],
                r['buy_sell'],
                round(float(r['price']), 6),
                round(float(r['amount']), 4),
                round(float(r['profit_loss']), 2),
                round(float(r['fee']), 4),
                int(r['taker_num']),
                int(r['maker_num']),
            )
        )

    if total_cnt > lim:
        lines.append('')
        lines.append('本次共{}条，已截断。'.format(total_cnt))
        lines.append('如需全部展示，请在请求中增加“全部展示”')
    return '\n'.join(lines)


def parse_bjt_datetime(text: str) -> datetime:
    s = (text or '').strip()
    fmts = ['%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=BJT)
        except ValueError:
            continue
    raise ValueError('时间格式错误: {}，请使用 YYYY-MM-DD HH:MM 或 YYYY-MM-DD HH:MM:SS'.format(s))


def validate_bjt_campaign_datetimes(dt_start: datetime, dt_end: datetime) -> None:
    ds, de = dt_start.date(), dt_end.date()
    if ds < BJT_CAMPAIGN_MIN_DATE or ds > BJT_CAMPAIGN_MAX_DATE:
        raise ValueError(
            '开始日期须在 {} 至 {}（北京时间）'.format(
                BJT_CAMPAIGN_MIN_DATE.isoformat(),
                BJT_CAMPAIGN_MAX_DATE.isoformat(),
            )
        )
    if de < BJT_CAMPAIGN_MIN_DATE or de > BJT_CAMPAIGN_MAX_DATE:
        raise ValueError(
            '结束日期须在 {} 至 {}（北京时间）'.format(
                BJT_CAMPAIGN_MIN_DATE.isoformat(),
                BJT_CAMPAIGN_MAX_DATE.isoformat(),
            )
        )


async def run_single(
    user_id: str,
    mode: str,
    days: int,
    print_stdout: bool,
    detail: bool = False,
    detail_limit: int = DETAIL_DEFAULT_LIMIT,
    detail_start: Optional[str] = None,
    detail_end: Optional[str] = None,
    detail_hours: Optional[int] = None,
):
    excluded = await get_excluded_user_ids_async()
    if str(user_id) in excluded:
        msg = "该用户为做市账户或模拟金账户，不提供查询结果"
        if print_stdout:
            print(msg)
        return msg

    end_day = datetime.now(BJT).date()
    now_ts = int(datetime.now(BJT).timestamp())
    ts_max = now_ts
    mode = (mode or 'window').lower().strip()
    days = max(1, int(days))
    start_day = None  # type: Optional[date]
    label = ''
    abs_range = bool(detail_start and detail_end)

    if abs_range:
        dt_start = parse_bjt_datetime(detail_start)  # type: ignore[arg-type]
        dt_end = parse_bjt_datetime(detail_end)  # type: ignore[arg-type]
        validate_bjt_campaign_datetimes(dt_start, dt_end)
        if dt_end < dt_start:
            raise ValueError('时间范围非法：结束时间早于开始时间')
        ts_min = int(dt_start.timestamp())
        ts_max = int((dt_end + timedelta(minutes=1) - timedelta(seconds=1)).timestamp())
        label = '{} 到 {}'.format(
            dt_start.strftime('%Y-%m-%d %H:%M'),
            dt_end.strftime('%Y-%m-%d %H:%M'),
        )
    elif detail:
        if detail_hours is not None:
            h = max(1, int(detail_hours))
            ts_max = now_ts
            ts_min = int((datetime.now(BJT) - timedelta(hours=h)).timestamp())
            label = '近{}小时'.format(h)
        else:
            start_day = end_day - timedelta(days=days - 1)
            ts_min = int(datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT).timestamp())
            label = '近{}天'.format(days)
    else:
        if mode == 'all':
            # all 模式给一个较大但有限窗口，避免无下界查询
            start_day = end_day - timedelta(days=3650)
            label = '全部/全量'
        else:
            start_day = end_day - timedelta(days=days - 1)
            label = '近{}天'.format(days)
        ts_min = int(datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT).timestamp())

    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_contract_deal
    try:
        df = await mongo_orders_to_dataframe(col, str(user_id), ts_min, ts_max)
        if detail:
            text = '用户 {} 成交明细（{}）\n{}'.format(str(user_id), label, build_detail_lines(df, detail_limit))
        else:
            if abs_range:
                dff = df
            else:
                dff = filter_df_window(df, start_day, end_day) if df.shape[0] else df
            stats = calc_trade_stats(dff)
            text = format_stats_report(str(user_id), label, stats)
        if print_stdout:
            print(text)
        return text
    finally:
        client.close()


def parse_args():
    p = argparse.ArgumentParser(description='on-demand single user deal analysis')
    p.add_argument('--user_id', type=str, required=True)
    p.add_argument('--mode', type=str, default='window', help='window | all')
    p.add_argument('--days', type=int, default=3)
    p.add_argument('--detail', action='store_true', help='return deal details only')
    p.add_argument('--detail-limit', type=int, default=DETAIL_DEFAULT_LIMIT)
    p.add_argument('--detail-start', type=str, default=None, help='YYYY-MM-DD HH:MM')
    p.add_argument('--detail-end', type=str, default=None, help='YYYY-MM-DD HH:MM')
    p.add_argument('--detail-hours', type=int, default=None)
    p.add_argument('--print_stdout', action='store_true')
    args, _ = p.parse_known_args()
    return args


def main():
    args = parse_args()
    asyncio.run(
        run_single(
            args.user_id,
            args.mode,
            args.days,
            args.print_stdout,
            detail=args.detail,
            detail_limit=args.detail_limit,
            detail_start=args.detail_start,
            detail_end=args.detail_end,
            detail_hours=args.detail_hours,
        )
    )


if __name__ == '__main__':
    main()

