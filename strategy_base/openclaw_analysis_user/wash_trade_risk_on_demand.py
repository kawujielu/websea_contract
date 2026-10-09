from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import motor.motor_asyncio

from mongdb_order_stats import calc_trade_stats
from user_deal_analysis_on_demand import (
    filter_df_window,
    mongo_orders_to_dataframe,
    MONGO_URI,
    parse_bjt_datetime,
    validate_bjt_campaign_datetimes,
)
from exclude_user_ids import get_excluded_user_ids_async

BJT = ZoneInfo('Asia/Shanghai')


def _holding_text_to_seconds(text: str) -> float:
    s = str(text or '').strip()
    if not s:
        return 0.0
    # 典型格式: "0 days 00:08:38"
    try:
        if 'days' in s:
            left, right = s.split('days', 1)
            days = float(left.strip())
            hh, mm, ss = right.strip().split(':')
            return days * 86400.0 + float(hh) * 3600.0 + float(mm) * 60.0 + float(ss)
    except Exception:
        pass
    try:
        return float(s)
    except Exception:
        return 0.0


def _percent_to_float(text: str) -> float:
    s = str(text or '').strip().replace('%', '')
    try:
        return float(s)
    except Exception:
        return 0.0


def build_wash_report(user_id: str, label: str, stats: dict) -> str:
    median_txt = stats.get('中位数持仓时长', '0 days 00:00:00')
    ratio_txt = stats.get('持仓时间小于5min的交易轮次占比', '0.0%')
    trips = int(float(stats.get('总交易轮次(开平算一次)', 0)))

    cond1 = _holding_text_to_seconds(median_txt) < 300.0
    cond2 = _percent_to_float(ratio_txt) > 50.0
    cond3 = trips > 20

    conclusion = '大概率是刷单用户' if (cond1 and cond2 and cond3) else '无法确认，需要人工排查'

    lines = [
        # '刷量排查结果：',
        '用户 {} 刷量排查（{}）'.format(user_id, label),
        '总盈亏: {} 刨除手续费总盈亏: {} 总手续费: {} 手续费占比: {}'.format(
            stats.get('总盈亏', 0.0),
            stats.get('刨除手续费总盈亏', 0.0),
            stats.get('总手续费', stats.get('手续费总和', 0.0)),
            stats.get('手续费占比', 'N/A'),
        ),
        '中位数持仓时长: {} (<5min): {}'.format(median_txt, '是' if cond1 else '否'),
        '持仓<5min占比: {} (>50%): {}'.format(ratio_txt, '是' if cond2 else '否'),
        '总交易轮次(开平算一次): {} (>20): {}'.format(trips, '是' if cond3 else '否'),
        '结论: {}'.format(conclusion),
    ]
    return '\n'.join(lines)


async def run_single(
    user_id: str,
    mode: str,
    days: int,
    print_stdout: bool,
    detail_start: Optional[str] = None,
    detail_end: Optional[str] = None,
):
    excluded = await get_excluded_user_ids_async()
    if str(user_id) in excluded:
        msg = "该用户为做市账户或模拟金账户，不提供查询结果"
        if print_stdout:
            print(msg)
        return msg

    end_day = datetime.now(BJT).date()
    ts_max = int(datetime.now(BJT).timestamp())
    mode = (mode or 'window').lower().strip()
    days = max(1, int(days))
    start_day = None  # type: Optional[date]
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
    elif mode == 'all':
        start_day = end_day - timedelta(days=3650)
        label = '全部/全量'
        ts_min = int(datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT).timestamp())
    else:
        start_day = end_day - timedelta(days=days - 1)
        label = '近{}天'.format(days)
        ts_min = int(datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT).timestamp())

    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_contract_deal
    try:
        df = await mongo_orders_to_dataframe(col, str(user_id), ts_min, ts_max)
        if abs_range:
            dff = df
        else:
            dff = filter_df_window(df, start_day, end_day) if df.shape[0] else df
        stats = calc_trade_stats(dff)
        text = build_wash_report(str(user_id), label, stats)
        if print_stdout:
            print(text)
        return text
    finally:
        client.close()


def parse_args():
    p = argparse.ArgumentParser(description='wash trade risk on-demand')
    p.add_argument('--user_id', type=str, required=True)
    p.add_argument('--mode', type=str, default='window', help='window | all')
    p.add_argument('--days', type=int, default=3)
    p.add_argument('--print_stdout', action='store_true')
    p.add_argument('--detail-start', type=str, default=None, help='活动窗内指定时段开始 YYYY-MM-DD HH:MM')
    p.add_argument('--detail-end', type=str, default=None, help='活动窗内指定时段结束 YYYY-MM-DD HH:MM')
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
            detail_start=args.detail_start,
            detail_end=args.detail_end,
        )
    )


if __name__ == '__main__':
    main()

