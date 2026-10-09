from __future__ import annotations

import argparse
import asyncio
import os
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import motor.motor_asyncio
import pandas as pd

from exclude_user_ids import get_excluded_user_ids_sync
from mongdb_order_stats import calc_trade_stats
from pkl_user_query_analyzer import DEFAULT_DATA_DIR, ensure_user_id, load_window_df
from user_deal_analysis_on_demand import mongo_orders_to_dataframe

from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

MONGO_URI = os.environ.get(
    "UPM_MONGO_URI",
    "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/",
)

MAX_TOP = int(os.environ.get("UPM_MONTHLY_PROFIT_TOP_MAX", "50"))
PKL_DATA_DIR = os.environ.get(
    "UPM_DAILY_PKL_DIR",
    DEFAULT_DATA_DIR,
)


def parse_args():
    p = argparse.ArgumentParser(
        description="自然月内（北京时间）按 PKL 汇总盈亏并扣除手续费后排名，取 TopN 用户并输出全量交易统计"
    )
    p.add_argument("--month", type=int, default=None, help="月份 1-12")
    p.add_argument(
        "--year",
        type=int,
        default=None,
        help="年份，默认当前北京时间年份",
    )
    p.add_argument(
        "--rolling-unit",
        choices=["week", "month"],
        default=None,
        help="滚动窗口单位：week/month（例如上3周、上2月）",
    )
    p.add_argument("--rolling-n", type=int, default=None, help="滚动窗口数量 N")
    p.add_argument("--top", type=int, default=10, help="取前 N 名，默认 10")
    p.add_argument(
        "--simple-output",
        action="store_true",
        help="仅输出 Top 用户ID与盈利金额（适用于群聊简报）",
    )
    p.add_argument("--print_stdout", action="store_true")
    args, _ = p.parse_known_args()
    return args


def bjt_month_bounds(year: int, month: int) -> Tuple[int, int]:
    """该自然月 [月初 00:00:00, 月末 23:59:59] 北京时间，闭区间时间戳。"""
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=BJT)
    last_day = monthrange(year, month)[1]
    end = datetime(year, month, last_day, 23, 59, 59, tzinfo=BJT)
    return int(start.timestamp()), int(end.timestamp())


def month_days_for_pkl(year: int, month: int) -> List[date]:
    """返回该月内已结束的自然日（不含今天），用于从 PKL 中加载。"""
    today = datetime.now(BJT).date()
    first = date(year, month, 1)
    last_day = monthrange(year, month)[1]
    last = date(year, month, last_day)
    end = min(last, today - datetime.resolution) if today > first else last  # type: ignore
    # 简化处理：仅包含 < 今天 的该月日期
    end = min(last, today)  # today 会在下方过滤掉
    days: List[date] = []
    cur = first
    while cur <= end:
        if cur < today:
            days.append(cur)
        cur = cur.fromordinal(cur.toordinal() + 1)
    return days


def rolling_days_for_pkl(unit: str, n: int) -> List[date]:
    """返回上N周/月对应的自然日（不含今天），用于 PKL 日切加载。"""
    today = datetime.now(BJT).date()
    end = today - timedelta(days=1)
    if end < date(1970, 1, 1):
        return []
    span_days = 7 * int(n) if unit == "week" else 30 * int(n)
    span_days = max(1, int(span_days))
    start = end - timedelta(days=span_days - 1)
    days: List[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur = cur + timedelta(days=1)
    return days


def format_user_block(
    uid: str,
    month_net_profit: float,
    month_fee_total: float,
    year: int,
    month: int,
    stats: Dict,
) -> str:
    line1 = "user_id={} 月盈利(U): {:.4f} （{}-{:02d} 北京时间）（已扣除{:.4f}手续费）".format(
        uid, float(month_net_profit), year, month, float(month_fee_total)
    )
    line2 = (
        "全部交易 总盈亏:{} 刨除手续费总盈亏:{} 总手续费:{} 手续费占比:{} "
        "成交笔数(按订单算):{} 交易性质:{} "
        "胜率:{} 盈亏比:{} 开平仓轮次:{} 持仓时间中位数:{}"
    ).format(
        stats.get("总盈亏", 0.0),
        stats.get("刨除手续费总盈亏", 0.0),
        stats.get("总手续费", stats.get("手续费总和", 0.0)),
        stats.get("手续费占比", "N/A"),
        stats.get("成交笔数(按订单算)", 0),
        stats.get("交易性质", {}),
        stats.get("胜率", "0%"),
        stats.get("盈亏比", 0.0),
        stats.get("总交易轮次(开平算一次)", 0),
        stats.get("中位数持仓时长", "0 days 00:00:00"),
    )
    return line1 + "\n" + line2


async def run_async(month: int, year: int, top: int) -> str:
    ts_min, ts_max = bjt_month_bounds(year, month)
    ts_now = int(datetime.now(BJT).timestamp())

    # 1) 从 PKL 日切文件中按自然日汇总该月的用户盈亏
    days = month_days_for_pkl(year, month)
    if not days:
        return "无数据：{}-{:02d} 当月无已结束自然日的 PKL 文件".format(year, month)
    df_raw, exists, miss = load_window_df(PKL_DATA_DIR, days)
    if df_raw.shape[0] == 0:
        return "无数据：{}-{:02d} 对应 PKL 文件全部缺失或为空".format(year, month)

    df_u = ensure_user_id(df_raw)
    # 严格按月份再过滤一次（避免跨月数据误入）
    # df_u["ts"] = pd.to_datetime(df_u["ts_text"], errors="coerce")
    # d0 = datetime.fromtimestamp(ts_min, tz=BJT)
    # d1 = datetime.fromtimestamp(ts_max, tz=BJT)
    # df_u = df_u[(df_u["ts"] >= d0) & (df_u["ts"] <= d1)].copy()
    # 先转成日期字符串（不带时区，只看自然日）
    df_u["ts"] = pd.to_datetime(df_u["ts_text"], errors="coerce")
    df_u["day"] = df_u["ts"].dt.strftime("%Y-%m-%d")
    month_prefix = f"{year:04d}-{month:02d}-"  # 比如 "2026-03-"
    df_u = df_u[df_u["day"].str.startswith(month_prefix)].copy()
    if df_u.shape[0] == 0:
        return "无数据：{}-{:02d} PKL 中无该月落在窗口内的成交记录".format(year, month)

    df_u["profit_loss"] = pd.to_numeric(df_u["profit_loss"], errors="coerce").fillna(0.0)
    if "fee" not in df_u.columns:
        df_u["fee"] = 0.0
    else:
        df_u["fee"] = pd.to_numeric(df_u["fee"], errors="coerce").fillna(0.0)

    # 保持模拟金过滤开启：get_excluded_user_ids_sync 会合并 account.config、环境变量与（可用时）ToolBox 远程模拟金列表。
    excluded = get_excluded_user_ids_sync()
    if excluded:
        df_u = df_u[~df_u["user_id"].astype(str).isin(excluded)].copy()

    gp = df_u.groupby("user_id", as_index=False).agg(
        gross_pnl=("profit_loss", "sum"),
        month_fee=("fee", "sum"),
    )
    gp["net_pnl"] = gp["gross_pnl"].astype(float) - gp["month_fee"].astype(float)
    if gp.shape[0] == 0:
        return "无数据：{}-{:02d} 过滤做市/模拟金后无有效用户".format(year, month)

    top_n = max(1, min(int(top), MAX_TOP))
    gp = gp.sort_values("net_pnl", ascending=False).head(top_n)
    top_rows = [
        (str(r["user_id"]), float(r["net_pnl"]), float(r["month_fee"]))
        for _, r in gp.iterrows()
    ]
    total_top_net = sum(x[1] for x in top_rows)
    n_on_list = len(top_rows)

    lines = [
        "自然月盈利 Top{}（按 PKL 日切文件汇总该月平仓盈亏，已排除做市/模拟金等；月盈利已扣除手续费） 本榜 {} 人月净盈利合计: {:.4f} U".format(
            top_n, n_on_list, total_top_net
        ),
        "统计月：{}-{:02d}（北京时间） | 全量指标截止：当前时刻".format(year, month),
        "",
    ]

    client2 = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col2 = client2.exchange.real_contract_deal
    try:
        for uid, net_pnl, month_fee in top_rows:
            try:
                df = await mongo_orders_to_dataframe(col2, str(uid), 0, ts_now)
                stats = calc_trade_stats(df, userid=str(uid))
                lines.append(
                    format_user_block(
                        str(uid), float(net_pnl), float(month_fee), year, month, stats
                    )
                )
                lines.append("")
            except Exception as e:
                lines.append(
                    "user_id={} 月盈利(U): {:.4f} （{}-{:02d} 北京时间）（已扣除{:.4f}手续费）\n全量统计失败: {}".format(
                        uid, float(net_pnl), year, month, float(month_fee), e
                    )
                )
                lines.append("")
    finally:
        client2.close()

    return "\n".join(lines).rstrip() + "\n"


async def run_rolling_async(unit: str, n: int, top: int, simple_output: bool) -> str:
    days = rolling_days_for_pkl(unit, n)
    if not days:
        return "无数据：滚动窗口内无可用自然日"
    df_raw, exists, miss = load_window_df(PKL_DATA_DIR, days)
    if df_raw.shape[0] == 0:
        return "无数据：窗口内 PKL 文件全部缺失或为空"

    df_u = ensure_user_id(df_raw)
    df_u["profit_loss"] = pd.to_numeric(df_u["profit_loss"], errors="coerce").fillna(0.0)
    if "fee" not in df_u.columns:
        df_u["fee"] = 0.0
    else:
        df_u["fee"] = pd.to_numeric(df_u["fee"], errors="coerce").fillna(0.0)

    # 保持模拟金过滤开启：get_excluded_user_ids_sync 会合并 account.config、环境变量与（可用时）ToolBox 远程模拟金列表。
    excluded = get_excluded_user_ids_sync()
    if excluded:
        df_u = df_u[~df_u["user_id"].astype(str).isin(excluded)].copy()
    if df_u.shape[0] == 0:
        return "无数据：过滤做市/模拟金后无有效用户"

    gp = df_u.groupby("user_id", as_index=False).agg(
        gross_pnl=("profit_loss", "sum"),
        total_fee=("fee", "sum"),
    )
    gp["net_pnl"] = gp["gross_pnl"].astype(float) - gp["total_fee"].astype(float)
    if gp.shape[0] == 0:
        return "无数据：窗口内无有效用户盈亏记录"

    top_n = max(1, min(int(top), MAX_TOP))
    gp = gp.sort_values("net_pnl", ascending=False).head(top_n).reset_index(drop=True)
    win_label = "上{}周".format(int(n)) if unit == "week" else "上{}月".format(int(n))
    total_top_net = float(gp["net_pnl"].sum())
    start_day = days[0].isoformat()
    end_day = days[-1].isoformat()

    lines = [
        "{}盈利Top{}（按 PKL 汇总，已扣手续费，排除做市/模拟金）".format(win_label, len(gp)),
        "窗口：{} ~ {}（北京时间自然日，不含今天）".format(start_day, end_day),
        "Top合计净盈利: {:.4f} U".format(total_top_net),
        "",
    ]
    if simple_output:
        for i, row in gp.iterrows():
            lines.append(
                "{:02d}. user_id={} 盈利金额(U)={:.4f}".format(
                    i + 1, str(row["user_id"]), float(row["net_pnl"])
                )
            )
        return "\n".join(lines).rstrip() + "\n"

    ts_now = int(datetime.now(BJT).timestamp())
    client2 = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col2 = client2.exchange.real_contract_deal
    try:
        for _, r in gp.iterrows():
            uid = str(r["user_id"])
            net_pnl = float(r["net_pnl"])
            total_fee = float(r["total_fee"])
            try:
                df = await mongo_orders_to_dataframe(col2, uid, 0, ts_now)
                stats = calc_trade_stats(df, userid=uid)
                line1 = "user_id={} 窗口盈利(U): {:.4f}（已扣手续费{:.4f}）".format(
                    uid, net_pnl, total_fee
                )
                line2 = (
                    "全部交易 总盈亏:{} 刨除手续费总盈亏:{} 总手续费:{} 手续费占比:{} "
                    "成交笔数(按订单算):{} 交易性质:{} "
                    "胜率:{} 盈亏比:{} 开平仓轮次:{} 持仓时间中位数:{}"
                ).format(
                    stats.get("总盈亏", 0.0),
                    stats.get("刨除手续费总盈亏", 0.0),
                    stats.get("总手续费", stats.get("手续费总和", 0.0)),
                    stats.get("手续费占比", "N/A"),
                    stats.get("成交笔数(按订单算)", 0),
                    stats.get("交易性质", {}),
                    stats.get("胜率", "0%"),
                    stats.get("盈亏比", 0.0),
                    stats.get("总交易轮次(开平算一次)", 0),
                    stats.get("中位数持仓时长", "0 days 00:00:00"),
                )
                lines.append(line1)
                lines.append(line2)
                lines.append("")
            except Exception as e:
                lines.append(
                    "user_id={} 窗口盈利(U): {:.4f}\n全量统计失败: {}".format(uid, net_pnl, e)
                )
                lines.append("")
    finally:
        client2.close()

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    top = int(args.top)
    now_bjt = datetime.now(BJT)

    if args.rolling_unit:
        if args.month is not None:
            sys.stderr.write("rolling 查询不应同时传 --month\n")
            return 2
        if args.rolling_n is None or int(args.rolling_n) <= 0:
            sys.stderr.write("rolling 查询需传正整数 --rolling-n\n")
            return 2
        unit = str(args.rolling_unit)
        n = int(args.rolling_n)
        text = asyncio.run(run_rolling_async(unit, n, top, bool(args.simple_output)))
    else:
        if args.month is None:
            sys.stderr.write("请传 --month，或使用 --rolling-unit/--rolling-n\n")
            return 2
        month = int(args.month)
        if month < 1 or month > 12:
            sys.stderr.write("month 须在 1-12 之间\n")
            return 2
        year = int(args.year) if args.year is not None else now_bjt.year
        text = asyncio.run(run_async(month, year, top))
    if args.print_stdout:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
