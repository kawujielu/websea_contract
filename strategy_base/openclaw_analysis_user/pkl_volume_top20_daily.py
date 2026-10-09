# -*- coding: utf-8 -*-
"""每天北京时间 08:00：分析前 1/3/7 天交易量 Top20 用户。"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List

import pandas as pd
from zoneinfo import ZoneInfo

from exclude_user_ids import get_excluded_user_ids_sync
from mongdb_order_stats import calc_trade_stats, holding_records_dataframe
from pkl_user_query_analyzer import (
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
    expected_dates,
    load_window_df,
)

BJT = ZoneInfo("Asia/Shanghai")
DATA_DIR = "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl"
TOP_N = 20
WINDOWS = (1, 3, 7)


def _fmt_hold(sec: float) -> str:
    if sec != sec:  # NaN
        return "-"
    return str(pd.Timedelta(seconds=float(sec)))


def _user_row(uid: str, sub: pd.DataFrame) -> Dict:
    cdf = _pkl_legs_to_calc_trade_stats_df(sub, uid)
    st = calc_trade_stats(cdf, userid=uid)
    hold = holding_records_dataframe(
        cdf.assign(ts=pd.to_datetime(cdf["tsText"]))[["ts", "buy_sell", "amount"]]
    )
    if hold.shape[0]:
        secs = hold["holding_seconds"]
        h_max, h_min, h_med = float(secs.max()), float(secs.min()), float(secs.median())
        lt1m = "{:.2%}".format(float((secs < 60).mean()))
    else:
        h_max = h_min = h_med = float("nan")
        lt1m = "0%"

    symbols = ",".join(sorted(sub["symbol"].astype(str).unique().tolist()))
    return {
        "id": uid,
        "交易量": st.get("总成交金额", 0.0),
        "已实现盈亏": st.get("总盈亏", 0.0),
        "开平仓轮次": st.get("总交易轮次(开平算一次)", 0),
        "胜率": st.get("胜率", "0%"),
        "盈亏比": st.get("盈亏比", 0.0),
        "手续费": st.get("总手续费", 0.0),
        "手续费占比": st.get("手续费占比", "N/A"),
        "持仓时间max": _fmt_hold(h_max),
        "持仓时间min": _fmt_hold(h_min),
        "持仓时间中位数": _fmt_hold(h_med),
        "持仓<1min占比": lt1m,
        "交易对": symbols,
    }


def analyze_window(data_dir: str, days: int) -> pd.DataFrame:
    day_list = expected_dates(days)
    raw, exists, miss = load_window_df(data_dir, day_list)
    print("窗口近{}天 {}~{} 覆盖{}/{} 缺失:{}".format(
        days,
        day_list[0],
        day_list[-1],
        len(exists),
        len(day_list),
        ",".join(miss) if miss else "无",
    ))
    if raw.empty:
        return pd.DataFrame()

    df = ensure_user_id(raw)
    excluded = get_excluded_user_ids_sync()
    if excluded:
        df = df[~df["user_id"].astype(str).isin(excluded)]
    if df.empty:
        return pd.DataFrame()

    price = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    fv = pd.to_numeric(df.get("face_value", 1.0), errors="coerce").fillna(1.0)
    df = df.assign(_vol=price * amount * fv)
    top_ids: List[str] = (
        df.groupby("user_id")["_vol"].sum().sort_values(ascending=False).head(TOP_N).index.astype(str).tolist()
    )

    rows = []
    for uid in top_ids:
        sub = df[df["user_id"].astype(str) == uid]
        try:
            rows.append(_user_row(uid, sub))
        except Exception as e:
            rows.append({"id": uid, "交易量": float(sub["_vol"].sum()), "错误": str(e)})
    return pd.DataFrame(rows)


def run_once(data_dir: str) -> None:
    print("=" * 80)
    print("运行时间(北京): {}".format(datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")))
    for days in WINDOWS:
        print("-" * 80)
        out = analyze_window(data_dir, days)
        if out.empty:
            print("近{}天: 无数据".format(days))
            continue
        print("近{}天 交易量 Top{}".format(days, TOP_N))
        with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 60):
            print(out.to_string(index=False))


def main() -> int:
    p = argparse.ArgumentParser(description="每日08:00交易量Top20（1/3/7天）")
    p.add_argument("--data-dir", default=os.environ.get("UPM_PKL_DEALS_DIR", DATA_DIR))
    p.add_argument("--once", action="store_true", help="立即跑一轮后退出")
    args, _ = p.parse_known_args()

    if args.once:
        run_once(args.data_dir)
        return 0

    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger

    sched = BlockingScheduler(timezone=BJT)
    sched.add_job(run_once, CronTrigger(hour=8, minute=0, timezone=BJT), args=[args.data_dir], id="vol_top20")
    print("已调度: 每天北京时间 08:00 运行，data_dir={}".format(args.data_dir))
    run_once(args.data_dir)  # 启动时先跑一轮
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())

