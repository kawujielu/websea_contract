# -*- coding: utf-8 -*-
"""统计 data 目录下日切 PKL 的每日/平均交易指标，及交易量 TopN 交易对。"""
from __future__ import annotations

import os
import re
from collections import defaultdict

import pandas as pd

from exclude_user_ids import get_excluded_user_ids_sync
from pkl_user_query_analyzer import ensure_user_id

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "mongo_daily_deals_pkl")
SPLIT_DATE = "2026-04-28"  # 前段 < 该日，后段 >= 该日
TOP_N = 15  # 总交易量排名前 N 的交易对
PKL_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.pkl$")


def load_day(fp: str) -> pd.DataFrame:
    d = ensure_user_id(pd.read_pickle(fp))
    excluded = get_excluded_user_ids_sync()
    if excluded:
        d = d[~d["user_id"].astype(str).isin(excluded)]
    price = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(d["amount"], errors="coerce").fillna(0.0)
    fv = pd.to_numeric(d.get("face_value", 1.0), errors="coerce").fillna(1.0)
    d = d.copy()
    d["notional"] = price * amount * fv
    return d


def day_stats(d: pd.DataFrame) -> dict:
    users = int(d["user_id"].nunique())
    trades = int(len(d))
    total = float(d["notional"].sum())
    return {
        "users": users,
        "trades": trades,
        "total_amt": total,
        "amt_per_user": total / users if users else 0.0,
        "amt_per_trade": total / trades if trades else 0.0,
    }


def avg_stats(rows: list) -> dict | None:
    if not rows:
        return None
    n = len(rows)
    return {k: sum(r[k] for r in rows) / n for k in rows[0]}


def print_avg(label: str, avg: dict | None, n: int):
    if avg is None:
        print("{}: 无数据".format(label))
        return
    print("{:<20} {:>10.2f} {:>12.2f} {:>16.2f} {:>16.2f} {:>16.2f}  ({}天)".format(
        label, avg["users"], avg["trades"], avg["total_amt"], avg["amt_per_user"], avg["amt_per_trade"], n
    ))


def main():
    files = sorted(
        (m.group(1), os.path.join(DATA_DIR, name))
        for name in os.listdir(DATA_DIR)
        if (m := PKL_RE.match(name))
    )
    if not files:
        print("无可用 pkl 文件: {}".format(DATA_DIR))
        return

    before, after = [], []
    sym_vol = defaultdict(float)
    print("{:<12} {:>10} {:>12} {:>16} {:>16} {:>16}".format(
        "日期", "交易人数", "交易笔数", "日交易总金额", "平均金额/人", "平均金额/笔"
    ))
    for day, fp in files:
        d = load_day(fp)
        s = day_stats(d)
        (before if day < SPLIT_DATE else after).append(s)
        for sym, v in d.groupby("symbol")["notional"].sum().items():
            sym_vol[sym] += float(v)
        print("{:<12} {:>10} {:>12} {:>16.2f} {:>16.2f} {:>16.2f}".format(
            day, s["users"], s["trades"], s["total_amt"], s["amt_per_user"], s["amt_per_trade"]
        ))

    print("-" * 100)
    print("{:<20} {:>10} {:>12} {:>16} {:>16} {:>16}".format(
        "分段日均", "交易人数", "交易笔数", "日交易总金额", "平均金额/人", "平均金额/笔"
    ))
    print_avg("{}前".format(SPLIT_DATE), avg_stats(before), len(before))
    print_avg("{}起".format(SPLIT_DATE), avg_stats(after), len(after))

    top = sorted(sym_vol.items(), key=lambda x: x[1], reverse=True)[:TOP_N]
    print("-" * 100)
    print("总交易量 Top{} 交易对".format(TOP_N))
    print("{:<6} {:<20} {:>18}".format("排名", "交易对", "总交易量"))
    for i, (sym, vol) in enumerate(top, 1):
        print("{:<6} {:<20} {:>18.2f}".format(i, sym, vol))


if __name__ == "__main__":
    main()

