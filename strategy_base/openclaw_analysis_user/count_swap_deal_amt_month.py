# -*- coding: utf-8 -*-
"""指定时间范围：按交易对统计真实用户买入/卖出金额，排除做市商与模拟金。

口径：
- 日切 PKL（mongo_daily_deals_dump）按日加载
- 同一成交去重为一笔（ts_text+symbol+price+amount+taker+maker）
- 任一侧非排除账户则计入；方向取 taker 的 buy_sell
- 名义金额 = price * amount * face_value（USDT）

运行: python pkl_symbol_buy_sell_volume.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

# ========== 参数（全部写在脚本内）==========
_DIR = os.path.dirname(os.path.abspath(__file__))
PKL_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")
# 闭区间 [START_DATE, END_DATE]，北京时间日切
START_DATE = date(2026, 9, 1)
END_DATE = date(2026, 9, 14)
CSV_PATH = os.path.join(_DIR, "data", "pkl_symbol_buy_sell_volume.csv")
PRINT_STDOUT = True
# 主动买/卖（taker 方向）：开多/平空=买，开空/平多=卖
BUY_SIDES = {"开多", "平空", "1", "4", "buy", "BUY"}
SELL_SIDES = {"开空", "平多", "2", "3", "sell", "SELL"}
# ==========================================

BJT = ZoneInfo("Asia/Shanghai")
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402


def ensure_user_id(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d
    grp = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    d = d.reset_index(drop=True)
    d["_i"] = d.groupby(grp).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    d.loc[d["_i"] % 2 == 1, "user_id"] = d.loc[d["_i"] % 2 == 1, "maker_user"].astype(str)
    return d.drop(columns=["_i"])


def day_symbol_buy_sell(fp: str, excluded: Set[str]) -> Dict[str, Tuple[float, float]]:
    """单日：symbol -> (buy_usdt, sell_usdt)。"""
    raw = pd.read_pickle(fp)
    if raw is None or len(raw) == 0:
        return {}
    df = ensure_user_id(raw)
    keys = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    u = df.drop_duplicates(keys)
    tk = u["taker_user"].astype(str)
    mk = u["maker_user"].astype(str)
    if excluded:
        # 两侧都是做市/模拟金则剔除
        u = u[(~tk.isin(excluded)) | (~mk.isin(excluded))]
        tk = u["taker_user"].astype(str)
    if u.empty:
        return {}

    pr = pd.to_numeric(u["price"], errors="coerce").fillna(0.0)
    amt = pd.to_numeric(u["amount"], errors="coerce").fillna(0.0)
    fv = (
        pd.to_numeric(u["face_value"], errors="coerce").fillna(1.0)
        if "face_value" in u.columns
        else 1.0
    )
    vol = pr * amt * fv
    side = u["buy_sell"].astype(str)
    buy = vol.where(side.isin(BUY_SIDES), 0.0)
    sell = vol.where(side.isin(SELL_SIDES), 0.0)
    g = pd.DataFrame({"symbol": u["symbol"].astype(str), "buy": buy, "sell": sell})
    agg = g.groupby("symbol", sort=False).agg(buy=("buy", "sum"), sell=("sell", "sum"))
    return {str(sym): (float(r.buy), float(r.sell)) for sym, r in agg.iterrows()}


def iter_dates(start_d: date, end_d: date):
    cur = start_d
    while cur <= end_d:
        yield cur
        cur += timedelta(days=1)


def main() -> int:
    if END_DATE < START_DATE:
        raise ValueError("END_DATE 不能早于 START_DATE")

    excluded = get_excluded_user_ids_sync()
    print(
        "窗口 {} ~ {}  排除账户数={}  PKL={}".format(
            START_DATE, END_DATE, len(excluded), os.path.abspath(PKL_DIR)
        )
    )

    totals: Dict[str, List[float]] = {}  # sym -> [buy, sell]
    hit, miss = 0, []
    for d in iter_dates(START_DATE, END_DATE):
        fp = os.path.join(PKL_DIR, "{}.pkl".format(d.isoformat()))
        if not os.path.isfile(fp):
            miss.append(d.isoformat())
            continue
        hit += 1
        for sym, (b, s) in day_symbol_buy_sell(fp, excluded).items():
            if sym not in totals:
                totals[sym] = [0.0, 0.0]
            totals[sym][0] += b
            totals[sym][1] += s

    print(
        "命中{}天 缺失{}天{}".format(
            hit, len(miss), (" 例:" + ",".join(miss[:5])) if miss else ""
        )
    )

    rows = []
    for sym, (buy, sell) in totals.items():
        vol = buy + sell
        rows.append(
            {
                "交易对": sym,
                "买入金额(USDT)": round(buy, 2),
                "卖出金额(USDT)": round(sell, 2),
                "成交量(USDT)": round(vol, 2),
            }
        )
    rows.sort(key=lambda x: x["成交量(USDT)"], reverse=True)
    for i, r in enumerate(rows, 1):
        r["排名"] = i

    out_df = pd.DataFrame(
        rows,
        columns=["排名", "交易对", "买入金额(USDT)", "卖出金额(USDT)", "成交量(USDT)"],
    )

    total_buy = float(out_df["买入金额(USDT)"].sum()) if len(out_df) else 0.0
    total_sell = float(out_df["卖出金额(USDT)"].sum()) if len(out_df) else 0.0
    total_vol = float(out_df["成交量(USDT)"].sum()) if len(out_df) else 0.0

    if PRINT_STDOUT:
        print("排名  交易对  买入  卖出  成交量")
        for _, r in out_df.iterrows():
            print(
                "{:<4}  {}  {:.2f}  {:.2f}  {:.2f}".format(
                    int(r["排名"]),
                    r["交易对"],
                    r["买入金额(USDT)"],
                    r["卖出金额(USDT)"],
                    r["成交量(USDT)"],
                )
            )
        print("---------- 合计 ----------")
        print(
            "总买入金额={:.2f}  总卖出金额={:.2f}  总成交量={:.2f}".format(
                total_buy, total_sell, total_vol
            )
        )

    path = os.path.abspath(CSV_PATH)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    # CSV 末尾追加合计行
    summary = pd.DataFrame(
        [
            {
                "排名": "",
                "交易对": "合计",
                "买入金额(USDT)": round(total_buy, 2),
                "卖出金额(USDT)": round(total_sell, 2),
                "成交量(USDT)": round(total_vol, 2),
            }
        ]
    )
    pd.concat([out_df, summary], ignore_index=True).to_csv(
        path, index=False, encoding="utf-8-sig"
    )
    print("结果已保存 CSV: {}".format(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

