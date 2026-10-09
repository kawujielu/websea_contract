# -*- coding: utf-8 -*-
"""近30天各交易对真实成交量(USDT)，排除做市/模拟金，按量降序。"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Set
from zoneinfo import ZoneInfo

import pandas as pd

# ========== 参数 ==========
_DIR = os.path.dirname(os.path.abspath(__file__))
PKL_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")
RECENT_DAYS = 30
CSV_PATH = os.path.join(_DIR, "data", "symbol_real_volume_30d.csv")  # 结果 CSV
PRINT_STDOUT = True
# ==========================

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


def day_symbol_vol(fp: str, excluded: Set[str]) -> Dict[str, float]:
    """单日：去重成一笔成交，任一侧非排除账户则计入一次名义金额。"""
    raw = pd.read_pickle(fp)
    if raw is None or len(raw) == 0:
        return {}
    df = ensure_user_id(raw)
    keys = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    u = df.drop_duplicates(keys)
    tk = u["taker_user"].astype(str)
    mk = u["maker_user"].astype(str)
    if excluded:
        u = u[(~tk.isin(excluded)) | (~mk.isin(excluded))]
    if u.empty:
        return {}
    pr = pd.to_numeric(u["price"], errors="coerce").fillna(0.0)
    amt = pd.to_numeric(u["amount"], errors="coerce").fillna(0.0)
    fv = pd.to_numeric(u["face_value"], errors="coerce").fillna(1.0) if "face_value" in u.columns else 1.0
    u = u.assign(_vol=pr * amt * fv)
    return u.groupby(u["symbol"].astype(str))["_vol"].sum().astype(float).to_dict()


def main() -> int:
    end_d = datetime.now(BJT).date()
    start_d = end_d - timedelta(days=RECENT_DAYS - 1)
    excluded = get_excluded_user_ids_sync()
    print("窗口 {} ~ {}  排除账户数={}  PKL={}".format(
        start_d, end_d, len(excluded), os.path.abspath(PKL_DIR)
    ))

    totals: Dict[str, float] = {}
    hit, miss = 0, []
    cur = start_d
    while cur <= end_d:
        fp = os.path.join(PKL_DIR, "{}.pkl".format(cur.isoformat()))
        if not os.path.isfile(fp):
            miss.append(cur.isoformat())
        else:
            hit += 1
            for sym, v in day_symbol_vol(fp, excluded).items():
                totals[sym] = totals.get(sym, 0.0) + v
        cur += timedelta(days=1)

    print("命中{}天 缺失{}天{}".format(
        hit, len(miss), (" 例:" + ",".join(miss[:5])) if miss else ""
    ))
    ranked: List[tuple] = sorted(totals.items(), key=lambda x: x[1], reverse=True)

    if PRINT_STDOUT:
        print("排名  交易对  成交量(USDT)")
        for i, (sym, vol) in enumerate(ranked, 1):
            print("{:<4}  {}  {:.2f}".format(i, sym, vol))

    out_df = pd.DataFrame(
        [{"排名": i, "交易对": sym, "成交量(USDT)": round(vol, 2)} for i, (sym, vol) in enumerate(ranked, 1)]
    )
    path = os.path.abspath(CSV_PATH)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    out_df.to_csv(path, index=False, encoding="utf-8-sig")
    print("结果已保存 CSV: {}".format(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

