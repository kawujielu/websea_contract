#!/usr/bin/env python3
"""
功能：读取 filter/ 下最新筛选结果，打分后写入 score/{日期}_address_scores.parquet。

用法：
    cd /home/ubuntu/strategy_base/strategy/AB/HYPER
    python pipeline/score_address_deals.py
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hyper_config import DATA_DIR

FILTER_DIR = DATA_DIR / "filter"
SCORE_DIR = DATA_DIR / "score"
TRACKED_COINS = {"BTC", "ETH", "SOL", "HYPE"}
SCORE_GAOSHOU = 1.5
SCORE_XINSHOU = -1.5


def classify_score(score):
    if score > SCORE_GAOSHOU:
        return "高手"
    if score < SCORE_XINSHOU:
        return "新手"
    return "其他"


def load_filter_file():
    """取 filter 目录下最新日期的筛选结果。"""
    files = sorted(FILTER_DIR.glob("*_filter_address.parquet"))
    if not files:
        raise FileNotFoundError(f"未找到筛选文件: {FILTER_DIR}/*_filter_address.parquet")
    return files[-1]


def load_addresses():
    path = load_filter_file()
    print(f"读取筛选地址: {path.name}")
    return pd.read_parquet(path)["address"].drop_duplicates().tolist()


def load_deals_files():
    deals = {}
    for coin in sorted(TRACKED_COINS):
        path = DATA_DIR / f"{coin}_deals.parquet"
        if path.exists():
            deals[coin] = pd.read_parquet(path)
    return deals


def deals_to_fills(df, coin):
    df = df.sort_values("time")
    return pd.DataFrame({
        "coin": coin,
        "dir": df["dir"].astype(str),
        "closed_pnl": pd.to_numeric(df["closedPnl"], errors="coerce").fillna(0.0),
        "date": df["time"],
    })


def score_coin_verbose(df_coin):
    coin = str(df_coin["coin"].iloc[0]) if len(df_coin) else ""
    df = df_coin[df_coin["dir"].str.contains("Long|Short", case=False, na=False)].copy()
    df = df.sort_values("date").reset_index(drop=True)
    n = len(df)
    if n == 0:
        return None
    df["cum_pnl"] = df["closed_pnl"].cumsum()
    x = np.arange(1, n + 1).reshape(-1, 1)
    y = df["cum_pnl"].values
    model = LinearRegression().fit(x, y)
    r_sq = float(model.score(x, y)) if n > 1 else 0.0
    mean_pnl = float(df["closed_pnl"].mean())
    std_pnl = float(df["closed_pnl"].std(ddof=1)) if n > 1 else 0.0
    score = float(r_sq * (mean_pnl / std_pnl) * np.sqrt(n)) if std_pnl > 0 and not np.isnan(std_pnl) else 0.0
    return {
        "coin": coin, "N": n, "r_sq": r_sq, "mean_pnl": mean_pnl,
        "std_pnl": std_pnl if not np.isnan(std_pnl) else 0.0,
        "score": score, "total_pnl": float(df["closed_pnl"].sum()), "status": classify_score(score),
    }


def score_address(addr, deals_map):
    rows, all_fills = [], []
    addr = addr.lower()
    for coin, df in deals_map.items():
        sub = df[df["address"] == addr]
        if sub.empty:
            continue
        fills = deals_to_fills(sub, coin)
        all_fills.append(fills)
        row = score_coin_verbose(fills)
        if row:
            row["address"] = addr
            rows.append(row)
    if all_fills:
        combined = pd.concat(all_fills, ignore_index=True).sort_values("date")
        combined["coin"] = "ALL"
        row = score_coin_verbose(combined)
        if row:
            row["address"] = addr
            rows.append(row)
    return rows


def main():
    addresses = load_addresses()
    deals_map = load_deals_files()
    if not deals_map:
        raise FileNotFoundError("未找到 *_deals.parquet")
    print(f"地址 {len(addresses)} 个，交易对 {list(deals_map.keys())}")
    all_rows = []
    for i, addr in enumerate(addresses, 1):
        all_rows.extend(score_address(addr, deals_map))
        if i % 50 == 0 or i == len(addresses):
            print(f"  进度 {i}/{len(addresses)}，已打分 {len(all_rows)} 条")
    result = pd.DataFrame(all_rows)
    if result.empty:
        print("无打分结果")
        return
    SCORE_DIR.mkdir(parents=True, exist_ok=True)
    out = SCORE_DIR / f"{datetime.now():%Y-%m-%d}_address_scores.parquet"
    result.sort_values("score", ascending=False).to_parquet(out, index=False)
    print(f"已保存 {len(result)} 条 -> {out}")
    xinshou = result[(result["coin"] == "ALL") & (result["score"] < SCORE_XINSHOU)]
    print(f"ALL score < {SCORE_XINSHOU}: {len(xinshou)} 个")


if __name__ == "__main__":
    main()
