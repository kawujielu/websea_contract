#!/usr/bin/env python3
"""
功能：按钱包地址 + 指定交易对拉取历史成交并打分（userFillsByTime 分页）。

用法：
    python3 score_wallet.py
"""

import time

import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LinearRegression

# ---------- 参数 ----------
ADDRESS = "0xc1e42f862d202b4a0ed552c1145735ee088f6ccf"
COIN = "HYPE"                   # 统计/打分的交易对
API_URL = "https://api.hyperliquid.xyz/info"
TIMEOUT = 30
SLEEP_SEC = 1
START_TIME_MS = 0               # 从 earliest 开始拉
PAGE_LIMIT = 2000               # 单次返回上限


def post_info(payload):
    resp = requests.post(
        API_URL, json=payload,
        headers={"Content-Type": "application/json"}, timeout=TIMEOUT,
    )
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def fetch_all_fills(user):
    """userFillsByTime 正向分页，尽量拉全历史（官方上限约 1 万笔）。"""
    all_fills, start = [], START_TIME_MS
    while True:
        data = post_info({
            "type": "userFillsByTime",
            "user": user,
            "startTime": start,
            "aggregateByTime": False,
        })
        time.sleep(SLEEP_SEC)
        if not isinstance(data, list) or not data:
            break
        all_fills.extend(data)
        print(f"  已拉取 {len(all_fills)} 笔 (本页 {len(data)})")
        if len(data) < PAGE_LIMIT:
            break
        start = int(data[-1]["time"]) + 1
    # 去重（按 tid/hash+time）
    seen, uniq = set(), []
    for f in all_fills:
        k = (f.get("tid"), f.get("hash"), f.get("time"), f.get("oid"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(f)
    return uniq


def fetch_equity(user):
    data = post_info({"type": "clearinghouseState", "user": user})
    time.sleep(SLEEP_SEC)
    val = (data.get("marginSummary") or {}).get("accountValue")
    return float(val) if val is not None else None


def score_fills(df):
    """仅对传入的该交易对成交打分（同 score_address_deals，筛 Long|Short）。"""
    d = df[df["dir"].str.contains("Long|Short", case=False, na=False)].copy()
    d = d.sort_values("date").reset_index(drop=True)
    n = len(d)
    if n == 0:
        return 0.0
    x = np.arange(1, n + 1).reshape(-1, 1)
    y = d["closed_pnl"].cumsum().values
    model = LinearRegression().fit(x, y)
    r_sq = float(model.score(x, y)) if n > 1 else 0.0
    mean_pnl = float(d["closed_pnl"].mean())
    std_pnl = float(d["closed_pnl"].std(ddof=1)) if n > 1 else 0.0
    return float(r_sq * (mean_pnl / std_pnl) * np.sqrt(n)) if std_pnl > 0 and not np.isnan(std_pnl) else 0.0


def main():
    user = ADDRESS.lower()
    print(f"地址: {user}  交易对: {COIN}")
    fills = fetch_all_fills(user)
    # 只保留指定交易对（HYPE）的成交，后续统计与打分都基于此
    coin_fills = [f for f in fills if f.get("coin") == COIN]
    equity = fetch_equity(user)

    if not coin_fills:
        print(f"无 {COIN} 成交")
        print(f"账户权益: {equity}")
        return

    df = pd.DataFrame({
        "coin": COIN,
        "dir": [str(f.get("dir") or "") for f in coin_fills],
        "closed_pnl": [float(f.get("closedPnl") or 0) for f in coin_fills],
        "date": [int(f.get("time") or 0) for f in coin_fills],
        "notional": [abs(float(f.get("px") or 0) * float(f.get("sz") or 0)) for f in coin_fills],
    })
    n = len(df)
    total_pnl = float(df["closed_pnl"].sum())
    mean_pnl = float(df["closed_pnl"].mean())
    avg_amt = float(df["notional"].mean())
    score = score_fills(df)  # 只打分该交易对（HYPE）
    print(f"钱包地址: {user}")
    print(f"交易对: {COIN}")
    print(f"交易笔数: {n}")
    print(f"账户权益: {equity}")
    print(f"交易总盈亏: {total_pnl:.6f}")
    print(f"平均每笔交易盈亏: {mean_pnl:.6f}")
    print(f"平均每笔交易金额: {avg_amt:.6f}")
    print(f"{COIN}打分分数: {score:.6f}")


if __name__ == "__main__":
    main()
