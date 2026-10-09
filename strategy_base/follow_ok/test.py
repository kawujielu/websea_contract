"""指定 OKX 带单员：历史各交易对盈亏绝对值与占比（结合币种偏好）。"""
from __future__ import annotations

import time
from collections import defaultdict

from okx_public_preference_currency import get_public_preference_currency
from okx_public_subpositions_history import get_public_subpositions_history

# ========== 参数 ==========
LEAD_ID = "802192502018438605"  # 带单员 uniqueCode
INST_TYPE = "SWAP"
PAGE_LIMIT = "100"
MAX_PAGES = 50          # 最多翻页次数
API_SLEEP = 0.3
# ==========================


def fetch_all_history(unique_code: str) -> list:
    """翻页拉取历史带单（after=更旧）。"""
    rows, after = [], ""
    for _ in range(MAX_PAGES):
        data = get_public_subpositions_history(
            unique_code, inst_type=INST_TYPE, after=after, limit=PAGE_LIMIT
        )
        time.sleep(API_SLEEP)
        batch = data.get("data") or []
        if not batch:
            break
        rows.extend(batch)
        after = str(batch[-1].get("subPosId") or "")
        if not after or len(batch) < int(PAGE_LIMIT):
            break
    return rows


def main():
    pref = get_public_preference_currency(LEAD_ID, INST_TYPE)
    time.sleep(API_SLEEP)
    ccy_ratio = {
        str(r.get("ccy", "")).upper(): float(r.get("ratio") or 0)
        for r in (pref.get("data") or [])
    }

    rows = fetch_all_history(LEAD_ID)
    # instId -> [净盈亏, |pnl|合计, 笔数]
    agg = defaultdict(lambda: [0.0, 0.0, 0])
    for r in rows:
        inst = r.get("instId") or ""
        if not inst:
            continue
        pnl = float(r.get("pnl") or 0)
        a = agg[inst]
        a[0] += pnl
        a[1] += abs(pnl)
        a[2] += 1

    total_abs = sum(v[1] for v in agg.values()) or 1.0
    ranked = sorted(agg.items(), key=lambda x: x[1][1], reverse=True)

    print("带单员 {}  历史笔数={}  币种偏好={}".format(
        LEAD_ID, len(rows),
        {k: round(v, 4) for k, v in sorted(ccy_ratio.items(), key=lambda x: -x[1])},
    ))
    print("{:<22} {:>12} {:>12} {:>10} {:>8} {:>10}".format(
        "交易对", "净盈亏", "盈亏绝对值", "绝对值占比", "笔数", "币种偏好"
    ))
    for inst, (net, abs_pnl, n) in ranked:
        ccy = inst.split("-")[0].upper() if inst else ""
        share = abs_pnl / total_abs * 100
        print("{:<22} {:>12.2f} {:>12.2f} {:>9.2f}% {:>8} {:>9.2%}".format(
            inst, net, abs_pnl, share, n, ccy_ratio.get(ccy, 0.0),
        ))
    print("合计净盈亏={:.2f}  合计盈亏绝对值={:.2f}".format(
        sum(v[0] for v in agg.values()), sum(v[1] for v in agg.values()),
    ))


if __name__ == "__main__":
    main()

