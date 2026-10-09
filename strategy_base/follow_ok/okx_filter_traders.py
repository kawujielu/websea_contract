"""
从 OKX 筛选 4 类带单员（一次跑完）：
1. 主流币占比>70% 且 收益率>20%
2. 非主流币占比>70% 且 收益率>20%
3. 主流币占比>70% 且 收益率<-20%
4. 非主流币占比>70% 且 收益率<-20%
兼容 Python 3.9+
"""
import json
import time
from typing import Dict, List

import requests
from okx_public_subpositions_history import get_public_subpositions_history
from okx_public_preference_currency import get_public_preference_currency
from okx_public_current_subpositions import get_public_current_subpositions

URL = "https://www.okx.com/api/v5/copytrading/public-lead-traders"
MIN_LEAD_DAYS = "1"
LIMIT = 20
MAX_PAGE = 9
MAJOR_CCY = {"BTC", "ETH", "BNB", "SOL"}
MAX_LAST_TRADE_DAYS = 2
MAJOR_RATIO_MIN = 0.5   # 主流/非主流占比阈值
PNL_RATIO_ABS = 20      # 收益率绝对值阈值（%）
API_SLEEP = 1

CATS = ("主流高收益", "非主流高收益", "主流大亏", "非主流大亏")


def _profit_loss_ratio(unique_code: str):
    data = get_public_subpositions_history(unique_code)
    time.sleep(API_SLEEP)
    rows = data.get("data") or []
    profits, losses, last_ts = [], [], 0
    for row in rows:
        last_ts = max(last_ts, int(row.get("closeTime") or 0))
        pnl = float(row.get("pnl") or 0)
        if pnl > 0:
            profits.append(pnl)
        elif pnl < 0:
            losses.append(abs(pnl))
    last_days = round((time.time() * 1000 - last_ts) / 86400000, 1) if last_ts else None
    if not profits or not losses:
        return (0 if not profits and not losses else 1), last_days
    return round((sum(profits) / len(profits)) / (sum(losses) / len(losses)), 2), last_days


def _major_ccy_ratio(unique_code: str) -> float:
    data = get_public_preference_currency(unique_code)
    time.sleep(API_SLEEP)
    return round(sum(
        float(r.get("ratio") or 0)
        for r in (data.get("data") or [])
        if str(r.get("ccy", "")).upper() in MAJOR_CCY
    ), 4)


def _positions_visible(unique_code: str) -> bool:
    rows = get_public_current_subpositions(unique_code).get("data") or []
    time.sleep(API_SLEEP)
    return not any(not r.get("instId") and not r.get("markPx") for r in rows)


def _classify(major_ratio: float, pnl_ratio: float):
    """返回类别名，不匹配则 None。非主流占比 = 1 - 主流占比。"""
    if major_ratio > MAJOR_RATIO_MIN and pnl_ratio > PNL_RATIO_ABS:
        return CATS[0]
    if (1 - major_ratio) > MAJOR_RATIO_MIN and pnl_ratio > PNL_RATIO_ABS:
        return CATS[1]
    if major_ratio > MAJOR_RATIO_MIN and pnl_ratio < -PNL_RATIO_ABS:
        return CATS[2]
    if (1 - major_ratio) > MAJOR_RATIO_MIN and pnl_ratio < -PNL_RATIO_ABS:
        return CATS[3]
    return None


def filter_traders() -> Dict[str, List[dict]]:
    """返回 {类别: [详情dict, ...]}，统计完成后由调用方打印。"""
    result = {c: [] for c in CATS}
    data_ver = None
    for page in range(1, MAX_PAGE + 1):
        params = {"instType": "SWAP", "minLeadDays": MIN_LEAD_DAYS, "page": page, "limit": LIMIT}
        if data_ver:
            params["dataVer"] = data_ver
        resp = requests.get(URL, params=params, timeout=10).json()
        time.sleep(API_SLEEP)
        if resp.get("code") != "0" or not resp.get("data"):
            break
        block = resp["data"][0]
        ranks = block.get("ranks") or []
        if not ranks:
            break
        data_ver = block.get("dataVer") or data_ver

        for r in ranks:
            trader_id = r["uniqueCode"]
            plr, last_days = _profit_loss_ratio(trader_id)
            if last_days is None or last_days >= MAX_LAST_TRADE_DAYS:
                continue
            major_ratio = _major_ccy_ratio(trader_id)
            pnl_ratio = round(float(r.get("pnlRatio") or 0) * 100, 2)
            cat = _classify(major_ratio, pnl_ratio)
            if not cat or not _positions_visible(trader_id):
                continue
            result[cat].append({
                "uniqueCode": trader_id,
                "nickName": r.get("nickName", ""),
                "winRatio": float(r.get("winRatio") or 0),
                "plr": plr,
                "leadDays": r.get("leadDays"),
                "leadAum": int(float(r.get("aum") or 0)),
                "pnl": int(float(r.get("pnl") or 0)),
                "pnlRatio": pnl_ratio,
                "copyNum": int(r.get("copyTraderNum") or 0),
                "majorRatio": major_ratio,
                "lastDays": last_days,
            })

        if page >= int(block.get("totalPage") or page):
            break

    for c in CATS:
        result[c] = list({t["uniqueCode"]: t for t in result[c]}.values())
    return result


def print_result(result: Dict[str, List[dict]]):
    for cat in CATS:
        rows = result.get(cat) or []
        print("\n===== {} ({}人) =====".format(cat, len(rows)))
        for t in rows:
            print(
                "带单员:{} {} 胜率:{} 盈亏比:{} 带单天数:{} 带单规模:{} "
                "盈亏:{} 收益率:{}% 跟单人数:{} 主流币占比:{} 距上次交易:{}天".format(
                    t["uniqueCode"], t["nickName"], t["winRatio"], t["plr"],
                    t["leadDays"], t["leadAum"], t["pnl"], t["pnlRatio"],
                    t["copyNum"], t["majorRatio"], t["lastDays"],
                )
            )


if __name__ == "__main__":
    result = filter_traders()
    print_result(result)
    print(json.dumps(result, indent=1, ensure_ascii=False))

