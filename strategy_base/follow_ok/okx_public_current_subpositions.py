"""查询带单员当前带单仓位: GET /api/v5/copytrading/public-current-subpositions
每 INTERVAL 秒请求一次。兼容 Python 3.9+
"""
import time
from datetime import datetime, timedelta, timezone

import requests

# ========== 参数 ==========
UNIQUE_CODE = "B15E19173830B674"  # 带单员 uniqueCode
INST_TYPE = "SWAP"
LIMIT = "100"
INTERVAL = 3  # 秒
# ==========================

BJ = timezone(timedelta(hours=8))
URL = "https://www.okx.com/api/v5/copytrading/public-current-subpositions"


def get_public_current_subpositions(unique_code, inst_type=INST_TYPE, limit=LIMIT):
    params = {"instType": inst_type, "uniqueCode": unique_code, "limit": limit}
    return requests.get(URL, params=params, timeout=10).json()


if __name__ == "__main__":
    while True:
        try:
            now = datetime.now(BJ).strftime("%H:%M:%S")
            rows = get_public_current_subpositions(UNIQUE_CODE).get("data") or []
            print("----- {} 共{}笔 -----".format(now, len(rows)))
            for r in rows:
                ts = int(r.get("openTime") or 0)
                open_time = datetime.fromtimestamp(ts / 1000, BJ).strftime("%Y-%m-%d %H:%M:%S") if ts else ""
                print("{} 仓位={} openAvgPx={} markPx={} openTime={} posSide={} subPos={} pnl={}".format(
                    r.get("instId"), r.get("margin"), r.get("openAvgPx"), r.get("markPx"),
                    open_time, r.get("posSide"), r.get("subPos"), r.get("upl"),
                ))
        except Exception as e:
            print("[{}] 请求失败: {}".format(datetime.now(BJ).strftime("%H:%M:%S"), e))
        time.sleep(INTERVAL)

