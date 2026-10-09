"""查询带单员当前带单仓位: GET /api/v5/copytrading/public-current-subpositions
每 1s 请求一次。兼容 Python 3.9+
"""
import time
from datetime import datetime, timedelta, timezone

import requests

# ========== 参数 ==========
UNIQUE_CODE = "517A2A0B4F4C76D6"  # 带单员 uniqueCode
INST_TYPE = "SWAP"
LIMIT = "100"
INTERVAL = 3  # 秒
# ==========================

BJ = timezone(timedelta(hours=8))
URL = "https://www.okx.com/api/v5/copytrading/public-current-subpositions"
PARAMS = {
    "instType": INST_TYPE,
    "uniqueCode": UNIQUE_CODE,
    "limit": LIMIT,
}

while True:
    try:
        now = datetime.now(BJ).strftime("%H:%M:%S")
        rows = requests.get(URL, params=PARAMS, timeout=10).json().get("data") or []
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

