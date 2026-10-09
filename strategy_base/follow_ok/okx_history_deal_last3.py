"""查询 OKX 带单员历史带单，展示最近 3 笔。"""
import json
import sys
import time
from datetime import datetime

import requests

URL = "https://www.okx.com/api/v5/copytrading/public-subpositions-history"


def get_public_subpositions_history(unique_code: str, inst_type: str = "SWAP", limit: str = "20") -> dict:
    params = {"instType": inst_type, "uniqueCode": unique_code, "limit": str(limit)}
    for i in range(5):
        resp = requests.get(URL, params=params, timeout=10)
        if resp.status_code == 429:
            time.sleep(2 ** i)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


def latest_n(data: dict, n: int = 3) -> list:
    rows = data.get("data") or []
    rows = sorted(rows, key=lambda x: int(x.get("closeTime") or 0), reverse=True)[:n]
    out = []
    for r in rows:
        item = dict(r)
        ts = item.get("closeTime")
        if ts:
            item["closeTime"] = datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        out.append(item)
    return out


if __name__ == "__main__":
    # 用法: python okx_latest_3_subpositions.py [uniqueCode]
    code = sys.argv[1] if len(sys.argv) > 1 else "B15E19173830B674"
    while True:
        try:
            data = get_public_subpositions_history(code)
            rows = latest_n(data, 10)
            print("\n=== {} ===".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            if not rows:
                print(json.dumps(data, indent=2, ensure_ascii=False))
            else:
                print(json.dumps(rows, indent=2, ensure_ascii=False))
        except Exception as e:
            print("查询失败:", e)
        time.sleep(3)

