"""查询 OKX 带单员历史带单: GET /api/v5/copytrading/public-subpositions-history
兼容 Python 3.9+
"""
import json
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import requests

URL = "https://www.okx.com/api/v5/copytrading/public-subpositions-history"


def get_public_subpositions_history(
    unique_code: str,
    inst_type: str = "SWAP",
    after: str = "",
    before: str = "",
    limit: str = "100",
    retries: int = 5,
) -> Dict:
    """
    :param unique_code: 带单员唯一标识码
    :param inst_type: 产品类型，默认 SWAP
    :param after: 请求此 subPosId 之前（更旧）的数据
    :param before: 请求此 subPosId 之后（更新）的数据
    :param limit: 返回条数，最大 100，默认 100
    """
    params = {
        "instType": inst_type,
        "uniqueCode": unique_code,
        "limit": str(limit),
    }
    if after:
        params["after"] = after
    if before:
        params["before"] = before
    for i in range(retries):
        resp = requests.get(URL, params=params, timeout=10)
        if resp.status_code == 429:
            wait = 2 ** i
            print("429 限流，{}s 后重试 ({}/{})".format(wait, i + 1, retries))
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return resp.json()


def latest_by_close_time(data: dict) -> Optional[dict]:
    """取 closeTime 最大的一笔，并将 closeTime 转为日期格式。"""
    rows = data.get("data") or []
    if not rows:
        return None
    latest = max(rows, key=lambda x: int(x.get("closeTime") or 0))
    ts = latest.get("closeTime")
    if ts:
        latest = {**latest, "closeTime": datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d %H:%M:%S")}
    return latest


if __name__ == "__main__":
    # 用法: python okx_public_subpositions_history.py <uniqueCode> [limit]
    if len(sys.argv) < 2:
        print("用法: python okx_public_subpositions_history.py <uniqueCode> [limit=100]")
        sys.exit(1)

    code = sys.argv[1]
    limit = sys.argv[2] if len(sys.argv) > 2 else "100"
    data = get_public_subpositions_history(code, limit=limit)
    latest = latest_by_close_time(data)
    if latest is None:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(latest, indent=2, ensure_ascii=False))

