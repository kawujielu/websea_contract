"""查询 OKX 带单员公开表现统计: GET /api/v5/copytrading/public-stats
兼容 Python 3.9+
"""
import json
import sys
from typing import Dict

import requests

URL = "https://www.okx.com/api/v5/copytrading/public-stats"
# lastDays 枚举: 1=7天, 2=30天, 3=90天, 4=180天
DAYS_MAP = {"7": "1", "30": "2", "90": "3", "180": "4"}


def get_public_stats(unique_code: str, last_days: str = "2", inst_type: str = "SWAP") -> Dict:
    """
    :param unique_code: 带单员唯一标识码
    :param last_days: 1/2/3/4，或 7/30/90/180
    :param inst_type: 产品类型，默认 SWAP
    """
    last_days = DAYS_MAP.get(str(last_days), str(last_days))
    params = {
        "instType": inst_type,
        "uniqueCode": unique_code,
        "lastDays": last_days,
    }
    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    # 用法: python okx_public_stats.py <uniqueCode> [lastDays]
    if len(sys.argv) < 2:
        print("用法: python okx_public_stats.py <uniqueCode> [lastDays=2]")
        print("lastDays: 1=7天, 2=30天, 3=90天, 4=180天（也可传 7/30/90/180）")
        sys.exit(1)

    code = sys.argv[1]
    days = sys.argv[2] if len(sys.argv) > 2 else "2"
    data = get_public_stats(code, days)
    print(json.dumps(data, indent=2, ensure_ascii=False))

