"""查询 OKX 带单员币种偏好: GET /api/v5/copytrading/public-preference-currency
兼容 Python 3.9+
"""
import json
import sys
from typing import Dict

import requests

URL = "https://www.okx.com/api/v5/copytrading/public-preference-currency"


def get_public_preference_currency(unique_code: str, inst_type: str = "SWAP") -> Dict:
    """
    :param unique_code: 带单员唯一标识码
    :param inst_type: 产品类型，默认 SWAP
    """
    params = {"instType": inst_type, "uniqueCode": unique_code}
    resp = requests.get(URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    # 用法: python okx_public_preference_currency.py <uniqueCode> [instType]
    if len(sys.argv) < 2:
        print("用法: python okx_public_preference_currency.py <uniqueCode> [instType=SWAP]")
        sys.exit(1)

    code = sys.argv[1]
    inst = sys.argv[2] if len(sys.argv) > 2 else "SWAP"
    data = get_public_preference_currency(code, inst)
    print(json.dumps(data, indent=2, ensure_ascii=False))

