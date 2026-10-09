"""查询当前跟单持仓: GET /api/v5/copytrading/current-subpositions
兼容 Python 3.9+

用法: python okx_current_subpositions.py [instType=SWAP] [uniqueCode]
"""
import base64
import hashlib
import hmac
import json
import sys
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

# ========== 手动填写 ==========
API_KEY = "edb91757-df52-4bd1-a04a-576b7aad0b84"
SECRET_KEY = "7E785C4C118E33832852DB85C5A55C4E"
PASSPHRASE = "265199_JJsy"
# ==============================

BASE = "https://www.okx.com"
PATH = "/api/v5/copytrading/current-subpositions"


def _sign(timestamp, method, path_with_query, body=""):
    msg = "{}{}{}{}".format(timestamp, method, path_with_query, body)
    dig = hmac.new(SECRET_KEY.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(dig).decode()


def get_current_subpositions(inst_type="SWAP", unique_code=""):
    params = {"instType": inst_type}
    if unique_code:
        params["instId"] = unique_code
    query = "?" + urlencode(params)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    headers = {
        "OK-ACCESS-KEY": API_KEY,
        "OK-ACCESS-SIGN": _sign(ts, "GET", PATH + query),
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": PASSPHRASE,
    }
    return requests.get(BASE + PATH, params=params, headers=headers, timeout=10).json()


if __name__ == "__main__":
    inst = sys.argv[1] if len(sys.argv) > 1 else "SWAP"
    code = sys.argv[2] if len(sys.argv) > 2 else ""
    print(json.dumps(get_current_subpositions(inst, code), indent=2, ensure_ascii=False))


