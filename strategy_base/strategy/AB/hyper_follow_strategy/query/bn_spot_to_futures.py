#!/usr/bin/env python3
"""Binance 现货 -> U本位合约 划转。用法: python query/bn_spot_to_futures.py"""

import hashlib
import hmac
import time
import requests

API_KEY = "qBY47YZYAP6ZT4BQdOAqf7uyLUSOctSJ3OWeE3qFXBMQSrnNAHyP74tknFePsQFO"
API_SECRET = "9oKFd6iM3YObfPE3xrlDHIMIiMMOum4Z9M2ZPYhLRXxJ2wSqltqbsfmg7jNbI61i"
AMOUNT = 100  # USDT
# type: 1=现货->U本位合约, 2=U本位合约->现货
TRANSFER_TYPE = 1
URL = "https://api.binance.com/sapi/v1/futures/transfer"


def main():
    params = {
        "asset": "USDT",
        "amount": AMOUNT,
        "type": TRANSFER_TYPE,
        "timestamp": int(time.time() * 1000),
        "recvWindow": 10000,
    }
    query = "&".join(f"{k}={v}" for k, v in params.items())
    params["signature"] = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    resp = requests.post(URL, params=params, headers={"X-MBX-APIKEY": API_KEY}, timeout=30)
    print(resp.status_code, resp.text)


if __name__ == "__main__":
    main()
