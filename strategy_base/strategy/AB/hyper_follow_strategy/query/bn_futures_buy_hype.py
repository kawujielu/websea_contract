#!/usr/bin/env python3
"""Binance U本位：切单向持仓 + 杠杆10x + 市价买约20U HYPE。用法: python query/bn_futures_buy_hype.py"""

import hashlib
import hmac
import math
import time
import requests

API_KEY = "qBY47YZYAP6ZT4BQdOAqf7uyLUSOctSJ3OWeE3qFXBMQSrnNAHyP74tknFePsQFO"
API_SECRET = "9oKFd6iM3YObfPE3xrlDHIMIiMMOum4Z9M2ZPYhLRXxJ2wSqltqbsfmg7jNbI61i"
SYMBOL = "HYPEUSDT"
QUOTE_USDT = 20
LEVERAGE = 10
BASE = "https://fapi.binance.com"
HEADERS = {"X-MBX-APIKEY": API_KEY}


def sign(params):
    params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 10000}
    query = "&".join(f"{k}={v}" for k, v in params.items())
    params["signature"] = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return params


def post(path, params):
    resp = requests.post(f"{BASE}{path}", params=sign(params), headers=HEADERS, timeout=30)
    print(path, resp.status_code, resp.text)
    return resp


def main():
    # 1) 强制单向持仓
    post("/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
    # 2) 杠杆 10x
    post("/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": LEVERAGE})

    px = float(requests.get(f"{BASE}/fapi/v1/ticker/price", params={"symbol": SYMBOL}, timeout=10).json()["price"])
    info = requests.get(f"{BASE}/fapi/v1/exchangeInfo", params={"symbol": SYMBOL}, timeout=10).json()
    step = float(next(f["stepSize"] for f in info["symbols"][0]["filters"] if f["filterType"] == "LOT_SIZE"))
    prec = max(0, int(round(-math.log10(step)))) if step < 1 else 0
    # qty = math.floor(QUOTE_USDT / px / step) * step
    # qty = f"{qty:.{prec}f}".rstrip("0").rstrip(".") if prec else str(int(qty))
    qty = 0.4
    print(f"{SYMBOL} price={px} qty={qty} (~{QUOTE_USDT}U) lev={LEVERAGE}")

    # 3) 市价买入（单向持仓 positionSide=BOTH）
    post("/fapi/v1/order", {
        "symbol": SYMBOL, "side": "SELL", "type": "MARKET",
        "quantity": qty, "positionSide": "BOTH",
    })


if __name__ == "__main__":
    main()
