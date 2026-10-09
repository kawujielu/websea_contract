#!/usr/bin/env python3
"""查询 Hyperliquid 账户资金。用法: python query/query_user_balance.py"""

import requests

API_URL = "https://api.hyperliquid.xyz/info"
USER = "0x42982dafd5577db35255bb1dab0b288464844de1"
DEX = ""
TIMEOUT = 30


def fetch_balance(user=None):
    user = user or USER
    payload = {"type": "clearinghouseState", "user": user}
    if DEX:
        payload["dex"] = DEX
    try:
        resp = requests.post(API_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise RuntimeError(f"网络请求失败: {e}") from e
    if not resp.ok:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"响应格式异常: {data}")
    return data


def main():
    data = fetch_balance()
    margin = data.get("marginSummary") or {}
    print(f"地址: {USER}")
    print(f"accountValue: {margin.get('accountValue')}")
    print(f"withdrawable: {data.get('withdrawable')}")


if __name__ == "__main__":
    main()
