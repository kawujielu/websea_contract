#!/usr/bin/env python3
"""查询 Hyperliquid 持仓。用法: python query/query_user_positions.py"""

import requests

API_URL = "https://api.hyperliquid.xyz/info"
USER = "0x42982dafd5577db35255bb1dab0b288464844de1"
DEX = ""
TIMEOUT = 30


def fetch_clearinghouse(user=None):
    """返回 clearinghouseState 全量（含 assetPositions、marginSummary）。"""
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


def fetch_positions(user=None):
    return fetch_clearinghouse(user).get("assetPositions") or []


def main():
    positions = fetch_positions()
    print(f"地址: {USER}, 持仓: {len(positions)}")
    for i, item in enumerate(positions, 1):
        p = item.get("position") or {}
        print(f"{i}. {p.get('coin')} szi={p.get('szi')} entry={p.get('entryPx')} pnl={p.get('unrealizedPnl')}")


if __name__ == "__main__":
    main()
