#!/usr/bin/env python3
"""查询 Hyperliquid 用户成交（userFillsByTime 分页拉全）。用法: python query/query_user_fills.py"""

import time

import requests

API_URL = "https://api.hyperliquid.xyz/info"
USER = "0x42982dafd5577db35255bb1dab0b288464844de1"
TIMEOUT = 30
PAGE_LIMIT = 2000
PAGE_SLEEP = 2


def fetch_user_fills(user=None):
    """分页拉取尽量全部历史成交（官方约保留最近 1 万笔）。"""
    user = user or USER
    all_fills, start = [], 0
    while True:
        try:
            resp = requests.post(
                API_URL,
                json={
                    "type": "userFillsByTime",
                    "user": user,
                    "startTime": start,
                    "aggregateByTime": False,
                },
                headers={"Content-Type": "application/json"},
                timeout=TIMEOUT,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"网络请求失败: {e}") from e
        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(f"响应格式异常: {data}")
        if not data:
            break
        all_fills.extend(data)
        if len(data) < PAGE_LIMIT:
            break
        start = int(data[-1]["time"]) + 1
        time.sleep(PAGE_SLEEP)
    seen, uniq = set(), []
    for f in all_fills:
        k = (f.get("tid"), f.get("hash"), f.get("time"), f.get("oid"))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(f)
    return uniq


def main():
    fills = fetch_user_fills()
    print(f"地址: {USER}, 成交笔数: {len(fills)}")
    for i, f in enumerate(fills, 1):
        print(f"{i}. {f.get('coin')} px={f.get('px')} sz={f.get('sz')} pnl={f.get('closedPnl')}")


if __name__ == "__main__":
    main()
