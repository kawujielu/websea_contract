#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询 Websea 合约账户余额。
文档: https://webseaex.github.io/zh/futures-trade/wallet/
"""
import hashlib
import json
import random
import string
import time

import requests

# ---------- 参数（写死） ----------
API_KEY = "53dab96a47bd1021d9dcef3d9fk59654212"
API_SECRET = "p56gp29wyz5dybrbu430"
BASE_URL = "https://oapi.websea.com"
WALLET_PATH = "/v1/futures/wallet"
TIMEOUT_SEC = 15


def _nonce() -> str:
    suffix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(5))
    return f"{int(time.time() * 1000)}_{suffix}"


def _signature(nonce: str) -> str:
    parts = sorted([API_KEY, API_SECRET, nonce])
    return hashlib.sha1("".join(parts).encode("utf-8")).hexdigest()


def fetch_futures_wallet() -> dict:
    nonce = _nonce()
    headers = {
        "Token": API_KEY,
        "Nonce": nonce,
        "Signature": _signature(nonce),
    }
    url = f"{BASE_URL}{WALLET_PATH}"
    resp = requests.get(url, headers=headers, timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    data = fetch_futures_wallet()
    print(json.dumps(data, ensure_ascii=False, indent=2))

    if data.get("errno") != 0:
        print(f"请求失败: errno={data.get('errno')} errmsg={data.get('errmsg')}")
        return

    rows = data.get("result") or []
    if not rows:
        print("无余额记录")
        return

    print("\n--- 合约账户余额 ---")
    for row in rows:
        print(
            f"{row.get('asset')}: "
            f"balance={row.get('balance')} "
            f"avail={row.get('avail')} "
            f"hold={row.get('hold')} "
            f"frozen={row.get('frozen')} "
            f"unPnl={row.get('unPnl')}"
        )


if __name__ == "__main__":
    main()

