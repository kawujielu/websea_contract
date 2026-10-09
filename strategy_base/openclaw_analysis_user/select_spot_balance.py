#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询 WebSea / Gate / Binance 现货指定币种余额（非零才输出）。

用法: python select_spot_balance.py BTC
依赖: pip install requests
"""

from __future__ import annotations

import hashlib
import hmac
import random
import string
import sys
import time
from operator import itemgetter
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SPOT_HOST = "https://oapi.websea.com"
GATE_HOST = "https://api.gateio.ws"
GATE_API_PREFIX = "/api/v4"
BINANCE_HOST = "https://api.binance.com"
REQUEST_TIMEOUT = 30

ACCOUNTS = {
    "maker_near": {"token": "18c4725b9d218777c3863f812e21d9a2522", "sk": "2ijh2r7nf8nvjce4rnq7", "uid": 11},
    "maker_defense": {"token": "291d6fa8dc35f58690c38f7c3afcf2h2808", "sk": "7rk8zhyxbbxk5ro8r4to", "uid": 12},
    "maker_depth": {"token": "e5451dae5de519289e45aaab4be461f2856", "sk": "lmtn1yxnlwq6ecjqs5yv", "uid": 13},
    "dc": {"token": "78fc47c5590f77ca42d1e2c3bb432813", "sk": "5e5yg7ga285hctuqrtpb", "uid": 196},
    "spot_bak_1": {"token": "4ee5b2948440de07dc09c139835cfep3248", "sk": "ali6bpfyqv4lgaln4eq9", "uid": 14},
    "spot_bak_2": {"token": "d83e771c4d55f1fef98e34a2c43772o3330", "sk": "g1xo2axzjdax2ujh6twf", "uid": 15},
    "spot_bak_3": {"token": "027dc638e8819a3f8233645c9de076r3534", "sk": "rpaw214rnbgvt0vhl52j", "uid": 16},
    "spot_bak_4": {"token": "2ec49187f681419d1959af499d7bb0p3584", "sk": "wcj34ct96l2vh3s1hog7", "uid": 17},
}
GATE_ACCOUNT = {
    "apiKey": "dd0eeebf5f147b1c8c1cfb5c1db2f38f",
    "secret": "a05598264dd97f2b3aa614efea65b69b02fd972c3f285253da80c43a95b6343e",
}
BINANCE_ACCOUNT = {
    "apiKey": "lWbYa9CHzHSyulhmaPcXLyUT6coxSQAULHgAYU9NjA5UM3qd2SMZeR9cCBd4uiIE",
    "secret": "U40aH29UAiotheaikRk2eUvZtQOt8psNSyNGAYRLocLfeg6HKeIhHDqGkWY8rPqU",
}


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _ws_headers(token: str, sk: str, data: Dict[str, Any]) -> Dict[str, str]:
    nonce = "%d_%s" % (int(time.time() * 1000), "".join(random.sample(string.ascii_letters + string.digits, 5)))
    parts = [token, sk, nonce] + [f"{k}={v}" for k, v in data.items()]
    return {
        "Token": token,
        "Nonce": nonce,
        "Signature": hashlib.sha1("".join(sorted(parts)).encode()).hexdigest(),
        "User-Agent": "Mozilla/5.0",
    }


def fetch_websea(token: str, sk: str) -> Dict[str, Any]:
    params = {"show_all": 1}
    resp = requests.get(
        SPOT_HOST.rstrip("/") + "/openApi/wallet/list",
        params=params,
        headers=_ws_headers(token, sk, params),
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200:
        return {"errno": -1, "errmsg": f"HTTP {resp.status_code}", "result": body}
    return body


def fetch_gate(api_key: str, secret: str) -> Dict[str, Any]:
    path = f"{GATE_API_PREFIX}/spot/accounts"
    ts = str(time.time())
    m = hashlib.sha512()
    m.update(b"")
    sign = hmac.new(
        secret.encode(),
        ("%s\n%s\n\n%s\n%s" % ("GET", path, m.hexdigest(), ts)).encode(),
        hashlib.sha512,
    ).hexdigest()
    resp = requests.get(
        f"{GATE_HOST.rstrip('/')}{path}",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": sign,
        },
        timeout=REQUEST_TIMEOUT,
    )
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200 or not isinstance(body, list):
        return {"errno": -1, "errmsg": f"HTTP {resp.status_code}", "result": body}
    return {"errno": 0, "result": body}


def fetch_binance(api_key: str, secret: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {"timestamp": int(time.time() * 1000)}
    qs0 = "&".join(f"{k}={v}" for k, v in sorted(data.items()))
    data["signature"] = hmac.new(secret.encode(), qs0.encode(), hashlib.sha256).hexdigest()
    ordered = [(k, v) for k, v in sorted(data.items(), key=itemgetter(0)) if k != "signature"]
    ordered.append(("signature", data["signature"]))
    qs = "&".join(f"{k}={v}" for k, v in ordered)
    resp = requests.get(
        f"{BINANCE_HOST.rstrip('/')}/api/v3/account?{qs}",
        headers={"Accept": "application/json", "X-MBX-APIKEY": api_key},
        timeout=REQUEST_TIMEOUT,
    )
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200 or not isinstance(body, dict) or "code" in body:
        msg = body.get("msg", f"HTTP {resp.status_code}") if isinstance(body, dict) else str(body)
        return {"errno": -1, "errmsg": msg}
    bals = body.get("balances")
    if not isinstance(bals, list):
        return {"errno": -1, "errmsg": "unexpected response"}
    return {"errno": 0, "result": bals}


def _pick(
    rows: List[Any], currency: str, avail_key: str, frozen_key: str, cur_key: str
) -> Optional[Tuple[float, float, float]]:
    cur = currency.upper()
    for item in rows:
        if not isinstance(item, dict):
            continue
        if str(item.get(cur_key, "")).upper() != cur:
            continue
        avail = _to_float(item.get(avail_key, 0))
        frozen = _to_float(item.get(frozen_key, 0))
        total = avail + frozen
        if total <= 0:
            return None
        return avail, frozen, total
    return None


def query_coin_balances(currency: str) -> List[str]:
    lines: List[str] = []
    cur = currency.upper()

    for meta in ACCOUNTS.values():
        label = str(meta["uid"])
        try:
            payload = fetch_websea(meta["token"], meta["sk"])
            if payload.get("errno") != 0:
                lines.append(f"{label}: 查询失败 {payload.get('errmsg', 'unknown')}")
                continue
            hit = _pick(payload.get("result") or [], cur, "available", "frozen", "currency")
            if hit:
                a, f, t = hit
                lines.append(f"{label}: 可用={a:.8f} 冻结={f:.8f} 合计={t:.8f}")
        except requests.RequestException as e:
            lines.append(f"{label}: 请求异常 {e}")

    try:
        payload = fetch_gate(GATE_ACCOUNT["apiKey"], GATE_ACCOUNT["secret"])
        if payload.get("errno") != 0:
            lines.append(f"gate: 查询失败 {payload.get('errmsg', 'unknown')}")
        else:
            hit = _pick(payload.get("result") or [], cur, "available", "locked", "currency")
            if hit:
                a, f, t = hit
                lines.append(f"gate: 可用={a:.8f} 冻结={f:.8f} 合计={t:.8f}")
    except requests.RequestException as e:
        lines.append(f"gate: 请求异常 {e}")

    try:
        payload = fetch_binance(BINANCE_ACCOUNT["apiKey"], BINANCE_ACCOUNT["secret"])
        if payload.get("errno") != 0:
            lines.append(f"binance: 查询失败 {payload.get('errmsg', 'unknown')}")
        else:
            hit = _pick(payload.get("result") or [], cur, "free", "locked", "asset")
            if hit:
                a, f, t = hit
                lines.append(f"binance: 可用={a:.8f} 冻结={f:.8f} 合计={t:.8f}")
    except requests.RequestException as e:
        lines.append(f"binance: 请求异常 {e}")

    return lines


def format_balance_report(currency: str) -> str:
    cur = currency.upper()
    lines = query_coin_balances(cur)
    if not lines:
        return f"各账户 {cur} 余额均为 0"
    return f"{cur} 余额（非零）\n" + "\n".join(lines)


def main() -> int:
    if len(sys.argv) < 2 or not sys.argv[1].strip():
        print("用法: python select_spot_balance.py <币种>", file=sys.stderr)
        return 2
    print(format_balance_report(sys.argv[1].strip()))
    return 0


if __name__ == "__main__":
    sys.exit(main())

