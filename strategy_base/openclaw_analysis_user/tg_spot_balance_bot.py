#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram 群：监控「BTC 余额」类消息，查询 WebSea / Gate / Binance 现货该币非零余额。

与 telegram_bot.py 并行运行需使用不同 BOT_TOKEN。
依赖: pip install requests python-telegram-bot
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import random
import re
import string
import time
from operator import itemgetter
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
LOG = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ======================== 配置（请填写） ========================
BOT_TOKEN = "8880661986:AAGLCj50KGwkP4IooWAJe--uG0Wug0jToJg"  #os.environ.get("TG_SPOT_BALANCE_BOT_TOKEN", "")  # 新 Bot Token，勿与用户分析 bot 共用
ALLOWED_CHAT_ID: Optional[int] = "1843312449"  #None  # 新群 chat_id，建群后填入；None 表示不限制

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
# ==============================================================

_BALANCE_RE = re.compile(
    r"(?i)(?:(?P<c1>[A-Za-z0-9]{2,16})\s*余额|余额\s*(?P<c2>[A-Za-z0-9]{2,16}))"
)
_CHAT_LOCKS: Dict[int, asyncio.Lock] = {}


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _ws_sign(token: str, sk: str, nonce: str, data: Dict[str, Any]) -> str:
    parts = [token, sk, nonce] + [f"{k}={v}" for k, v in data.items()]
    return hashlib.sha1("".join(sorted(parts)).encode()).hexdigest()


def _ws_headers(token: str, sk: str, data: Dict[str, Any]) -> Dict[str, str]:
    nonce = "%d_%s" % (int(time.time() * 1000), "".join(random.sample(string.ascii_letters + string.digits, 5)))
    return {
        "Token": token,
        "Nonce": nonce,
        "Signature": _ws_sign(token, sk, nonce, data),
        "User-Agent": "Mozilla/5.0",
    }


def fetch_websea(token: str, sk: str) -> Dict[str, Any]:
    url = SPOT_HOST.rstrip("/") + "/openApi/wallet/list"
    params = {"show_all": 1}
    resp = requests.get(url, params=params, headers=_ws_headers(token, sk, params),
                        timeout=REQUEST_TIMEOUT, verify=False)
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200:
        return {"errno": -1, "errmsg": f"HTTP {resp.status_code}", "result": body}
    return body


def _gate_sign(secret: str, method: str, path: str, ts: str) -> str:
    m = hashlib.sha512()
    m.update(b"")
    payload = "%s\n%s\n\n%s\n%s" % (method, path, m.hexdigest(), ts)
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha512).hexdigest()


def fetch_gate(api_key: str, secret: str) -> Dict[str, Any]:
    path = f"{GATE_API_PREFIX}/spot/accounts"
    ts = str(time.time())
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
        "Timestamp": ts,
        "SIGN": _gate_sign(secret, "GET", path, ts),
    }
    resp = requests.get(f"{GATE_HOST.rstrip('/')}{path}", headers=headers, timeout=REQUEST_TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200 or not isinstance(body, list):
        return {"errno": -1, "errmsg": f"HTTP {resp.status_code}", "result": body}
    return {"errno": 0, "result": body}


def _binance_sign(secret: str, data: Dict[str, Any]) -> str:
    params = sorted((k, v) for k, v in data.items() if k != "signature")
    qs = "&".join(f"{k}={v}" for k, v in params)
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def fetch_binance(api_key: str, secret: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {"timestamp": int(time.time() * 1000)}
    data["signature"] = _binance_sign(secret, data)
    ordered = sorted(data.items(), key=itemgetter(0))
    # signature must be last for some clients; rebuild with signature at end
    ordered = [(k, v) for k, v in ordered if k != "signature"] + [("signature", data["signature"])]
    qs = "&".join(f"{k}={v}" for k, v in ordered)
    headers = {"Accept": "application/json", "X-MBX-APIKEY": api_key}
    resp = requests.get(f"{BINANCE_HOST.rstrip('/')}/api/v3/account?{qs}",
                        headers=headers, timeout=REQUEST_TIMEOUT)
    try:
        body = resp.json()
    except Exception:
        return {"errno": -1, "errmsg": resp.text}
    if resp.status_code != 200 or not isinstance(body, dict) or "code" in body:
        return {"errno": -1, "errmsg": body.get("msg", f"HTTP {resp.status_code}") if isinstance(body, dict) else str(body)}
    bals = body.get("balances")
    if not isinstance(bals, list):
        return {"errno": -1, "errmsg": "unexpected response"}
    return {"errno": 0, "result": bals}


def _pick_currency(rows: List[Dict[str, Any]], currency: str, avail_key: str, frozen_key: str, cur_key: str) -> Optional[Tuple[float, float, float]]:
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
    """返回展示行列表；余额为 0 的跳过。"""
    lines: List[str] = []
    cur = currency.upper()

    for meta in ACCOUNTS.values():
        label = str(meta["uid"])
        try:
            payload = fetch_websea(meta["token"], meta["sk"])
            if payload.get("errno") != 0:
                lines.append(f"{label}: 查询失败 {payload.get('errmsg', 'unknown')}")
                continue
            hit = _pick_currency(payload.get("result") or [], cur, "available", "frozen", "currency")
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
            hit = _pick_currency(payload.get("result") or [], cur, "available", "locked", "currency")
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
            hit = _pick_currency(payload.get("result") or [], cur, "free", "locked", "asset")
            if hit:
                a, f, t = hit
                lines.append(f"binance: 可用={a:.8f} 冻结={f:.8f} 合计={t:.8f}")
    except requests.RequestException as e:
        lines.append(f"binance: 请求异常 {e}")

    return lines


def parse_balance_coin(text: str) -> Optional[str]:
    m = _BALANCE_RE.search(text.strip())
    if not m:
        return None
    return (m.group("c1") or m.group("c2") or "").upper() or None


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if not message or not message.text:
        return
    chat_id = message.chat_id
    if ALLOWED_CHAT_ID is not None and chat_id != ALLOWED_CHAT_ID:
        return

    coin = parse_balance_coin(message.text)
    if not coin:
        return

    lock = _CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())
    if lock.locked():
        await message.reply_text("当前群已有查询在执行，请稍后再试。")
        return

    await message.reply_text(f"正在查询各账户 {coin} 余额...")
    async with lock:
        lines = await asyncio.to_thread(query_coin_balances, coin)
        if not lines:
            await message.reply_text(f"各账户 {coin} 余额均为 0")
            return
        # 若全是失败行也原样返回
        body = f"{coin} 余额（非零）\n" + "\n".join(lines)
        # Telegram 单条上限约 4096
        if len(body) <= 3900:
            await message.reply_text(body)
        else:
            chunk: List[str] = [f"{coin} 余额（非零）"]
            for line in lines:
                chunk.append(line)
                if sum(len(x) + 1 for x in chunk) > 3800:
                    await message.reply_text("\n".join(chunk))
                    chunk = [f"{coin} 余额（续）"]
            if len(chunk) > 1:
                await message.reply_text("\n".join(chunk))


def main() -> None:
    token = BOT_TOKEN.strip()
    if not token:
        raise RuntimeError("请设置 BOT_TOKEN 或环境变量 TG_SPOT_BALANCE_BOT_TOKEN（需与用户分析 bot 不同）")
    app = Application.builder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    LOG.info("tg_spot_balance_bot 运行中 allowed_chat_id=%s", ALLOWED_CHAT_ID)
    app.run_polling()


if __name__ == "__main__":
    main()

