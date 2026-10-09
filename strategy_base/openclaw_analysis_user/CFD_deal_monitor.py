# -*- coding: utf-8 -*-
"""每10分钟拉取名称含 500USDT 的合约成交，按北京自然日累计用户买卖金额 Top5（每日重置）。"""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from pymongo import MongoClient

# ========== 参数 ==========
MONGO_URI = "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/"
INTERVAL_SEC = 600  # 每10分钟；同时查询最近10分钟
TOP_N = 5
SYMBOL_REGEX = "500[-_]?USDT"  # 交易对名含 500USDT（忽略大小写，允许中间 -/_）
BUY_SIDES = {"1", "4"}  # 开多/平空=买
SELL_SIDES = {"2", "3"}  # 开空/平多=卖
EXCLUDED_UIDS = {"1496960"}  # 不纳入买卖金额/盈亏统计
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-5386797236"
# ==========================

BJT = ZoneInfo("Asia/Shanghai")
# uid -> [buy, sell, pnl]（仅当日，日切清空）
DayStats = DefaultDict[str, list]


def fetch_deals(ts_min: int, ts_max: int) -> list:
    client = MongoClient(MONGO_URI)
    try:
        col = client.exchange.real_contract_deal
        q = {
            "ts": {"$gte": int(ts_min), "$lte": int(ts_max)},
            "symbol": {"$regex": SYMBOL_REGEX, "$options": "i"},
        }
        proj = {
            "_id": 1, "ts": 1, "symbol": 1, "price": 1, "amount": 1,
            "takerUser": 1, "makerUser": 1,
            "takerBuyOrSell": 1, "makerBuyOrSell": 1,
            "takerFaceValue": 1, "makerFaceValue": 1,
            "takerProfitLoss": 1, "makerProfitLoss": 1,
        }
        return list(col.find(q, projection=proj).batch_size(2000))
    finally:
        client.close()


def _add(stats: DayStats, uid: str, side: str, notional: float, pnl: float) -> None:
    if not uid or uid in EXCLUDED_UIDS:
        return
    slot = stats[uid]
    if notional > 0:
        if side in BUY_SIDES:
            slot[0] += notional
        elif side in SELL_SIDES:
            slot[1] += notional
    slot[2] += pnl


def apply_deals(stats: DayStats, deals: list, seen: Set[str], day: str) -> int:
    """只累计 day 当日成交；跨日窗口内的昨日成交忽略。"""
    n = 0
    for d in deals:
        oid = str(d.get("_id") or "")
        if not oid or oid in seen:
            continue
        ts = int(d["ts"])
        if datetime.fromtimestamp(ts, tz=BJT).strftime("%Y-%m-%d") != day:
            continue
        seen.add(oid)
        n += 1
        px = float(d.get("price") or 0)
        amt = float(d.get("amount") or 0)
        _add(
            stats, str(d.get("takerUser") or ""),
            str(d.get("takerBuyOrSell") or ""),
            px * amt * float(d.get("takerFaceValue") or 1),
            float(d.get("takerProfitLoss") or 0),
        )
        _add(
            stats, str(d.get("makerUser") or ""),
            str(d.get("makerBuyOrSell") or ""),
            px * amt * float(d.get("makerFaceValue") or 1),
            float(d.get("makerProfitLoss") or 0),
        )
    return n


def format_top(stats: DayStats, day: str, title: str = "") -> str:
    rows: list[Tuple[float, float, float, float, str]] = []
    for uid, (buy, sell, pnl) in stats.items():
        rows.append((buy + sell, buy, sell, pnl, uid))
    rows.sort(key=lambda x: x[0], reverse=True)
    lines = [title or "【{}】500USDT 成交金额 Top{}".format(day, TOP_N)]
    if not rows:
        lines.append("无成交")
        return "\n".join(lines)
    for i, (total, buy, sell, pnl, uid) in enumerate(rows[:TOP_N], 1):
        lines.append(
            "#{:<2} uid={}  总={:.2f}  买={:.2f}  卖={:.2f}  盈亏={:.2f}".format(
                i, uid, total, buy, sell, pnl
            )
        )
    return "\n".join(lines)


def send_tg(text: str) -> None:
    url = "https://api.telegram.org/bot{}/sendMessage".format(TG_BOT_TOKEN)
    try:
        r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print("TG发送失败: {}".format(e))


def report(stats: DayStats, day: str, title: str = "") -> None:
    text = format_top(stats, day, title)
    print(text)
    send_tg(text)


def main() -> None:
    stats: DayStats = defaultdict(lambda: [0.0, 0.0, 0.0])
    seen: Set[str] = set()
    last_day = datetime.now(BJT).strftime("%Y-%m-%d")
    print(
        "启动: 每{}s 查近{}s、symbol~/{}/i、按北京日累计 Top{}（每日重置）".format(
            INTERVAL_SEC, INTERVAL_SEC, SYMBOL_REGEX, TOP_N
        )
    )
    while True:
        t0 = time.time()
        now = int(t0)
        today = datetime.now(BJT).strftime("%Y-%m-%d")
        if today != last_day:
            report(stats, last_day, "【日切结算 {}】Top{}".format(last_day, TOP_N))
            stats.clear()
            seen.clear()
            last_day = today
            print("【{}】成交量已重置".format(today))

        try:
            deals = fetch_deals(now - INTERVAL_SEC, now)
            n = apply_deals(stats, deals, seen, today)
            print(
                "\n{}  窗口成交={}  新增={}  当日用户={}".format(
                    datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S"),
                    len(deals), n, len(stats),
                )
            )
            report(stats, today)
        except Exception as e:
            print("{} 查询失败: {}".format(datetime.now(BJT).strftime("%H:%M:%S"), e))

        sleep = INTERVAL_SEC - (time.time() - t0)
        time.sleep(max(1.0, sleep))


if __name__ == "__main__":
    main()

