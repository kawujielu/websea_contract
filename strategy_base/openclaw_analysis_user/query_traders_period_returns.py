#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指定带单员：近1周/1月 整体收益、收益率、成交金额；结果推送到 Telegram。
数据源：mongo_daily_deals 日切 PKL（不查库）。

用法:
  python3 query_traders_period_returns.py
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from mongdb_order_stats import calc_trade_stats
from pkl_target_users_trade_stats import (
    ensure_user_id,
    filter_bjt_date_range,
    filter_target_users_raw,
    list_pkl_paths,
    load_concat_pkls,
    pkl_legs_to_calc_frame,
)

BJT = ZoneInfo("Asia/Shanghai")
DATA_DIR = "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl"

# 同 ToolBoxNew.send_tg
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-1002696845780"

# 交易员 | 昵称
TRADERS = [
    (631406, "帕卡龙龙"),
    (631401, "无限"),
    (604525, "Dark Whale"),
    (534407, "空军信仰者v2"),
    (1490833, "双核印钞机"),
    (646811, "阿辉来了"),
    (552724, "Shark killer"),
    (631214, "西瓜妹妹"),
    (646802, "梦回西游"),
    (646804, "烟雨江南"),
    (1490166, "OutMan"),
]
WINDOWS = [("1周", 7), ("1月", 30)]
# 可选：user_id -> 账户权益；有则收益率=收益/权益，否则=收益/成交金额
EQUITY: dict[str, float] = {}


def calc_ret(pnl: float, amount: float, equity: float | None):
    if equity and equity > 0:
        return pnl / equity
    if amount:
        return pnl / amount
    return None


def send_tg(text: str):
    """按行累加，超 4000 字分片发送（同 ToolBoxNew.send_tg）。"""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    buf = ""
    for line in text.split("\n"):
        buf += line + "\n"
        if len(buf) > 4000:
            requests.post(url, data={"chat_id": TG_CHAT_ID, "text": buf}, timeout=30)
            buf = ""
    if buf.strip():
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": buf}, timeout=30)


def main():
    end_day = datetime.now(BJT).date()
    start_need = end_day - timedelta(days=29)
    uids = [str(u) for u, _ in TRADERS]

    paths = [
        p for p in list_pkl_paths(DATA_DIR)
        if start_need <= datetime.strptime(Path(p).stem, "%Y-%m-%d").date() <= end_day
    ]
    lines = [
        f"带单员收益统计 {end_day}",
        f"{'交易员':<10}{'昵称':<14}{'窗口':<4}  {'收益':>12}  {'成交金额':>14}  {'收益率':>10}",
        "-" * 70,
    ]

    raw = load_concat_pkls(paths)
    raw = filter_target_users_raw(raw, uids) if raw.shape[0] else raw
    legs = ensure_user_id(raw) if raw.shape[0] else raw
    if legs.shape[0]:
        legs = legs[legs["user_id"].isin(uids)].copy()

    for uid, nick in TRADERS:
        sid = str(uid)
        eq = EQUITY.get(sid)
        sub_all = legs[legs["user_id"] == sid] if legs.shape[0] else legs
        for label, days in WINDOWS:
            start_d = end_day - timedelta(days=days - 1)
            sub = filter_bjt_date_range(sub_all, start_d, end_day) if sub_all.shape[0] else sub_all
            if sub.shape[0] == 0:
                pnl = amount = 0.0
            else:
                ss = calc_trade_stats(pkl_legs_to_calc_frame(sub), userid=sid)
                pnl = float(ss.get("总盈亏") or 0)
                amount = float(ss.get("总成交金额") or 0)
            amount_i = int(amount)
            ret = calc_ret(pnl, amount, eq)
            lines.append(
                f"{sid:<10}{nick:<14}{label:<4}  {pnl:>12.2f}  {amount_i:>14d}  "
                f"{(f'{ret*100:.2f}%' if ret is not None else '-'):>10}"
            )

    msg = "\n".join(lines)
    print(msg)
    try:
        send_tg(msg)
        print(f"\n已发送到 TG chat_id={TG_CHAT_ID}")
    except Exception as e:
        print(f"\nTG 发送失败: {e}")


if __name__ == "__main__":
    main()

