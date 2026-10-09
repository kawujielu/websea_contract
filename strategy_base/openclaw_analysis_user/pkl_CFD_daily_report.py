# -*- coding: utf-8 -*-
"""读取昨日 PKL，统计 500-USDT 合约单边成交金额、成交量 TopN 及盈亏 TopN。"""
from __future__ import annotations

import argparse
import os
import re
from datetime import date, datetime, timedelta
from typing import List, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from exclude_user_ids import get_excluded_user_ids_sync
from pkl_user_query_analyzer import DEFAULT_DATA_DIR, ensure_user_id, load_day_df

# ========== 参数 ==========
TOP_N = 5
SYMBOL_REGEX = r"500[-_]?USDT"  # 交易对名含 500USDT（忽略大小写，允许中间 -/_）
BUY_SIDES = {"开多", "平空", "1", "4"}  # 开多/平空=买
SELL_SIDES = {"开空", "平多", "2", "3"}  # 开空/平多=卖
EXCLUDED_UIDS = {"1496960"}  # 额外不纳入统计
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-5223829567"
DEAL_KEYS = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
# ==========================

BJT = ZoneInfo("Asia/Shanghai")
SYMBOL_RE = re.compile(SYMBOL_REGEX, re.I)


def parse_args():
    p = argparse.ArgumentParser(description="昨日 500-USDT PKL 单边成交 TopN / 盈亏 TopN")
    p.add_argument("--data-dir", type=str, default=DEFAULT_DATA_DIR)
    p.add_argument("--day", type=str, default="", help="YYYY-MM-DD，默认北京时间昨天")
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--no-tg", action="store_true", help="只打印，不发 Telegram")
    args, _ = p.parse_known_args()
    return args


def pick_stat_day(day_text: str) -> date:
    if day_text:
        return datetime.strptime(day_text, "%Y-%m-%d").date()
    return datetime.now(BJT).date() - timedelta(days=1)


def excluded_uids() -> Set[str]:
    out = set(EXCLUDED_UIDS)
    try:
        out |= {str(x) for x in get_excluded_user_ids_sync()}
    except Exception:
        pass
    return out


def filter_500usdt(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.shape[0] == 0:
        return pd.DataFrame()
    d = df.copy()
    d["symbol"] = d["symbol"].astype(str)
    return d[d["symbol"].str.contains(SYMBOL_RE, na=False)].copy()


def _notional(d: pd.DataFrame) -> pd.Series:
    pr = pd.to_numeric(d["price"], errors="coerce").fillna(0.0)
    amt = pd.to_numeric(d["amount"], errors="coerce").fillna(0.0)
    if "face_value" in d.columns:
        fv = pd.to_numeric(d["face_value"], errors="coerce").fillna(1.0)
    else:
        fv = pd.Series(1.0, index=d.index)
    fv = fv.mask(fv <= 0, 1.0)
    return pr * amt * fv


def onesided_market_notional(df: pd.DataFrame) -> float:
    """每笔成交只计一次名义金额（单边）。"""
    if df is None or df.shape[0] == 0:
        return 0.0
    keys = [c for c in DEAL_KEYS if c in df.columns]
    u = df.drop_duplicates(keys) if keys else df
    return float(_notional(u).sum())


def user_day_stats(df: pd.DataFrame) -> pd.DataFrame:
    """按用户汇总单边买/卖金额与盈亏。PKL 每行已是一侧腿，按用户加总即为该用户单边。"""
    cols = ["user_id", "buy", "sell", "total", "pnl"]
    if df is None or df.shape[0] == 0:
        return pd.DataFrame(columns=cols)
    d = df.copy()
    d["user_id"] = d["user_id"].astype(str)
    d["_notional"] = _notional(d)
    d["_pnl"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    side = d["buy_sell"].astype(str)
    d["_buy"] = d["_notional"].where(side.isin(BUY_SIDES), 0.0)
    d["_sell"] = d["_notional"].where(side.isin(SELL_SIDES), 0.0)
    g = d.groupby("user_id", as_index=False).agg(
        buy=("_buy", "sum"),
        sell=("_sell", "sum"),
        pnl=("_pnl", "sum"),
    )
    g["total"] = g["buy"] + g["sell"]
    return g[cols]


def _format_rank_block(
    title: str,
    rows: Sequence[Tuple[str, float, float, float, float]],
    top_n: int,
) -> List[str]:
    lines = [title]
    if not rows:
        lines.append("无成交")
        return lines
    for i, (uid, total, buy, sell, pnl) in enumerate(rows[:top_n], 1):
        lines.append(
            "#{:<2} uid={}  总={:.2f}  买={:.2f}  卖={:.2f}  盈亏={:.2f}".format(
                i, uid, total, buy, sell, pnl
            )
        )
    return lines


def format_report(day: str, stats: pd.DataFrame, market_notional: float, top_n: int) -> str:
    n = max(1, int(top_n))
    user_n = 0 if stats is None else int(stats.shape[0])
    lines = [
        "【{}】CFD合约 单边成交 合计={:.2f}  用户数={}".format(day, market_notional, user_n),
    ]
    if stats is None or stats.shape[0] == 0:
        lines.append("【{}CFD合约 成交金额 Top{}".format(day, n))
        lines.append("无成交")
        lines.append("【{}CFD合约 盈亏 Top{}".format(day, n))
        lines.append("无成交")
        return "\n".join(lines)

    vol_rows = [
        (str(r.user_id), float(r.total), float(r.buy), float(r.sell), float(r.pnl))
        for r in stats.sort_values(["total", "user_id"], ascending=[False, True]).itertuples(index=False)
    ]
    pnl_rows = [
        (str(r.user_id), float(r.total), float(r.buy), float(r.sell), float(r.pnl))
        for r in stats.sort_values(["pnl", "user_id"], ascending=[False, True]).itertuples(index=False)
    ]
    lines.extend(_format_rank_block("【{}】CFD合约 成交金额 Top{}".format(day, n), vol_rows, n))
    lines.extend(_format_rank_block("【{}】CFD合约 盈亏 Top{}".format(day, n), pnl_rows, n))
    return "\n".join(lines)


def send_tg(text: str) -> None:
    url = "https://api.telegram.org/bot{}/sendMessage".format(TG_BOT_TOKEN)
    try:
        r = requests.post(url, data={"chat_id": TG_CHAT_ID, "text": text}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print("TG发送失败: {}".format(e))


def build_day_report(data_dir: str, day: date, top_n: int) -> str:
    day_str = day.strftime("%Y-%m-%d")
    try:
        raw = load_day_df(data_dir, day)
    except FileNotFoundError as e:
        return "【{}】CFD合约 昨日PKL不存在: {}".format(day_str, e)

    d = filter_500usdt(ensure_user_id(raw))
    excl = excluded_uids()
    if excl and d.shape[0] > 0:
        d = d[~d["user_id"].astype(str).isin(excl)].copy()
    stats = user_day_stats(d)
    market = onesided_market_notional(d)
    return format_report(day_str, stats, market, top_n)


def main() -> None:
    args = parse_args()
    day = pick_stat_day(args.day)
    text = build_day_report(args.data_dir, day, args.top_n)
    print(text)
    if not args.no_tg:
        send_tg(text)


if __name__ == "__main__":
    main()

