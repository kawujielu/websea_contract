# -*- coding: utf-8 -*-
"""做市账户持仓合计：净持仓金额绝对值>N(默认100万)的交易对；并汇总普通用户持仓；推送 Telegram。

crontab（北京时间每天 09:00）:
  0 9 * * * /home/ubuntu/miniconda3/envs/strategy_base/bin/python3 /path/to/mm_hold_list_amt.py >> /tmp/mm_hold_list_amt.log 2>&1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, "/home/ubuntu/strategy_base")
sys.path.insert(0, "/home/ubuntu/strategy_base/strategy")

from ToolBoxNew import ToolBox  # noqa: E402

TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-5223829567"
TG_CHUNK = 3500
BJT = ZoneInfo("Asia/Shanghai")


async def fetch_holds(rest, **kwargs) -> list:
    """分页拉取 fetch_hold_list（与 adl_for_symbol 相同翻页方式）。"""
    rows, page = [], 1
    while True:
        data = await rest.fetch_hold_list(page=page, page_size=100, **kwargs)
        rows.extend(data.get("data") or [])
        total = int(data.get("pager", {}).get("total_page") or 1)
        if page >= total:
            break
        page += 1
        await asyncio.sleep(0.3)
    return rows


def _amt_side(i: dict, symbol_unit: dict) -> tuple[str, float, int]:
    symbol = i.get("symbol") or ""
    qty = float(i["amount"]) * float(symbol_unit.get(symbol, 1))
    amt = qty * float(i["mark_price"])
    side = 0 if int(i["openDirection"]) == 1 else 1
    return symbol, amt, side


def send_tg(text: str) -> None:
    if not text:
        return
    url = "https://api.telegram.org/bot{}/sendMessage".format(TG_BOT_TOKEN)
    buf = ""
    for line in text.split("\n"):
        piece = line + "\n"
        if len(buf) + len(piece) > TG_CHUNK and buf:
            _post_tg(url, buf.rstrip())
            buf = ""
        buf += piece
    if buf.strip():
        _post_tg(url, buf.rstrip())


def _post_tg(url: str, text: str) -> None:
    data = urllib.parse.urlencode({"chat_id": TG_CHAT_ID, "text": text}).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=30) as r:
            body = json.loads(r.read().decode())
        if not body.get("ok"):
            print("TG发送失败: {}".format(body))
    except Exception as e:
        print("TG发送失败: {}".format(e))


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("-N", "--min-abs-net", type=float, default=1_000_000, help="净持仓金额绝对值阈值(USDT)")
    p.add_argument("--no-telegram", action="store_true", help="仅打印，不发 Telegram")
    args = p.parse_args()
    thr = float(args.min_abs_net)

    tb = ToolBox()
    rest = tb.rest
    rest.DEBUG = False
    accounts = {str(k): v for k, v in tb.load_account().items() if k != "risk"}
    mm_uids = set(accounts.keys())
    symbol_unit = await tb.get_symbol_unit()

    total = defaultdict(lambda: [0.0, 0.0])
    by_uid = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))

    for uid in accounts:
        try:
            holds = await fetch_holds(rest, user_id=uid)
        except Exception as e:
            print(f"账户 {uid} 查询失败: {e}")
            continue
        for i in holds:
            symbol, amt, side = _amt_side(i, symbol_unit)
            if not symbol:
                continue
            total[symbol][side] += amt
            by_uid[symbol][uid][side] += amt

    rows = []
    for symbol, (long_a, short_a) in total.items():
        net = long_a - short_a
        if abs(net) <= thr:
            continue
        rows.append((symbol, long_a, short_a, net))
    rows.sort(key=lambda x: x[3], reverse=True)

    # 超阈值交易对：按 symbol 拉全量持仓，排除做市账户后汇总普通用户
    retail = {}
    for symbol, _, _, _ in rows:
        try:
            holds = await fetch_holds(rest, symbol=symbol)
        except Exception as e:
            print(f"{symbol} 普通用户持仓查询失败: {e}")
            retail[symbol] = (0.0, 0.0, 0.0)
            continue
        long_a = short_a = 0.0
        for i in holds:
            uid = str(i.get("user_id") or i.get("user_name") or "")
            if uid in mm_uids:
                continue
            _, amt, side = _amt_side(i, symbol_unit)
            if side == 0:
                long_a += amt
            else:
                short_a += amt
        retail[symbol] = (long_a, short_a, long_a - short_a)

    now = datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "做市账户合计持仓 {}".format(now),
        "阈值=净持仓金额绝对值>{} USDT | 按净持仓降序".format(thr),
    ]
    if not rows:
        lines.append("无符合条件的交易对")
    else:
        for symbol, long_a, short_a, net in rows:
            rl, rs, rn = retail.get(symbol, (0.0, 0.0, 0.0))
            lines.append("")
            lines.append(
                "{}  做市多={:.1f}  做市空={:.1f}  做市净={:.1f}".format(
                    symbol, long_a, short_a, net
                )
            )
            lines.append(
                "  普通用户合计  多={:.1f}  空={:.1f}  净={:.1f}".format(rl, rs, rn)
            )
            acc_rows = []
            for uid, (ul, us) in by_uid[symbol].items():
                unet = ul - us
                if abs(ul) < 1e-8 and abs(us) < 1e-8:
                    continue
                acc_rows.append((uid, ul, us, unet))
            acc_rows.sort(key=lambda x: abs(x[3]), reverse=True)
            for uid, ul, us, unet in acc_rows:
                lines.append(
                    "  做市账户{}  多={:.1f}  空={:.1f}  净={:.1f}".format(
                        uid, ul, us, unet
                    )
                )

    report = "\n".join(lines)
    print(report)
    if not args.no_telegram:
        # tg_text = ("@XQ9527\n" + report) if rows else report
        tg_text = report
        send_tg(tg_text)
        print("已发送 Telegram chat_id={}".format(TG_CHAT_ID))


if __name__ == "__main__":
    asyncio.run(main())

