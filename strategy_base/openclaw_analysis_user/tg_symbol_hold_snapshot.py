# -*- coding: utf-8 -*-
"""
单合约「当前持仓」快照（供 Telegram 机器人 subprocess 调用）

用途：
  拉取全市场 fetch_hold_list，剔除做市账户与模拟金（与 user_floating_profit.run_once 一致），
  对指定合约汇总：净名义 USDT（含方向）、多空名义合计、多空持仓人数、多空人均持仓名义、多空名义 TopN 用户及浮动盈亏。

口径（与 user_floating_profit.py 一致）：
  单行名义 USDT = (方向×张数) × 合约乘数(contractSize) × 标记价(mark_price)
  openDirection: 1=多，2=空；浮动盈亏取 profit_loss / profitLoss。

环境变量（与 user_floating_profit 对齐）：
  WEBSEA_RISK_TOKEN / WEBSEA_RISK_SECRET — REST 鉴权
  WEBSEA_ACCOUNT_JSON — account.config.json，默认 /home/ubuntu/CCGo/resources/account.config.json
  MM_EXTRA_UIDS — 额外排除 uid，逗号分隔

依赖：Python 3.9.19+；运行目录需能 import crypto_center（与 user_floating_profit 相同 sys.path 策略）。

调用示例：
  python tg_symbol_hold_snapshot.py --symbol BTC-USDT
  python tg_symbol_hold_snapshot.py --symbol BTCUSDT --top 3

输出：仅 stdout 为报告正文；stderr 为进度/错误。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

_HERE = os.path.dirname(os.path.abspath(__file__))
_STRATEGY_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _STRATEGY_ROOT not in sys.path:
    sys.path.insert(0, _STRATEGY_ROOT)

import user_floating_profit as ufp  # noqa: E402


def _norm_symbol_key(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", (s or "").strip()).upper()


def _resolve_api_symbol(requested: str, candidates: Set[str]) -> Optional[str]:
    key = _norm_symbol_key(requested)
    if not key:
        return None
    for sym in candidates:
        if _norm_symbol_key(sym) == key:
            return sym
    return None


def _fmt_int(x: float) -> str:
    return str(int(round(float(x))))


def _build_report(
    when: datetime,
    api_symbol: str,
    net_u: float,
    long_sum_u: float,
    short_sum_u: float,
    n_long_users: int,
    n_short_users: int,
    top_long: List[Tuple[str, float, float]],
    top_short: List[Tuple[str, float, float]],
) -> str:
    lines = [
        "======== 单合约持仓快照（已排除模拟金+做市） ========",
        "时间(北京时间): {}".format(when.strftime("%Y-%m-%d %H:%M:%S")),
        "合约(symbol): {}".format(api_symbol),
        "",
        "净持仓金额(U，正=净多，负=净空): {}".format(_fmt_int(net_u)),
        "多头名义合计(U): {}".format(_fmt_int(long_sum_u)),
        "空头名义合计(U，按绝对值汇总): {}".format(_fmt_int(short_sum_u)),
        "多头持仓人数: {}".format(n_long_users),
        "空头持仓人数: {}".format(n_short_users),
        "多头人均持仓金额(U): {}".format(
            _fmt_int(long_sum_u / n_long_users) if n_long_users > 0 else "N/A"
        ),
        "空头人均持仓金额(U): {}".format(
            _fmt_int(short_sum_u / n_short_users) if n_short_users > 0 else "N/A"
        ),
        "",
        "多头 Top{} (user_id, 名义U, 浮动盈亏U)".format(len(top_long)),
    ]
    for i, (uid, nu, pu) in enumerate(top_long, start=1):
        lines.append("  {}. {}  {}  {}".format(i, uid, _fmt_int(nu), _fmt_int(pu)))
    lines.append("")
    lines.append("空头 Top{} (user_id, 名义U, 浮动盈亏U)".format(len(top_short)))
    for i, (uid, nu, pu) in enumerate(top_short, start=1):
        lines.append("  {}. {}  {}  {}".format(i, uid, _fmt_int(nu), _fmt_int(pu)))
    lines.append("")
    lines.append(
        "说明: 名义U=张数×合约乘数×标记价；空头侧单用户名义列为正数便于阅读。"
        "人均持仓金额=该侧名义合计÷该侧持仓人数（四舍五入到整数U）。"
    )
    return "\n".join(lines)


async def async_main(symbol_arg: str, top_n: int) -> int:
    rest = ufp.Contract(ufp.DEFAULT_TOKEN, ufp.DEFAULT_SECRET, dev=False)
    rest.DEBUG = False
    rest.rest_timeout = 120

    mm_uids = ufp.load_mm_uids_from_account_json(ufp.DEFAULT_ACCOUNT_JSON)
    try:
        sim_uids = await ufp.collect_sim_uids(rest)
    except Exception:
        sys.stderr.write("拉取模拟金用户失败，将仅按持仓行 tag 过滤 E 组\n")
        sys.stderr.write(traceback.format_exc())
        sim_uids = set()

    excluded: Set[str] = set(mm_uids) | set(sim_uids)

    try:
        res = await rest.fetch_symbol_info()
        symbol_unit: Dict[str, float] = {s: float(v["contractSize"]) for s, v in res.items()}
    except Exception:
        sys.stderr.write(traceback.format_exc())
        symbol_unit = {}

    holds = await ufp.fetch_all_hold_list(rest)
    candidates: Set[str] = set(symbol_unit.keys())
    for row in holds:
        s = row.get("symbol")
        if s:
            candidates.add(str(s))

    api_symbol = _resolve_api_symbol(symbol_arg, candidates)
    if api_symbol is None:
        sys.stderr.write(
            "未找到与「{}」匹配的合约代码（请检查大小写、连字符或与接口 symbol 一致）\n".format(
                symbol_arg
            )
        )
        return 1

    long_n: DefaultDict[str, float] = defaultdict(float)
    long_p: DefaultDict[str, float] = defaultdict(float)
    short_n: DefaultDict[str, float] = defaultdict(float)
    short_p: DefaultDict[str, float] = defaultdict(float)
    net_u = 0.0
    matched_rows = 0
    total_sym_rows = sum(1 for r in holds if str(r.get("symbol", "")) == api_symbol)

    for row in holds:
        if str(row.get("symbol", "")) != api_symbol:
            continue
        uid = str(row.get("user_id", ""))
        tag = str(row.get("tag") or "")
        if uid in excluded or "E" in tag:
            continue
        try:
            amt = float(row.get("amount", 0))
            od = int(row.get("openDirection", 0))
        except (TypeError, ValueError):
            continue
        try:
            mark = float(row.get("mark_price", 0))
        except (TypeError, ValueError):
            mark = 0.0
        csize = float(symbol_unit.get(api_symbol, 1.0))
        signed = amt if od == 1 else (-amt if od == 2 else 0.0)
        n = signed * csize * mark
        pnl = ufp.row_profit_loss(row)
        net_u += n
        matched_rows += 1
        if od == 1 and amt != 0:
            long_n[uid] += n
            long_p[uid] += pnl
        elif od == 2 and amt != 0:
            short_n[uid] += abs(n)
            short_p[uid] += pnl

    if matched_rows == 0:
        if total_sym_rows > 0:
            sys.stderr.write(
                "该合约有 {} 条持仓记录，但均在排除名单或 tag 含 E 被过滤。\n".format(
                    total_sym_rows
                )
            )
        else:
            sys.stderr.write("接口中无该合约持仓行（symbol={}）。\n".format(api_symbol))
        print(
            _build_report(
                datetime.now(BJT),
                api_symbol,
                0.0,
                0.0,
                0.0,
                0,
                0,
                [],
                [],
            ),
            flush=True,
        )
        return 0

    long_sum = sum(long_n.values())
    short_sum = sum(short_n.values())
    n_long = sum(1 for v in long_n.values() if v > 0)
    n_short = sum(1 for v in short_n.values() if v > 0)

    tl = sorted(long_n.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_long: List[Tuple[str, float, float]] = [
        (u, long_n[u], long_p[u]) for u, _ in tl if long_n[u] > 0
    ]
    ts = sorted(short_n.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_short: List[Tuple[str, float, float]] = [
        (u, short_n[u], short_p[u]) for u, _ in ts if short_n[u] > 0
    ]

    text = _build_report(
        datetime.now(BJT),
        api_symbol,
        net_u,
        long_sum,
        short_sum,
        n_long,
        n_short,
        top_long,
        top_short,
    )
    print(text, flush=True)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="单合约持仓快照（stdout 报告）")
    p.add_argument("--symbol", required=True, help="合约代码，如 BTC-USDT / BTCUSDT")
    p.add_argument("--top", type=int, default=3, help="多空各自 TopN，默认 3")
    args, _ = p.parse_known_args()
    return args


def main() -> int:
    args = parse_args()
    top_n = max(1, int(args.top))
    try:
        return asyncio.run(async_main((args.symbol or "").strip(), top_n))
    except KeyboardInterrupt:
        return 130
    except Exception:
        sys.stderr.write(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
