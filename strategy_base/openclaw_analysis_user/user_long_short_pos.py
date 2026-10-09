# -*- coding: utf-8 -*-
"""
全市场多空持仓汇总（排除模拟金 + 做市账户）

口径参考 user_floating_profit.py：
  1) 模拟金：fetch_tag_list E1~E99 中 tag 含 E 的 user_id；持仓行 tag 含 E 也跳过
  2) 做市：account.config.json 里 websea 非「测试」账户的 uid（不含 risk）

输出五列（持仓金额单位：USDT = 张数×合约乘数×标记价）：
  symbol | 多头持仓(U) | 空头持仓(U) | 同账户多空对锁(U) | 持仓人数

  同账户多空对锁 = Σ_uid min(该用户该交易对多头金额, 空头金额)
  持仓人数 = 该交易对去重后的持仓用户数（同一用户多空只计 1 人）

运行:
  python market_long_short_positions.py
  python market_long_short_positions.py --no-telegram

环境变量:
  WEBSEA_RISK_TOKEN / WEBSEA_RISK_SECRET
  WEBSEA_ACCOUNT_JSON — 默认 /home/ubuntu/CCGo/resources/account.config.json
  MM_EXTRA_UIDS — 额外排除 uid，逗号分隔
  MIN_TOTAL_NOTIONAL_U — Telegram 仅推送 多头+空头 大于该值的交易对，默认 10000
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Any, DefaultDict, Dict, List, Set, Tuple

from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

_HERE = os.path.dirname(os.path.abspath(__file__))
# 兼容：本机 websea 根目录 / 服务器 strategy_base（含 crypto_center、utils）
_CANDIDATE_ROOTS = [
    os.path.abspath(os.path.join(_HERE, "..", "..", "..", "..")),  # .../websea
    os.path.abspath(os.path.join(_HERE, "..", "..", "..", "..", "合约项目", "strategy_base")),
    os.environ.get("WEBSEA_REPO_ROOT", ""),
    "/home/ubuntu/strategy_base",
    "/home/ubuntu/CCGo",
    os.path.abspath(os.path.join(_HERE, "..", "..")),  # 与 user_floating_profit 同级探测
]
for _root in _CANDIDATE_ROOTS:
    if not _root:
        continue
    if _root not in sys.path and (
        os.path.isdir(os.path.join(_root, "crypto_center"))
        or os.path.isdir(os.path.join(_root, "utils"))
    ):
        sys.path.insert(0, _root)

from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # noqa: E402
from utils import restclient as rc  # noqa: E402

DEFAULT_ACCOUNT_JSON = os.environ.get(
    "WEBSEA_ACCOUNT_JSON",
    "/home/ubuntu/CCGo/resources/account.config.json",
)
DEFAULT_TOKEN = os.environ.get("WEBSEA_RISK_TOKEN", "c1cf4185b2bed317aeb6e6674491fbef")
DEFAULT_SECRET = os.environ.get("WEBSEA_RISK_SECRET", "")
DEFAULT_MIN_TOTAL_U = float(os.environ.get("MIN_TOTAL_NOTIONAL_U", "10000"))

# 与 user_floating_profit.py 同群
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID_STR = "-5223829567"


def load_mm_uids_from_account_json(path: str) -> Set[str]:
    """与 user_floating_profit / ToolBoxNew.load_account 相同解析。"""
    uids: Set[str] = set()
    if not path or not os.path.isfile(path):
        return uids
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    websea = data.get("websea") or {}
    for _k, v in websea.items():
        desc = v.get("description") or ""
        if "测试" in desc:
            continue
        raw = v.get("uid") or ""
        uid = raw.split(",")[0].strip() if "," in raw else str(raw).strip()
        if not uid or uid == "risk":
            continue
        uids.add(str(uid))
    extra = os.environ.get("MM_EXTRA_UIDS", "").strip()
    if extra:
        for x in extra.split(","):
            x = x.strip()
            if x:
                uids.add(x)
    return uids


async def collect_sim_uids(rest: Contract) -> Set[str]:
    temp_tag_list: List[Any] = []
    for num in range(1, 100):
        last_page = 0
        tag_list_page = 1
        while True:
            try:
                if last_page == 0:
                    data = await rest.fetch_tag_list(tag=f"E{num}", page=1, page_size=1000)
                    tag_list_page = int(data["pager"]["total_page"])
                    for row in data["data"]:
                        if row not in temp_tag_list:
                            temp_tag_list.append(row)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, tag_list_page + 1):
                    last_page = n
                    data = await rest.fetch_tag_list(tag=f"E{num}", page=n, page_size=1000)
                    for row in data["data"]:
                        if row not in temp_tag_list:
                            temp_tag_list.append(row)
                    await asyncio.sleep(0.5)
                break
            except Exception:
                print(traceback.format_exc())
                await asyncio.sleep(5)

    out: Set[str] = set()
    for row in temp_tag_list:
        tag = str(row.get("tag") or "")
        if "E" in tag:
            out.add(str(row.get("user_id")))
    return out


async def fetch_all_hold_list(rest: Contract, page_size: int = 100) -> List[dict]:
    rows: List[dict] = []
    last_page = 0
    while True:
        try:
            if last_page == 0:
                data = await rest.fetch_hold_list(page_size=page_size)
                total_page = int(data["pager"]["total_page"])
                for i in data["data"]:
                    rows.append(i)
            begin = 2 if last_page == 0 else last_page
            for n in range(begin, total_page + 1):
                last_page = n
                data = await rest.fetch_hold_list(page=n, page_size=page_size)
                for i in data["data"]:
                    rows.append(i)
                await asyncio.sleep(0.5)
            break
        except Exception:
            print(traceback.format_exc())
            await asyncio.sleep(5)
    return rows


def aggregate_long_short(
    holds: List[dict],
    excluded: Set[str],
    symbol_unit: Dict[str, float],
) -> Tuple[List[Tuple[str, float, float, float, int]], int, int]:
    """
    Returns:
      rows: [(symbol, long_u, short_u, dual_min_u, holders), ...] 按 多+空 金额降序
      kept, skipped
    """
    # symbol -> [long_u, short_u]
    per_sym: DefaultDict[str, List[float]] = defaultdict(lambda: [0.0, 0.0])
    # (uid, symbol) -> [long_u, short_u]
    per_user_sym: DefaultDict[Tuple[str, str], List[float]] = defaultdict(
        lambda: [0.0, 0.0]
    )
    holders: DefaultDict[str, Set[str]] = defaultdict(set)
    kept = 0
    skipped = 0

    for row in holds:
        uid = str(row.get("user_id", ""))
        tag = str(row.get("tag") or "")
        if uid in excluded or "E" in tag:
            skipped += 1
            continue

        symbol = row.get("symbol")
        if not symbol:
            skipped += 1
            continue

        try:
            amt = float(row.get("amount", 0))
            od = int(row.get("openDirection", 0))
        except (TypeError, ValueError):
            skipped += 1
            continue

        if amt <= 0 or od not in (1, 2):
            skipped += 1
            continue

        try:
            mark = float(row.get("mark_price", 0))
        except (TypeError, ValueError):
            mark = 0.0
        csize = float(symbol_unit.get(symbol, 1.0))
        notional = abs(amt * csize * mark)

        if od == 1:
            per_sym[symbol][0] += notional
            per_user_sym[(uid, symbol)][0] += notional
        else:
            per_sym[symbol][1] += notional
            per_user_sym[(uid, symbol)][1] += notional
        holders[symbol].add(uid)
        kept += 1

    dual_by_sym: DefaultDict[str, float] = defaultdict(float)
    for (_uid, symbol), sides in per_user_sym.items():
        long_u, short_u = sides[0], sides[1]
        if long_u > 0 and short_u > 0:
            dual_by_sym[symbol] += min(long_u, short_u)

    rows: List[Tuple[str, float, float, float, int]] = []
    for sym, pair in per_sym.items():
        long_u, short_u = pair[0], pair[1]
        rows.append(
            (
                sym,
                long_u,
                short_u,
                dual_by_sym.get(sym, 0.0),
                len(holders.get(sym, set())),
            )
        )
    rows.sort(key=lambda x: x[1] + x[2], reverse=True)
    return rows, kept, skipped


def format_table(
    when: datetime,
    rows: List[Tuple[str, float, float, float, int]],
    kept: int,
    skipped: int,
    mm_cnt: int,
    sim_cnt: int,
    min_total_u: float,
    n_sym_all: int,
) -> str:
    lines = [
        "======== 全市场多空持仓（已排除模拟金 + 做市） ========",
        "时间(北京): {}".format(when.strftime("%Y-%m-%d %H:%M:%S")),
        "金额(U)=张数×合约乘数×标记价；同账户多空对锁=Σ min(该户该币多头金额, 空头金额)",
        "筛选: 仅展示 多头+空头 > {:.0f} U 的交易对（全量有持仓交易对数={}，本表行数={}）".format(
            min_total_u, n_sym_all, len(rows)
        ),
        "本快照: 保留持仓条数={} 跳过={} | 排除做市uid={} 模拟金uid={}".format(
            kept, skipped, mm_cnt, sim_cnt
        ),
        "",
        "{:16} {:>16} {:>16} {:>16} {:>8}".format(
            "symbol", "多头持仓(U)", "空头持仓(U)", "同账户多空对锁(U)", "持仓人数"
        ),
        "-" * 78,
    ]
    if not rows:
        lines.append("（无符合条件的交易对）")
    else:
        for sym, long_u, short_u, dual, n_holders in rows:
            lines.append(
                "{:16} {:>16,.2f} {:>16,.2f} {:>16,.2f} {:>8d}".format(
                    sym, long_u, short_u, dual, n_holders
                )
            )
    return "\n".join(lines)


async def send_report_telegram(
    client: rc.RestClient,
    token: str,
    chat_id: int,
    text: str,
) -> None:
    """按行累积，单条不超过 Telegram 约 4096 限制（与 user_floating_profit 一致）。"""
    send_text = ""
    for t in text.split("\n"):
        send_text += t + "\n"
        if len(send_text) > 4000:
            await client.tg_warning(token=token, chat_id=chat_id, content=send_text)
            send_text = ""
    if send_text:
        await client.tg_warning(token=token, chat_id=chat_id, content=send_text)


async def async_main(
    min_total_u: float,
    tg_token: str,
    tg_chat_id: int | None,
    no_telegram: bool,
) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    rest = Contract(DEFAULT_TOKEN, DEFAULT_SECRET, dev=False)
    rest.DEBUG = False
    rest.rest_timeout = 120

    mm_uids = load_mm_uids_from_account_json(DEFAULT_ACCOUNT_JSON)
    if not mm_uids and not os.path.isfile(DEFAULT_ACCOUNT_JSON):
        print(
            "警告: 未找到 account.config.json（{}），做市账户仅依赖 MM_EXTRA_UIDS".format(
                DEFAULT_ACCOUNT_JSON
            ),
            flush=True,
        )

    sim_uids: Set[str] = set()
    try:
        sim_uids = await collect_sim_uids(rest)
    except Exception:
        print("拉取模拟金用户失败，将仅按持仓行 tag 过滤 E 组", flush=True)
        print(traceback.format_exc(), flush=True)

    tg_client: rc.RestClient | None = None
    if not no_telegram and tg_token and tg_chat_id is not None:
        tg_client = rc.RestClient()
    elif not no_telegram and (not tg_token or tg_chat_id is None):
        print("提示: Telegram token 或 chat_id 为空，跳过群推送", flush=True)

    excluded = set(mm_uids) | set(sim_uids)

    try:
        res = await rest.fetch_symbol_info()
        symbol_unit = {s: float(v["contractSize"]) for s, v in res.items()}
    except Exception:
        print(traceback.format_exc(), flush=True)
        symbol_unit = {}

    holds = await fetch_all_hold_list(rest)
    all_rows, kept, skipped = aggregate_long_short(holds, excluded, symbol_unit)
    tg_rows = [r for r in all_rows if (r[1] + r[2]) > float(min_total_u)]

    text = format_table(
        datetime.now(BJT),
        tg_rows,
        kept,
        skipped,
        len(mm_uids),
        len(sim_uids),
        min_total_u,
        len(all_rows),
    )
    print(text, flush=True)

    if tg_client is not None and tg_token and tg_chat_id is not None:
        try:
            await send_report_telegram(tg_client, tg_token, tg_chat_id, text)
            print("Telegram 推送完成（{} 个交易对）".format(len(tg_rows)), flush=True)
        except Exception:
            print("Telegram 推送失败:\n{}".format(traceback.format_exc()), flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="全市场多空持仓汇总并推送 Telegram（排除模拟金/做市）"
    )
    p.add_argument(
        "--min-total",
        type=float,
        default=DEFAULT_MIN_TOTAL_U,
        help="仅推送/展示 多头+空头 大于该值(U) 的交易对，默认 10000",
    )
    p.add_argument(
        "--tg-token",
        type=str,
        default=TG_BOT_TOKEN,
        help="Telegram Bot Token（默认脚本内写死，可覆盖）",
    )
    p.add_argument(
        "--tg-chat-id",
        type=str,
        default=TG_CHAT_ID_STR,
        help="Telegram 群 chat_id（默认脚本内写死，可覆盖）",
    )
    p.add_argument(
        "--no-telegram",
        action="store_true",
        help="不发送 Telegram",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    raw_chat = (args.tg_chat_id or "").strip()
    chat_id: int | None = None
    if raw_chat:
        try:
            chat_id = int(raw_chat)
        except ValueError:
            sys.stderr.write("无效的 --tg-chat-id: {}\n".format(raw_chat))
            raise SystemExit(2)
    asyncio.run(
        async_main(
            args.min_total,
            (args.tg_token or "").strip(),
            chat_id,
            args.no_telegram,
        )
    )


if __name__ == "__main__":
    main()
