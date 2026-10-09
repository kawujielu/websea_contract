# -*- coding: utf-8 -*-
"""
单次汇总全市场持仓（排除模拟金 + 做市账户）
版本: v1.5.1
日期: 2026-04-06

每次进程启动执行一轮：fetch_hold_list 拉全量持仓，按交易对聚合后打印并可推 Telegram；执行完即退出。定时需求请用系统 cron 等调度。

  - 净持仓名义（USDT）：Σ(方向×张数×合约乘数×标记价)，正为净多、负为净空
  - 浮动盈亏：Σ(接口返回的 profit_loss / profitLoss)

排除规则（与现有脚本一致）：
  1) 模拟金：fetch_tag_list E1~E20 下 tag 含 E 的用户；且持仓行 tag 含 E 的也跳过
  2) 做市：account.config.json（WEBSEA_ACCOUNT_JSON）里 websea 非「测试」账户的 uid（与 ToolBoxNew.load_account 一致），不含 risk 占位

运行（策略目录为当前工作目录时）:
  python 其他独立脚本/检查仓位+adl相关/全市场净持仓与浮动盈亏汇总_排除模拟金做市.py

环境变量:
  WEBSEA_RISK_TOKEN / WEBSEA_RISK_SECRET  — 覆盖默认风控只读 token（与 user_pos_float_profit 同源时可只设 TOKEN）
  WEBSEA_ACCOUNT_JSON — account.config.json 路径，默认 /home/ubuntu/CCGo/resources/account.config.json
  MM_EXTRA_UIDS — 额外排除的 uid，逗号分隔（json 读不到时可用）
  MIN_NET_NOTIONAL_U — 仅展示 |净持仓金额| 超过该阈值(U) 的交易对，默认 10000；可用 --min-notional 覆盖
  Telegram：Bot Token 与群 chat_id 已在脚本内写死；需关闭推送时用 --no-telegram；仍可用 --tg-token / --tg-chat-id 临时覆盖
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, DefaultDict, Dict, List, Set, Tuple

from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

# 与「全部用户持仓浮动盈亏监控」同级引用方式：脚本在 策略/其他独立脚本/检查仓位+adl相关/
_HERE = os.path.dirname(os.path.abspath(__file__))
_STRATEGY_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _STRATEGY_ROOT not in sys.path:
    sys.path.insert(0, _STRATEGY_ROOT)

from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # noqa: E402
from utils import restclient as rc  # noqa: E402


DEFAULT_ACCOUNT_JSON = os.environ.get(
    "WEBSEA_ACCOUNT_JSON",
    "/home/ubuntu/CCGo/resources/account.config.json",
)
DEFAULT_TOKEN = os.environ.get("WEBSEA_RISK_TOKEN", "c1cf4185b2bed317aeb6e6674491fbef")
DEFAULT_SECRET = os.environ.get("WEBSEA_RISK_SECRET", "")

TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID_STR = "-5223829567"


def load_mm_uids_from_account_json(path: str) -> Set[str]:
    """与 ToolBoxNew.load_account 相同解析，返回做市等业务 uid（字符串），排除 risk 键。"""
    uids: Set[str] = set()
    if not path or not os.path.isfile(path):
        return uids
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    websea = data.get("websea") or {}
    for _k, v in websea.items():
        desc = (v.get("description") or "")
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
    """E1~E20 标记组中 tag 含 E 的 user_id；每个 E{n} 单独分页，避免跨 tag 复用页码。"""
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


def row_profit_loss(row: dict) -> float:
    v = row.get("profit_loss")
    if v is None:
        v = row.get("profitLoss")
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


async def run_once(
    rest: Contract,
    excluded: Set[str],
    symbol_unit: Dict[str, float],
    min_abs_notional: float,
) -> Tuple[
    List[Tuple[str, float, float]],
    List[Tuple[str, float, float]],
    int,
    int,
    int,
    Dict[str, Dict[str, List[Tuple[str, float, float, datetime | None]]]],
    Dict[str, Dict[str, Tuple[int, float, float]]],
]:
    """
    Returns:
      rows: [(symbol, net_notional, pnl), ...] 已过滤 |净持仓|>阈值，按 |净持仓| 降序
      rows_excluded_hedge_30d:
            在 rows 同口径下，额外剔除「持仓>30天 且 同用户同交易对同时有多空」后的结果
      kept_rows, skipped_rows, n_symbols_before_filter
      per_symbol_side_top5:
        {
          symbol: {
            "多": [(uid, notional_abs, pnl_sum, open_time_min), ...最多5],
            "空": [(uid, notional_abs, pnl_sum, open_time_min), ...最多5],
          }
        }
      per_symbol_side_excluded_gt7d:
        symbol -> side -> (剔除用户数, 合计持仓金额, 合计浮动盈亏)；仅统计持仓>7天被剔出排名的用户
    """
    holds = await fetch_all_hold_list(rest)
    per_sym: DefaultDict[str, List[float]] = defaultdict(lambda: [0.0, 0.0])
    per_sym_excluded_hedge_30d: DefaultDict[str, List[float]] = defaultdict(
        lambda: [0.0, 0.0]
    )
    per_sym_side_user: DefaultDict[
        str, DefaultDict[str, DefaultDict[str, List[Any]]]
    ] = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [0.0, 0.0, None])))
    user_symbol_sides: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)
    contrib_rows: List[Tuple[str, str, str, float, float, datetime | None]] = []
    kept = 0
    skipped = 0
    snapshot_now = datetime.now(BJT)

    def _parse_open_time(v: Any) -> datetime | None:
        if v is None or v == "":
            return None
        try:
            fv = float(v)
            if fv > 1e12:
                fv = fv / 1000.0
            return datetime.fromtimestamp(fv, tz=BJT)
        except (TypeError, ValueError, OSError):
            pass
        s = str(v).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=BJT)
            return dt.astimezone(BJT)
        except ValueError:
            return None

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

        signed = amt if od == 1 else (-amt if od == 2 else 0.0)
        try:
            mark = float(row.get("mark_price", 0))
        except (TypeError, ValueError):
            mark = 0.0

        csize = float(symbol_unit.get(symbol, 1.0))
        notional = signed * csize * mark
        notional_abs = abs(amt * csize * mark)
        pnl = row_profit_loss(row)
        side = "多" if od == 1 else ("空" if od == 2 else "未知")
        if side != "未知":
            user_symbol_sides[(uid, symbol)].add(side)
        if side != "未知":
            uid_bucket = per_sym_side_user[symbol][side][uid]
            uid_bucket[0] += notional_abs
            uid_bucket[1] += pnl
            open_t = _parse_open_time(
                row.get("open_time")
                or row.get("openTime")
                or row.get("create_time")
                or row.get("createTime")
                or row.get("ctime")
            )
            old_open = uid_bucket[2]
            if old_open is None or (open_t is not None and open_t < old_open):
                uid_bucket[2] = open_t
            contrib_rows.append((symbol, uid, side, notional, pnl, open_t))
        kept += 1

    for symbol, uid, side, notional, pnl, open_t in contrib_rows:
        per_sym[symbol][0] += notional
        per_sym[symbol][1] += pnl
        has_both = len(user_symbol_sides.get((uid, symbol), set())) >= 2
        held_30d = (
            open_t is not None
            and (snapshot_now - open_t).total_seconds() >= 30 * 24 * 3600
        )
        if not (has_both and held_30d):
            per_sym_excluded_hedge_30d[symbol][0] += notional
            per_sym_excluded_hedge_30d[symbol][1] += pnl

    n_all = len(per_sym)
    rows: List[Tuple[str, float, float]] = []
    for sym, pair in per_sym.items():
        net_n, pnl_sum = pair[0], pair[1]
        if abs(net_n) > float(min_abs_notional):
            rows.append((sym, net_n, pnl_sum))
    rows.sort(key=lambda x: abs(x[1]), reverse=True)
    rows_excluded_hedge_30d: List[Tuple[str, float, float]] = []
    for sym, pair in per_sym_excluded_hedge_30d.items():
        net_n, pnl_sum = pair[0], pair[1]
        if abs(net_n) > float(min_abs_notional):
            rows_excluded_hedge_30d.append((sym, net_n, pnl_sum))
    rows_excluded_hedge_30d.sort(key=lambda x: abs(x[1]), reverse=True)
    selected_symbols = {s for s, _, _ in rows}
    per_symbol_side_top5: Dict[
        str, Dict[str, List[Tuple[str, float, float, datetime | None]]]
    ] = {}
    per_symbol_side_excluded_gt7d: Dict[str, Dict[str, Tuple[int, float, float]]] = {}
    seven_days = timedelta(days=7)
    for sym in selected_symbols:
        side_map: Dict[str, List[Tuple[str, float, float, datetime | None]]] = {}
        ex_map: Dict[str, Tuple[int, float, float]] = {}
        for side in ("多", "空"):
            users = per_sym_side_user.get(sym, {}).get(side, {})
            packed: List[Tuple[str, float, float, datetime | None]] = []
            for uid, vals in users.items():
                packed.append((uid, float(vals[0]), float(vals[1]), vals[2]))
            excluded = [
                x
                for x in packed
                if x[3] is not None and (snapshot_now - x[3]) > seven_days
            ]
            included = [
                x
                for x in packed
                if not (x[3] is not None and (snapshot_now - x[3]) > seven_days)
            ]
            included.sort(key=lambda x: x[1], reverse=True)
            side_map[side] = included[:5]
            n_ex = len(excluded)
            if n_ex:
                sum_n = sum(x[1] for x in excluded)
                sum_p = sum(x[2] for x in excluded)
                ex_map[side] = (n_ex, sum_n, sum_p)
            else:
                ex_map[side] = (0, 0.0, 0.0)
        per_symbol_side_top5[sym] = side_map
        per_symbol_side_excluded_gt7d[sym] = ex_map

    return (
        rows,
        rows_excluded_hedge_30d,
        kept,
        skipped,
        n_all,
        per_symbol_side_top5,
        per_symbol_side_excluded_gt7d,
    )


def format_report(
    when: datetime,
    rows: List[Tuple[str, float, float]],
    rows_excluded_hedge_30d: List[Tuple[str, float, float]],
    per_symbol_side_top5: Dict[
        str, Dict[str, List[Tuple[str, float, float, datetime | None]]]
    ],
    per_symbol_side_excluded_gt7d: Dict[str, Dict[str, Tuple[int, float, float]]],
    kept: int,
    skipped: int,
    mm_cnt: int,
    sim_cnt: int,
    min_abs_notional: float,
    n_symbols_all: int,
) -> str:
    lines = [
        "======== 全市场净持仓（已排除模拟金 + 做市） ========",
        "时间(北京时间): {}".format(when.strftime("%Y-%m-%d %H:%M:%S")),
        "说明: 净持仓金额(U)=Σ(方向×张数×合约乘数×标记价)；浮动盈亏(U)=Σ(持仓接口 profit_loss / profitLoss)。",
        "金额列均四舍五入到整数 U；ALL合计(本表)=本表各行取整后求和。",
        "筛选: 仅展示 |净持仓金额| > {:.0f} U 的交易对；排序: 按 |净持仓金额| 降序。（全量有持仓交易对数={}，本表行数={}）".format(
            min_abs_notional, n_symbols_all, len(rows)
        ),
        "本快照: 保留持仓条数={} 跳过条数={} | 排除做市uid数={} 模拟金uid数={}".format(
            kept, skipped, mm_cnt, sim_cnt
        ),
        "{:14} {:>22} {:>22}".format("symbol", "净持仓金额(U)", "浮动盈亏(U)"),
        "-" * 62,
    ]
    total_n = 0
    total_p = 0
    if not rows:
        lines.append("(无满足条件的交易对)")
    else:
        for sym, n, p in rows:
            rn = int(round(n))
            rp = int(round(p))
            total_n += rn
            total_p += rp
            lines.append("{:14} {:>22d} {:>22d}".format(sym, rn, rp))
    lines.append("-" * 62)
    lines.append(
        "{:14} {:>22d} {:>22d}".format("ALL合计(本表)", total_n, total_p)
    )
    lines.append("")
    lines.append(
        "======== 剔除双向>30天后的净持仓（结构同上） ========"
    )
    lines.append(
        "规则: 剔除同用户同交易对同时有多空，且该持仓已超过30天的持仓金额与浮动盈亏。"
    )
    lines.append("{:14} {:>22} {:>22}".format("symbol", "净持仓金额(U)", "浮动盈亏(U)"))
    lines.append("-" * 62)
    total_n2 = 0
    total_p2 = 0
    if not rows_excluded_hedge_30d:
        lines.append("(无满足条件的交易对)")
    else:
        for sym, n, p in rows_excluded_hedge_30d:
            rn = int(round(n))
            rp = int(round(p))
            total_n2 += rn
            total_p2 += rp
            lines.append("{:14} {:>22d} {:>22d}".format(sym, rn, rp))
    lines.append("-" * 62)
    lines.append("{:14} {:>22d} {:>22d}".format("ALL合计(本表)", total_n2, total_p2))
    lines.append("")
    lines.append("======== 交易对分多空：持仓金额前5用户 ========")
    lines.append(
        "说明: 持仓金额(U)=|张数×合约乘数×标记价|；已持仓时间=当前时间-最早开仓时间。"
        "排名已剔除「已持仓时间超过7天」的用户；若有剔除则下列给出人数与合计。"
    )

    def _fmt_duration(open_t: datetime | None) -> str:
        if open_t is None:
            return "未知"
        delta = when - open_t
        sec = int(delta.total_seconds())
        if sec < 0:
            sec = 0
        d, r = divmod(sec, 86400)
        h, r = divmod(r, 3600)
        m, _ = divmod(r, 60)
        if d > 0:
            return "{}天{}小时{}分".format(d, h, m)
        if h > 0:
            return "{}小时{}分".format(h, m)
        return "{}分".format(m)

    for sym, _, _ in rows:
        lines.append("[{}]".format(sym))
        for side in ("多", "空"):
            lines.append("  {}持仓TOP5:".format(side))
            lines.append(
                "  {:>3} {:>14} {:>14} {:>14} {:>14}".format(
                    "名次", "用户ID", "持仓金额(U)", "浮动盈亏(U)", "已持仓时间"
                )
            )
            sub = per_symbol_side_top5.get(sym, {}).get(side, [])
            if not sub:
                lines.append("  (无)")
            else:
                for i, (uid, notional_abs, pnl_sum, open_t) in enumerate(sub, start=1):
                    lines.append(
                        "  {:>3d} {:>14} {:>14d} {:>14d} {:>14}".format(
                            i,
                            uid,
                            int(round(notional_abs)),
                            int(round(pnl_sum)),
                            _fmt_duration(open_t),
                        )
                    )
            n_ex, sum_n, sum_p = per_symbol_side_excluded_gt7d.get(sym, {}).get(
                side, (0, 0.0, 0.0)
            )
            if n_ex > 0:
                lines.append(
                    "  已剔除{}个用户(已持仓>7天)，合计持仓金额{} U，合计浮动盈亏{} U".format(
                        n_ex,
                        int(round(sum_n)),
                        int(round(sum_p)),
                    )
                )
    lines.append("")
    return "\n".join(lines)


async def send_report_telegram(
    client: rc.RestClient,
    token: str,
    chat_id: int,
    text: str,
) -> None:
    """按行累积，单条不超过 Telegram 约 4096 限制（与 set_fund_rate.send_tg 一致）。"""
    send_text = ""
    for t in text.split("\n"):
        send_text += t + "\n"
        if len(send_text) > 4000:
            await client.tg_warning(token=token, chat_id=chat_id, content=send_text)
            send_text = ""
    if send_text:
        await client.tg_warning(token=token, chat_id=chat_id, content=send_text)


async def async_main(
    min_abs_notional: float,
    tg_token: str,
    tg_chat_id: int | None,
    no_telegram: bool,
) -> None:
    rest = Contract(DEFAULT_TOKEN, DEFAULT_SECRET, dev=False)
    rest.DEBUG = False
    rest.rest_timeout = 120

    mm_uids = load_mm_uids_from_account_json(DEFAULT_ACCOUNT_JSON)
    if not mm_uids and not os.path.isfile(DEFAULT_ACCOUNT_JSON):
        print(
            "警告: 未找到 account.config.json（{}），做市账户仅依赖 MM_EXTRA_UIDS".format(
                DEFAULT_ACCOUNT_JSON
            )
        )

    sim_uids: Set[str] = set()
    try:
        sim_uids = await collect_sim_uids(rest)
    except Exception:
        print("拉取模拟金用户失败，将仅按持仓行 tag 过滤 E 组")
        print(traceback.format_exc())

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
        print(traceback.format_exc())
        symbol_unit = {}

    now = datetime.now(BJT)
    (
        rows,
        rows_excluded_hedge_30d,
        kept,
        skipped,
        n_sym_all,
        per_symbol_side_top5,
        per_symbol_side_excluded_gt7d,
    ) = await run_once(rest, excluded, symbol_unit, min_abs_notional)
    text = format_report(
        now,
        rows,
        rows_excluded_hedge_30d,
        per_symbol_side_top5,
        per_symbol_side_excluded_gt7d,
        kept,
        skipped,
        len(mm_uids),
        len(sim_uids),
        min_abs_notional,
        n_sym_all,
    )
    print(text, flush=True)

    if tg_client is not None and tg_token and tg_chat_id is not None:
        try:
            await send_report_telegram(tg_client, tg_token, tg_chat_id, text)
        except Exception:
            print("Telegram 推送失败:\n{}".format(traceback.format_exc()), flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="单次执行：按交易对汇总净持仓与浮动盈亏（排除模拟金/做市）")
    p.add_argument(
        "--interval",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--sim-refresh-minutes",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--once",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p.add_argument(
        "--min-notional",
        type=float,
        default=float(os.environ.get("MIN_NET_NOTIONAL_U", "10000")),
        help="仅展示 |净持仓金额(U)| 大于该值的交易对，默认 10000；环境变量 MIN_NET_NOTIONAL_U",
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
        help="不发送 Telegram，即使已配置 token/chat",
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
            args.min_notional,
            (args.tg_token or "").strip(),
            chat_id,
            args.no_telegram,
        )
    )


if __name__ == "__main__":
    main()

