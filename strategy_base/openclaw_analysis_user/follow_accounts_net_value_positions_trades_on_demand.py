from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from follow_accounts_api_keys import FOLLOW_ACCOUNTS


BJT = ZoneInfo("Asia/Shanghai")


def _ensure_contract_import_path():
    """
    让 `from client.env_pro.rest.websea.contract import WebseaContract as Contract`
    在本脚本运行时可用。
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # code -> 用户分析 -> skills -> AI AGENT -> websea
    websea_root = os.path.abspath(os.path.join(script_dir, "..", "..", "..", ".."))
    strategy_base_root = os.path.join(websea_root, "合约项目", "strategy_base")
    if strategy_base_root not in sys.path:
        sys.path.insert(0, strategy_base_root)


def parse_args():
    p = argparse.ArgumentParser(description="follow accounts net value/positions/trades on demand")
    p.add_argument("--mode", type=str, default="window", help="window | all")
    p.add_argument("--days", type=int, default=3, help="window days")
    p.add_argument("--detail-limit", type=int, default=100, help="per account trades limit; 0 means no limit")
    p.add_argument("--print_stdout", action="store_true")
    args, _ = p.parse_known_args()
    return args


def build_time_window(mode: str, days: int) -> Tuple[str, int, int]:
    now_dt = datetime.now(BJT)
    ts_max = int(now_dt.timestamp())

    mode = (mode or "window").lower().strip()
    if mode == "all":
        start_day = now_dt.date() - timedelta(days=3650)
        label = "全部/全量"
    else:
        d = max(1, int(days))
        start_day = now_dt.date() - timedelta(days=d - 1)
        label = "近{}天".format(d)

    ts_min = int(datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT).timestamp())
    return label, ts_min, ts_max


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def position_direction_txt(p_type: int) -> str:
    if int(p_type) == 1:
        return "多仓"
    if int(p_type) == 2:
        return "空仓"
    return str(p_type)


def contract_trade_side_txt(contract_type: Any, order_type: Any) -> str:
    """
    contract_type: 开仓/平仓（ContractType.Open/Close or its string）
    order_type: buy/sell（CurrentList.type）
    """
    ct = str(contract_type or "")
    ot = str(order_type or "").lower()

    is_open = ("开仓" in ct) or ("Open" in ct) or ct.strip().lower().endswith("open")
    is_close = ("平仓" in ct) or ("Close" in ct) or ct.strip().lower().endswith("close")

    is_buy = ("buy" in ot) or ("bid" in ot)
    is_sell = ("sell" in ot) or ("ask" in ot)

    if is_open:
        if is_buy:
            return "开多"
        if is_sell:
            return "开空"
        return "开仓(未知方向)"
    if is_close:
        if is_buy:
            return "平空"
        if is_sell:
            return "平多"
        return "平仓(未知方向)"

    # fallback
    if is_buy:
        return "交易(未知contract_type-买)"
    if is_sell:
        return "交易(未知contract_type-卖)"
    return "交易(未知方向)"


async def fetch_order_history_for_range(
    contract: Any,
    ts_min: int,
    ts_max: int,
    limit_each: int,
    symbol: Optional[str] = None,
) -> Tuple[List[Any], bool]:
    """
    由于 get_order_history 只有分页入参（order_from），无法直接按时间拉取。
    我们按 direct='prev' 从新到旧拉，直到 ctime < ts_min 为止。
    返回：orders_in_range_sorted_desc, truncated_flag
    """
    direct = "prev"
    is_full = 2

    collected: List[Any] = []
    order_from = None  # type: Optional[str]

    truncated = False

    while True:
        # limit=100 是合约接口允许的最大值
        page = await contract.get_order_history(
            symbol=symbol,
            order_from=order_from,
            direct=direct,
            limit=100,
            is_full=is_full,
        )
        print(11111111111111, page)
        if not page:
            break

        # page 是 {order_id: CurrentList}
        vals = list(page.values())
        # 按时间倒序
        try:
            vals.sort(key=lambda x: int(getattr(x, "ctime", 0) or 0), reverse=True)
        except Exception:
            pass

        # 取本页内所有落在区间的
        page_min_ctime = None
        for v in vals:
            ctime_s = _safe_int(getattr(v, "ctime", 0), 0)
            if page_min_ctime is None or ctime_s < page_min_ctime:
                page_min_ctime = ctime_s
            if ts_min <= ctime_s <= ts_max:
                collected.append(v)

        # 如果本页最小时间都已经小于 ts_min，说明再翻页没有必要（因为后续只会更旧）
        if page_min_ctime is not None and page_min_ctime < ts_min:
            break

        # 如果本页都在区间内，继续拉下一页；用本页最旧订单 id 做 order_from
        oldest = vals[-1] if vals else None
        order_from = getattr(oldest, "order_id", None) if oldest is not None else None
        if not order_from:
            # 无法继续分页
            break

        # 若已经收集足够且需要截断，则可以提前停
        if limit_each and limit_each > 0 and len(collected) > limit_each:
            truncated = True
            break

    # 最终按 ctime 倒序
    try:
        collected.sort(key=lambda x: int(getattr(x, "ctime", 0) or 0), reverse=True)
    except Exception:
        pass

    if limit_each and limit_each > 0 and len(collected) > limit_each:
        collected = collected[:limit_each]
        truncated = True

    return collected, truncated


def format_positions_only_required(p_list: List[Any]) -> Tuple[List[str], float, float]:
    """
    只输出用户要求的持仓字段：
    symbol / amount / direction / open_price_avg / liquidation_price / lever_rate

    另外返回：account_equity（去重策略）和 realized/unrealized sum 用于净值展示。
    """
    if not p_list:
        return ["暂无持仓"], 0.0, 0.0

    equities_rounded = set()
    realized_sum = 0.0
    unrealized_sum = 0.0

    pos_lines: List[str] = []
    for p in p_list:
        equities_rounded.add(round(_safe_float(getattr(p, "equity", 0.0), 0.0), 8))
        realized_sum += _safe_float(getattr(p, "profit", 0.0), 0.0)
        unrealized_sum += _safe_float(getattr(p, "un_profit", 0.0), 0.0)

        symbol = getattr(p, "symbol", "")
        amount = _safe_int(getattr(p, "amount", 0), 0)
        direction = position_direction_txt(getattr(p, "type", 0))
        open_price_avg = getattr(p, "open_price_avg", None)
        liquidation_price = getattr(p, "liquidation_price", None)
        lever_rate = getattr(p, "lever_rate", None)

        pos_lines.append(
            "交易对:{} 持仓数量:{} 方向:{} 开仓价格:{} 爆仓价格:{} 杠杆倍数:{}".format(
                symbol,
                amount,
                direction,
                open_price_avg if open_price_avg is not None else "N/A",
                liquidation_price if liquidation_price is not None else "N/A",
                lever_rate if lever_rate is not None else "N/A",
            )
        )

    equities_list = list(equities_rounded)
    if len(equities_list) == 1:
        account_equity = float(equities_list[0])
    else:
        account_equity = float(sum(equities_list))

    return pos_lines, account_equity, realized_sum + unrealized_sum * 0.0  # placeholder


def format_trades_lines(orders: List[Any]) -> str:
    if not orders:
        return "无成交明细"
    lines: List[str] = []
    for o in orders:
        ctime_s = _safe_int(getattr(o, "ctime", 0), 0)
        ts_str = datetime.fromtimestamp(ctime_s, tz=BJT).strftime("%Y-%m-%d %H:%M:%S")
        symbol = getattr(o, "symbol", "")
        contract_type = getattr(o, "contract_type", None)
        order_type = getattr(o, "type", None)
        side = contract_trade_side_txt(contract_type, order_type)

        price_avg = getattr(o, "price_avg", None)
        price = getattr(o, "price", None)
        deal_amount = getattr(o, "deal_amount", None)
        profit = getattr(o, "profit", None)

        px = price_avg if price_avg is not None else price
        qty = deal_amount if deal_amount is not None else 0
        pnl = profit if profit is not None else 0

        lines.append(
            "{} {} {} 价:{} 量:{} 盈亏:{}".format(
                ts_str,
                symbol,
                side,
                round(_safe_float(px, 0.0), 6),
                round(_safe_float(qty, 0.0), 6),
                round(_safe_float(pnl, 0.0), 4),
            )
        )
    return "\n".join(lines)


async def run_one_account(
    account: Dict[str, Any],
    mode: str,
    days: int,
    detail_limit: int,
) -> str:
    name = str(account.get("name", "") or "").strip() or "未命名账户"
    uid = str(account.get("uid", "") or "").strip()
    token = str(account.get("token", "") or "")
    secret_key = str(account.get("secret_key", "") or "")
    if not uid or not token or not secret_key:
        return "账户:{} uid={} token/secret_key 未配置，跳过".format(name, uid)
    try:
        uid_int = int(uid)
    except Exception:
        return "账户:{} uid={} 非法（必须为数字），请在 follow_accounts_api_keys.py 填写真实 uid".format(name, uid)

    label, ts_min, ts_max = build_time_window(mode, days)

    _ensure_contract_import_path()
    if "../.." not in sys.path:
        sys.path.append("../..")
    from client.env_pro.rest.websea.contract import WebseaContract as Contract  # type: ignore

    contract = Contract(token=token, secret_key=secret_key, uid=uid_int)

    try:
        positions = await contract.get_position(is_full=2)
    except Exception:
        positions = []

    pos_lines, account_equity, _ = format_positions_only_required(positions or [])
    realized_sum = 0.0
    unrealized_sum = 0.0
    if positions:
        for p in positions:
            realized_sum += _safe_float(getattr(p, "profit", 0.0), 0.0)
            unrealized_sum += _safe_float(getattr(p, "un_profit", 0.0), 0.0)

    # trades
    try:
        orders, truncated = await fetch_order_history_for_range(
            contract=contract,
            ts_min=ts_min,
            ts_max=ts_max,
            limit_each=detail_limit,
        )
    except Exception:
        orders, truncated = [], False

    header = "账户:{} uid={} 净值/盈亏（{}）".format(name, uid, label)
    pnl_line = "净值:{} realized_profit:{} unrealized_profit:{}".format(
        round(float(account_equity), 6),
        round(float(realized_sum), 4),
        round(float(unrealized_sum), 4),
    )
    body = "\n".join(pos_lines)
    trades_txt = format_trades_lines(orders)
    if truncated and detail_limit and detail_limit > 0:
        trades_txt = "{}\n\n本次成交明细已截断（展示最近{}条）。若需全部展示，请在请求中增加“全部展示”".format(
            trades_txt, detail_limit
        )

    return "\n".join([header, pnl_line, "持仓：", body, "", "成交明细：", trades_txt])


async def run_all(mode: str, days: int, detail_limit: int, concurrency: int = 3) -> str:
    if not FOLLOW_ACCOUNTS:
        return "FOLLOW_ACCOUNTS 为空，请先填写你的带单账户列表"

    sem = asyncio.Semaphore(concurrency)
    outputs: List[str] = []

    async def _worker(acc: Dict[str, Any]):
        async with sem:
            return await run_one_account(acc, mode=mode, days=days, detail_limit=detail_limit)

    tasks = [_worker(acc) for acc in FOLLOW_ACCOUNTS]
    for coro in asyncio.as_completed(tasks):
        try:
            outputs.append(await coro)
        except Exception as e:
            outputs.append("某账户处理失败: {}".format(e))

    # 为了可读性保持输出顺序与输入一致（as_completed 打乱的话）
    # 简化处理：如果你希望顺序严格按 FOLLOW_ACCOUNTS，后续我可改成 enumerate + index 排序。
    return "\n\n".join(outputs)


def main():
    args = parse_args()
    mode = args.mode
    days = int(args.days)
    detail_limit = int(args.detail_limit)

    text = asyncio.run(run_all(mode=mode, days=days, detail_limit=detail_limit))
    if args.print_stdout:
        print(text)


if __name__ == "__main__":
    main()


