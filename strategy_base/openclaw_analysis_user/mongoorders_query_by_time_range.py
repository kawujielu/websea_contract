# -*- coding: utf-8 -*-
"""
连接 fund.mongoorders（与 update_mysql.py / spot_active_rank.py 相同库表），
按指定时间段筛选成交，输出：① 条数；② 按 symbol 汇总净成交数量与加权均价；
③ 汇总表「对冲价」列（REST 深度：净数量<0 取卖一买入对冲，>0 取买一卖出对冲）；
④ 汇总表「对冲盈亏」列：以对冲价一步平掉净头寸相对区间内成交均价的盈亏（计价与 Σ(p×amount) 一致）；
⑤ 输出各 symbol 可计算对冲盈亏的合计；depth 请求失败时打印错误信息及接口原始响应（含重试过程）；
⑥ 汇总需买入资产的 USDT 合计（卖一×买入数量）、逐 symbol 展示需卖出数量。

修改下方「配置区」后在本目录执行：

  python mongoorders_query_by_time_range.py

依赖：mysql-connector-python、requests。Python 3.9+。
"""
from __future__ import annotations

import time
import sys
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

# ---------------------------------------------------------------------------
# 配置区
# ---------------------------------------------------------------------------

# 统计区间（北京时间，自然日，含首尾整天）
START_DATE = "2026-04-28"  # YYYY-MM-DD
END_DATE = "2026-05-16"  # YYYY-MM-DD

# ts 字符串格式（与库内 mongoorders.ts 一致）：iso / compact / slash
TS_STR_STYLE = "iso"

# MySQL（与 update_mysql.py 一致，直接写在本脚本）
MYSQL_HOST = "abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com"
MYSQL_USER = "admin"
MYSQL_PASSWORD = "A(?xvw8~v(ke0(O,=Se!W(!UGBujuh(XkBHuQTRu2"
MYSQL_DATABASE = "fund"
MYSQL_PORT = 3306
TABLE_NAME = "mongoorders"

# 公共行情 REST（与 daily_profit_top10_kline_report / monitor con_gear_depth 同源）
REST_PUBLIC_BASE = "https://exqv.websea.work" #"https://coqv.websea.work"
DEPTH_API_PATH = "/openApi/market/depth" #"/qapi-v1/market/depth"
REQUEST_TIMEOUT_SEC = 15
# 单 symbol 拉 depth：每次请求失败（网络/HTTP/无深度等）后间隔 1s 再试，
# 最多共尝试 DEPTH_FETCH_MAX_ATTEMPTS 次（含首次）
DEPTH_FETCH_MAX_ATTEMPTS = 5
DEPTH_FETCH_RETRY_SLEEP_SEC = 1.0
# 逐 symbol 拉深度间隔（秒），减轻限频
PRICE_FETCH_SLEEP_SEC = 0.05

# ---------------------------------------------------------------------------


def _depth_fail_msg(reason: str, raw_text: str) -> str:
    """失败说明 + 接口原始响应体。"""
    body = raw_text if raw_text is not None else ""
    return "{} | 原始响应: {}".format(reason, body)


def _fetch_depth_bid_ask_once(
    symbol: str,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    单次 GET depth；成功返回 (买一, 卖一, None)，失败返回 (None, None, 错误说明)。
    """
    sym = str(symbol).strip()
    url = REST_PUBLIC_BASE.rstrip("/") + DEPTH_API_PATH
    raw_text = ""
    try:
        r = requests.get(url, params={"symbol": sym}, timeout=REQUEST_TIMEOUT_SEC)
        raw_text = r.text
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if not raw_text and e.response is not None:
            raw_text = e.response.text or ""
        status = e.response.status_code if e.response is not None else "?"
        return None, None, _depth_fail_msg("HTTP {}".format(status), raw_text)
    except requests.exceptions.RequestException as e:
        return None, None, "请求异常（无响应体）: {}".format(e)

    try:
        data = r.json()
    except ValueError:
        return None, None, _depth_fail_msg("响应非 JSON", raw_text)

    try:
        errno = int(data.get("errno", -1))
        if errno != 0:
            return None, None, _depth_fail_msg("接口 errno={}".format(errno), raw_text)
        res = data.get("result") or {}
        bids = res.get("bids") or []
        asks = res.get("asks") or []
        bid1: Optional[float] = None
        ask1: Optional[float] = None
        if bids:
            bid1 = float(bids[0][0])
        if asks:
            ask1 = float(asks[0][0])
        if bid1 is None and ask1 is None:
            return None, None, _depth_fail_msg(
                "深度无买一/卖一 (bids={}, asks={})".format(len(bids), len(asks)),
                raw_text,
            )
        return bid1, ask1, None
    except (IndexError, TypeError, ValueError) as e:
        return None, None, _depth_fail_msg("解析深度失败: {}".format(e), raw_text)


def fetch_depth_bid_ask(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """
    拉深度买一/卖一；单次失败则间隔 DEPTH_FETCH_RETRY_SLEEP_SEC 再试，
    最多共尝试 DEPTH_FETCH_MAX_ATTEMPTS 次（含首次）；失败时向 stderr 输出原因。
    """
    sym = str(symbol).strip()
    max_n = max(1, int(DEPTH_FETCH_MAX_ATTEMPTS))
    sleep_sec = float(DEPTH_FETCH_RETRY_SLEEP_SEC)
    last_err = "未知错误"
    for i in range(max_n):
        bid1, ask1, err = _fetch_depth_bid_ask_once(sym)
        if err is None:
            return bid1, ask1
        last_err = err
        print(
            "[depth] symbol={} 第 {}/{} 次失败: {}".format(sym, i + 1, max_n, last_err),
            file=sys.stderr,
        )
        if i < max_n - 1 and sleep_sec > 0:
            time.sleep(sleep_sec)
    print(
        "[depth] symbol={} 最终失败（共尝试 {} 次）: {}".format(sym, max_n, last_err),
        file=sys.stderr,
    )
    return None, None


def build_symbol_depth_quotes(
    symbols: Sequence[str],
) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    out: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    for i, sym in enumerate(symbols):
        s = str(sym).strip()
        if not s:
            continue
        out[s] = fetch_depth_bid_ask(s)
        if i < len(symbols) - 1 and PRICE_FETCH_SLEEP_SEC > 0:
            time.sleep(PRICE_FETCH_SLEEP_SEC)
    return out


def hedge_price_from_depth(
    net_amt: float,
    bid1: Optional[float],
    ask1: Optional[float],
) -> Tuple[Optional[float], Optional[str]]:
    """
    按净头寸方向取对冲价：净数量<0 需买入→卖一；>0 需卖出→买一。
    返回 (对冲价, 档位说明)；无法取价时 (None, None)。
    """
    if abs(net_amt) < 1e-15:
        return None, None
    if net_amt < 0:
        if ask1 is None:
            return None, None
        return ask1, "卖一"
    if bid1 is None:
        return None, None
    return bid1, "买一"


def _parse_day(s: str) -> date:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def _fmt_ts(dt: datetime) -> str:
    if TS_STR_STYLE == "compact":
        return dt.strftime("%Y%m%d%H%M%S")
    if TS_STR_STYLE == "slash":
        return dt.strftime("%Y/%m/%d %H:%M:%S")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def day_range_ts_bounds(start_d: date, end_d: date) -> Tuple[str, str]:
    """[start 00:00:00, end 23:59:59] 闭区间，用于 ts 字符串比较。"""
    t0 = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=BJT)
    t1 = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=BJT)
    return _fmt_ts(t0), _fmt_ts(t1)


def connect_mysql():
    import mysql.connector

    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
    )


def fetch_symbol_amount_price_stats(
    cursor: Any, ts_lo: str, ts_hi: str
) -> List[Tuple[str, float, float, Optional[float]]]:
    """
    按 symbol 聚合：净数量 = Σamount，总金额 = Σ(price×amount)，均价 = 总金额/净数量。
    返回 [(symbol, net_amount, total_notional, avg_price_or_None), ...]，symbol 升序。
    """
    tbl = TABLE_NAME.replace("`", "")
    sql = (
        "SELECT symbol, "
        "COALESCE(SUM(amount), 0) AS net_amount, "
        "COALESCE(SUM(price * amount), 0) AS total_notional "
        "FROM `{table}` "
        "WHERE ts >= %s AND ts <= %s "
        "GROUP BY symbol "
        "ORDER BY symbol ASC"
    ).format(table=tbl)
    cursor.execute(sql, (ts_lo, ts_hi))
    out: List[Tuple[str, float, float, Optional[float]]] = []
    for row in cursor.fetchall() or []:
        sym = str(row[0]) if row[0] is not None else ""
        net_amt = float(row[1] or 0.0)
        total_notional = float(row[2] or 0.0)
        if abs(net_amt) < 1e-15:
            avg_p = None
        else:
            avg_p = total_notional / net_amt
        out.append((sym, net_amt, total_notional, avg_p))
    return out


def hedge_pnl_quote_at_hedge_price(
    net_amt: float, avg_price: Optional[float], hedge_px: Optional[float]
) -> Optional[float]:
    """
    以对冲价一次性平掉净头寸相对区间 VWAP 的盈亏（计价口径与 Σ(price×amount) 一致）。

    与 mongoorders 常见约定一致：净 amount 为负表示需买入对冲（卖一），为正表示需卖出对冲（买一）。
    盈亏 = (对冲价 - 成交均价) × (-净数量)。
    """
    if abs(net_amt) < 1e-15 or avg_price is None or hedge_px is None:
        return None
    return (hedge_px - avg_price) * (-net_amt)


def _print_symbol_stats(
    stats: List[Tuple[str, float, float, Optional[float]]],
    depth_quotes: Dict[str, Tuple[Optional[float], Optional[float]]],
) -> Tuple[float, int, int]:
    """
    打印 symbol 汇总表。返回 (对冲盈亏合计, 计入合计的 symbol 数, N/A 未计入数)。
    """
    if not stats:
        print("(无 symbol 汇总)")
        return 0.0, 0, 0
    print(
        "{:<20} {:>16} {:>20} {:>14} {:>22} {:>18}".format(
            "symbol",
            "净成交Σamount",
            "累积Σ(p×a)",
            "成交均价",
            "对冲价",
            "对冲盈亏(U)",
        )
    )
    print("-" * 120)
    total_pnl = 0.0
    pnl_count = 0
    na_count = 0
    for sym, net_amt, total_notional, avg_p in stats:
        avg_s = "{:.8f}".format(avg_p) if avg_p is not None else "N/A(净数量0)"
        bid1, ask1 = depth_quotes.get(sym, (None, None))
        hedge_px, side = hedge_price_from_depth(net_amt, bid1, ask1)
        if hedge_px is not None and side:
            cur_s = "{:.8f}({})".format(hedge_px, side)
        elif bid1 is None and ask1 is None:
            cur_s = "获取失败"
        else:
            cur_s = "N/A"
        pnl = hedge_pnl_quote_at_hedge_price(net_amt, avg_p, hedge_px)
        if pnl is not None:
            pnl_s = "{:.8f}".format(pnl)
            total_pnl += pnl
            pnl_count += 1
        else:
            pnl_s = "N/A"
            na_count += 1
        print(
            "{:<20} {:>16.8f} {:>20.8f} {:>14} {:>22} {:>18}".format(
                sym, net_amt, total_notional, avg_s, cur_s, pnl_s
            )
        )
    print("-" * 120)
    print(
        "对冲盈亏(U)合计: {:.8f}（{} 个 symbol 计入，{} 个 N/A 未计入）".format(
            total_pnl, pnl_count, na_count
        )
    )
    return total_pnl, pnl_count, na_count


def _print_hedge_requirements(
    stats: List[Tuple[str, float, float, Optional[float]]],
    depth_quotes: Dict[str, Tuple[Optional[float], Optional[float]]],
) -> None:
    """
    需买入（净数量<0）：按卖一估算 USDT 并加总；
    需卖出（净数量>0）：逐 symbol 展示卖出数量。
    """
    buy_usdt_total = 0.0
    buy_count = 0
    buy_skip = 0
    sell_rows: List[Tuple[str, float]] = []

    for sym, net_amt, _total_notional, _avg_p in stats:
        if abs(net_amt) < 1e-15:
            continue
        if net_amt < 0:
            _bid1, ask1 = depth_quotes.get(sym, (None, None))
            buy_qty = -net_amt
            if ask1 is None:
                buy_skip += 1
                continue
            buy_usdt_total += ask1 * buy_qty
            buy_count += 1
        else:
            sell_rows.append((sym, net_amt))

    print("---")
    print(
        "对冲需求汇总（净数量<0 需买入@卖一，>0 需卖出；"
        "买入 USDT = 卖一 × |净数量|）："
    )
    if buy_count or buy_skip:
        print(
            "需买入 USDT 合计: {:.8f}（{} 个 symbol 计入{}）".format(
                buy_usdt_total,
                buy_count,
                "，{} 个缺卖一未计入".format(buy_skip) if buy_skip else "",
            )
        )
    else:
        print("需买入 USDT 合计: 0.00000000（无需买入）")

    if sell_rows:
        print("需卖出数量:")
        for sym, qty in sell_rows:
            print("  {}: {:.8f}".format(sym, qty))
        print("（共 {} 个 symbol 需卖出）".format(len(sell_rows)))
    else:
        print("需卖出数量: (无)")


def main() -> int:
    start_d = _parse_day(START_DATE)
    end_d = _parse_day(END_DATE)
    if start_d > end_d:
        print("START_DATE 不能晚于 END_DATE", file=sys.stderr)
        return 2

    ts_lo, ts_hi = day_range_ts_bounds(start_d, end_d)

    db = None
    cursor = None
    try:
        db = connect_mysql()
        if not db.is_connected():
            print("数据库未连接", file=sys.stderr)
            return 1
        print("已连接数据库: {} / 表: {}".format(MYSQL_DATABASE, TABLE_NAME))
        print(
            "筛选条件: ts >= '{}' AND ts <= '{}'（北京时间 {} ~ {}）".format(
                ts_lo,
                ts_hi,
                start_d.isoformat(),
                end_d.isoformat(),
            )
        )
        print("ts 字符串格式: {}".format(TS_STR_STYLE or "iso"))
        print("---")

        cursor = db.cursor()
        count_sql = (
            "SELECT COUNT(*) FROM `{table}` WHERE ts >= %s AND ts <= %s"
        ).format(table=TABLE_NAME.replace("`", ""))
        cursor.execute(count_sql, (ts_lo, ts_hi))
        total = int(cursor.fetchone()[0])

        print("符合条件的数据条数: {}".format(total))
        print("---")
        print(
            "按 symbol 汇总（均价 = 区间内 Σ(price×amount) / Σ(amount)，"
            "净成交数量 = Σ(amount)；对冲价 = REST 深度：净数量<0 取卖一买入，>0 取买一卖出；"
            "对冲盈亏(U) = (对冲价 − 成交均价) × (−净数量)，即按对冲价一步平掉净头寸相对均价的盈亏）："
        )
        sym_stats = fetch_symbol_amount_price_stats(cursor, ts_lo, ts_hi)
        syms_order = [row[0] for row in sym_stats]
        depth_map = build_symbol_depth_quotes(syms_order)
        _print_symbol_stats(sym_stats, depth_map)
        _print_hedge_requirements(sym_stats, depth_map)
        print("---")
        print("共 {} 个 symbol".format(len(sym_stats)))

        return 0
    except Exception as e:
        print("数据库操作错误: {}".format(e), file=sys.stderr)
        return 1
    finally:
        if cursor is not None:
            cursor.close()
        if db is not None and db.is_connected():
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
