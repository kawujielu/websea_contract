# -*- coding: utf-8 -*-
"""C 组用户交易汇总（含初始资金 / 最大回撤 / 总收益率）。

参考:
  - tag_tz_users_trade_stats.py: fetch_tag_list 拉标记组用户
  - mongdb_order.py: Mongo real_contract_deal 查询与 calc_trade_stats

运行:
  python c_group_users_trade_report.py

依赖: pip install motor pandas numpy
可选: 本机可 import crypto_center（WEBSEA_REPO_ROOT）
"""
from __future__ import annotations

import asyncio
import os
import sys
import warnings
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# ======================== 写死配置 ========================
TAG = "C"
RISK_TOKEN = "c1cf4185b2bed317aeb6e6674491fbef"
RISK_SECRET = ""
# 含 crypto_center 的仓库根；拉不到用户时可改 MANUAL_UIDS
WEBSEA_REPO_ROOT = "/home/ubuntu/strategy_base"
# 若 API 失败或想写死用户，填这里（非空则跳过 API）
MANUAL_UIDS: List[str] = []

MONGO_URI = "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/"  #"mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/"
MONGO_DB = "exchange"
MONGO_COLL = "real_contract_deal"

# 查询区间（北京时间，含首尾日）
BEGIN_DATE = "2026-08-01"
END_DATE = "2026-09-20"

PAGE_SLEEP = 0.5
OUT_DIR = os.path.join(os.path.expanduser("~"), "Desktop")
STATS_CSV = os.path.join(OUT_DIR, "c_group_trade_stats.csv")
# ==========================================================

_BJT = timezone(timedelta(hours=8))
BUY_SELL_SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}
OPEN_ACTIONS = {"开多", "开空"}
CLOSE_ACTIONS = {"平多", "平空"}


def _bjt_date_range_to_ts(begin_date: str, end_date: str) -> Tuple[int, int]:
    lo = datetime.strptime(begin_date, "%Y-%m-%d").replace(tzinfo=_BJT)
    hi = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=_BJT
    )
    return int(lo.timestamp()), int(hi.timestamp())


def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _uid_terms(uid: str) -> List[Any]:
    terms: List[Any] = [str(uid)]
    if str(uid).isdigit():
        try:
            terms.append(int(uid))
        except ValueError:
            pass
    return terms


async def fetch_tag_uids(rest, tag: str) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []

    def consume(rows):
        for row in rows or []:
            uid = row.get("user_id") if isinstance(row, dict) else None
            if uid is None:
                continue
            s = str(uid)
            if s not in seen:
                seen.add(s)
                out.append(s)

    first = await rest.fetch_tag_list(tag=tag, page=1, page_size=1000)
    consume(first.get("data"))
    total = int((first.get("pager") or {}).get("total_page", 1))
    for page in range(2, total + 1):
        await asyncio.sleep(PAGE_SLEEP)
        consume((await rest.fetch_tag_list(tag=tag, page=page, page_size=1000)).get("data"))
    return out


def _make_rest():
    root = WEBSEA_REPO_ROOT
    if root and root not in sys.path:
        sys.path.insert(0, root)
    from crypto_center.client.rest.websea.contract_pro import (  # noqa: WPS433
        WebseaContractNew as Contract,
    )

    rest = Contract(RISK_TOKEN, RISK_SECRET, dev=False)
    rest.DEBUG = False
    return rest


async def load_c_users() -> List[str]:
    if MANUAL_UIDS:
        uids = [str(u) for u in MANUAL_UIDS]
        print("使用 MANUAL_UIDS: {} 个".format(len(uids)))
        return uids

    rest = _make_rest()
    uids = await fetch_tag_uids(rest, TAG)
    print("{} 组用户数: {}  ids={}".format(TAG, len(uids), uids))
    return uids


def _row_unrealized(row: dict) -> float:
    v = row.get("profit_loss")
    if v is None:
        v = row.get("profitLoss")
    return _to_float(v, 0.0)


async def fetch_uids_unrealized(
    uids: List[str], page_size: int = 100
) -> Tuple[Dict[str, float], float]:
    """拉全市场持仓，汇总目标 uid 的未实现盈亏。返回 (uid->pnl, 合计)。"""
    want = {str(u) for u in uids}
    per_uid: Dict[str, float] = {u: 0.0 for u in want}
    rest = _make_rest()
    last_page = 0
    while True:
        try:
            if last_page == 0:
                data = await rest.fetch_hold_list(page_size=page_size)
                total_page = int(data["pager"]["total_page"])
                for row in data.get("data") or []:
                    uid = str(row.get("user_id", ""))
                    if uid in want:
                        per_uid[uid] = per_uid.get(uid, 0.0) + _row_unrealized(row)
            begin = 2 if last_page == 0 else last_page
            for n in range(begin, total_page + 1):
                last_page = n
                data = await rest.fetch_hold_list(page=n, page_size=page_size)
                for row in data.get("data") or []:
                    uid = str(row.get("user_id", ""))
                    if uid in want:
                        per_uid[uid] = per_uid.get(uid, 0.0) + _row_unrealized(row)
                await asyncio.sleep(PAGE_SLEEP)
            break
        except Exception as e:
            print("拉取持仓未实现盈亏失败(重试): {}".format(e))
            await asyncio.sleep(5)
    total = round(sum(per_uid.values()), 2)
    return per_uid, total


def _parse_balance(deal: dict, uid: str) -> Optional[float]:
    """取该用户在本笔成交上的 balance 字段（非空且可解析）。"""
    if str(deal.get("takerUser")) == uid or str(deal.get("takerAccount")) == uid:
        raw = deal.get("takerBalance")
    elif str(deal.get("makerUser")) == uid or str(deal.get("makerAccount")) == uid:
        raw = deal.get("makerBalance")
    else:
        return None
    if raw is None or str(raw).strip() == "":
        return None
    v = _to_float(raw, default=float("nan"))
    if v != v:  # NaN
        return None
    return v


async def fetch_user_deals(collection, uid: str, ts_min: int, ts_max: int) -> List[dict]:
    uid_terms = _uid_terms(uid)
    ts_rng = {"$gte": ts_min, "$lte": ts_max}
    query = {
        "$or": [
            {"takerUser": {"$in": uid_terms}, "ts": ts_rng},
            {"makerUser": {"$in": uid_terms}, "ts": ts_rng},
        ]
    }
    cursor = collection.find(query).sort("ts", 1)
    return [d async for d in cursor]


async def fetch_user_balance_before(
    collection, uid: str, ts_before: int, scan_limit: int = 200
) -> Optional[float]:
    """取 ts < ts_before 时间序上最近一笔非空且 >0 的 balance，作区间初始资金。"""
    uid_terms = _uid_terms(uid)
    query = {
        "$or": [
            {"takerUser": {"$in": uid_terms}, "ts": {"$lt": ts_before}},
            {"makerUser": {"$in": uid_terms}, "ts": {"$lt": ts_before}},
        ]
    }
    cursor = collection.find(query).sort("ts", -1).limit(scan_limit)
    async for deal in cursor:
        bal = _parse_balance(deal, str(uid))
        if bal is not None and bal > 0:
            return bal
    return None


def deals_to_order_rows(uid: str, deals: List[dict]) -> Tuple[List[list], float, Optional[float]]:
    """
    按订单聚合成交行；同时累计盈亏，并取时间序上首个非空 balance 作初始资金。
    返回 (user_list行, profit_sum, initial_balance)。
    """
    buy_sell_side = BUY_SELL_SIDE
    deals_by_order_id: Dict[str, list] = {}
    profit_sum = 0.0
    initial_balance: Optional[float] = None
    deal_num = 0

    for deal in deals:
        if initial_balance is None:
            bal = _parse_balance(deal, uid)
            if bal is not None and bal > 0:
                initial_balance = bal

        taker = str(deal.get("takerUser", ""))
        maker = str(deal.get("makerUser", ""))
        price = _to_float(deal.get("price"))
        amount = _to_float(deal.get("amount"))
        face_value = _to_float(deal.get("takerFaceValue") or deal.get("makerFaceValue"), 1.0)
        maker_is_protected = deal.get("makerIsProtected", 0)
        taker_is_protected = deal.get("takerIsProtected", 0)
        maker_sub_id = deal.get("makerSubId", 0)
        taker_sub_id = deal.get("takerSubId", 0)

        if taker == uid:
            side_type = deal.get("takerBuyOrSell")
            fee = deal.get("takerFee")
            profit = deal.get("takerProfitLoss")
            multiple = deal.get("takerMultiple")
            order_id = deal.get("takerOrder")
            deal_is_protected = taker_is_protected
            deal_sub_id = taker_sub_id
            face_value = _to_float(deal.get("takerFaceValue"), face_value)
        elif maker == uid:
            side_type = deal.get("makerBuyOrSell")
            fee = deal.get("makerFee")
            profit = deal.get("makerProfitLoss")
            multiple = deal.get("makerMultiple")
            order_id = deal.get("makerOrder")
            deal_is_protected = maker_is_protected
            deal_sub_id = maker_sub_id
            face_value = _to_float(deal.get("makerFaceValue"), face_value)
        else:
            continue

        deal_num += 1
        side = buy_sell_side.get(str(side_type), str(side_type))
        profit_sum += _to_float(profit)
        date = datetime.utcfromtimestamp(int(deal["ts"])) + timedelta(hours=8)
        date = date.strftime("%Y-%m-%d %H:%M:%S")
        row = [
            date,
            deal.get("symbol"),
            side,
            price,
            amount,
            _to_float(profit),
            _to_float(fee),
            taker,
            maker,
            multiple,
            face_value,
            deal_is_protected,
            deal_sub_id,
        ]
        deals_by_order_id.setdefault(str(order_id), []).append(row)

    user_list: List[list] = []
    trade_type_stats = Counter()
    for order_id, od in deals_by_order_id.items():
        date = od[0][0]
        symbol = od[0][1]
        side = od[0][2]
        multiple = od[0][9]
        face_value = float(od[0][10] or 1)
        amount = sum(float(i[4]) for i in od) * face_value
        amt = sum(float(i[3]) * float(i[4]) for i in od) * face_value
        taker_amt = sum(float(i[3]) * float(i[4]) for i in od if str(i[7]) == uid) * face_value
        maker_amt = sum(float(i[3]) * float(i[4]) for i in od if str(i[8]) == uid) * face_value
        price = round(amt / amount, 6) if amount else 0.0
        profit = sum(float(i[5]) for i in od)
        fee = sum(float(i[6]) for i in od)
        taker_num = len([1 for i in od if str(i[7]) == uid])
        maker_num = len([1 for i in od if str(i[8]) == uid])
        for deal in od:
            deal_is_protected, deal_sub_id = deal[11], deal[12]
            if deal_is_protected == 0 and deal_sub_id == 0:
                trade_type_stats["自主交易"] += 1
            elif deal_is_protected != 0 and deal_sub_id != 0:
                trade_type_stats["跟单交易"] += 1
            else:
                trade_type_stats["其他"] += 1

        user_list.append(
            [
                uid,
                date,
                symbol,
                side,
                float(profit),
                float(price),
                float(amount),
                amt,
                int(_to_float(multiple)),
                fee,
                taker_num,
                maker_num,
                taker_amt,
                maker_amt,
                dict(trade_type_stats),
            ]
        )

    return user_list, profit_sum, initial_balance


def _ensure_amt(df: pd.DataFrame) -> pd.Series:
    if "amt" in df.columns and pd.api.types.is_numeric_dtype(df["amt"]):
        return df["amt"].astype(float)
    return df["price"].astype(float) * df["amount"].astype(float)


def max_consecutive(arr) -> int:
    max_cnt = cnt = 0
    for x in arr:
        if x:
            cnt += 1
            max_cnt = max(max_cnt, cnt)
        else:
            cnt = 0
    return max_cnt


def calc_trade_stats(
    df: pd.DataFrame,
    userid=None,
    initial_balance: Optional[float] = None,
) -> dict:
    """在 mongdb_order.calc_trade_stats 基础上增加初始资金 / 回撤% / 总收益率。"""
    d = df.copy()
    positions = {"long": deque(), "short": deque()}
    round_trip_count = {"long": 0, "short": 0}
    in_position = {"long": False, "short": False}
    holding_records = []

    d["ts"] = pd.to_datetime(d["tsText"])
    d = d.sort_values("ts").reset_index(drop=True)

    for _, r in d.iterrows():
        ts, side, amt = r["ts"], r["buy_sell"], float(r["amount"])
        if side == "开多":
            if not in_position["long"]:
                in_position["long"] = True
                positions["long"].append({"open_time": ts, "amount": amt})
        elif side == "开空":
            if not in_position["short"]:
                in_position["short"] = True
                positions["short"].append({"open_time": ts, "amount": amt})
        elif side in ("平多", "平空"):
            key = "long" if side == "平多" else "short"
            remain = amt
            while remain > 0 and positions[key]:
                pos = positions[key][0]
                close_qty = min(remain, pos["amount"])
                holding_records.append(
                    {
                        "direction": key,
                        "open_time": pos["open_time"],
                        "close_time": ts,
                        "amount": close_qty,
                        "holding_seconds": (ts - pos["open_time"]).total_seconds(),
                    }
                )
                pos["amount"] -= close_qty
                remain -= close_qty
                if pos["amount"] == 0:
                    positions[key].popleft()
            if side == "平多" and not positions["long"] and in_position["long"]:
                round_trip_count["long"] += 1
                in_position["long"] = False
            if side == "平空" and not positions["short"] and in_position["short"]:
                round_trip_count["short"] += 1
                in_position["short"] = False

    holding_df = pd.DataFrame(holding_records)
    if userid is not None and "userid" in d.columns:
        d = d[d["userid"].astype(str) == str(userid)].copy()

    d["amt_"] = _ensure_amt(d).astype(float)
    d["fee_"] = (
        pd.to_numeric(d["fee"], errors="coerce").fillna(0.0)
        if "fee" in d.columns
        else 0.0
    )

    trade_type_stats = dict(d["trade_type_stats"].iloc[0]) if len(d) else {}
    trade_count = len(d)
    open_count = int(d["buy_sell"].isin(["开多", "开空"]).sum())
    close_count = int(d["buy_sell"].isin(["平多", "平空"]).sum())
    total_round_trips = round_trip_count["long"] + round_trip_count["short"]
    pnl_df = d[d["profit_loss"] != 0]
    total_profit = round(float(pnl_df["profit_loss"].sum()), 2) if len(pnl_df) else 0.0
    total_fee = round(float(d["fee"].sum()), 2) if "fee" in d.columns else 0.0
    max_profit = round(float(pnl_df["profit_loss"].max()), 2) if len(pnl_df) else 0.0
    max_loss = round(float(pnl_df["profit_loss"].min()), 2) if len(pnl_df) else 0.0
    win_rate = (
        str(round((pnl_df["profit_loss"] > 0).mean() * 100, 1)) + "%"
        if len(pnl_df)
        else "0%"
    )
    if len(pnl_df) and (pnl_df["profit_loss"] < 0).any() and (pnl_df["profit_loss"] > 0).any():
        profit_loss_ratio = round(
            float(pnl_df[pnl_df["profit_loss"] > 0]["profit_loss"].mean())
            / abs(float(pnl_df[pnl_df["profit_loss"] < 0]["profit_loss"].mean())),
            2,
        )
    else:
        profit_loss_ratio = 0.0
    expect_profit = round(float(pnl_df["profit_loss"].mean()), 2) if len(pnl_df) else 0.0
    total_trade_amount = round(float(d["amt"].sum()), 2)
    profit_trade_ratio = (
        str(round(total_profit / total_trade_amount * 100, 4)) + "%"
        if total_trade_amount
        else "0%"
    )
    taker_amt = round(float(d["taker_amt"].sum()), 2)
    maker_amt = round(float(d["maker_amt"].sum()), 2)
    tm = taker_amt + maker_amt

    # 持仓时长
    if len(holding_df) and holding_df["amount"].sum() > 0:
        holding_df["holding_minutes"] = holding_df["holding_seconds"] / 60
        weighted_avg_holding = (
            holding_df["holding_seconds"] * holding_df["amount"]
        ).sum() / holding_df["amount"].sum()
        max_holding = str(pd.Timedelta(seconds=float(holding_df["holding_seconds"].max())))
        min_holding = str(pd.Timedelta(seconds=float(holding_df["holding_seconds"].min())))
        median_holding = str(
            pd.Timedelta(minutes=float(holding_df["holding_minutes"].median()))
        )
        rounded = holding_df["holding_minutes"].round()
        mode_val = float(rounded.mode()[0]) if not rounded.mode().empty else 0.0
        mode_holding = str(pd.Timedelta(minutes=mode_val))
        mode_count = int((rounded == mode_val).sum())
        mode_ratio = (
            str(round(mode_count / total_round_trips * 100, 2)) + "%"
            if total_round_trips
            else "0%"
        )
        quantiles = holding_df["holding_minutes"].quantile(
            [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0]
        )
        weighted_avg_holding = str(pd.Timedelta(seconds=float(weighted_avg_holding)))
        holding_round_trips = int((holding_df["holding_seconds"] < 300).sum())
        holding_round_trips_ratio = (
            str(round(holding_round_trips / total_round_trips * 100, 2)) + "%"
            if total_round_trips
            else "0%"
        )
    else:
        max_holding = min_holding = median_holding = mode_holding = weighted_avg_holding = "-"
        mode_count = 0
        mode_ratio = holding_round_trips_ratio = "0%"
        quantiles = pd.Series(dtype=float)

    profits = pnl_df["profit_loss"] if len(pnl_df) else pd.Series(dtype=float)
    max_win_streak = max_consecutive(profits > 0) if len(profits) else 0
    max_loss_streak = max_consecutive(profits < 0) if len(profits) else 0

    symbol_profit = (
        d[d["profit_loss"] != 0]
        .groupby("symbol")["profit_loss"]
        .sum()
        .sort_values(ascending=False)
        if len(d)
        else pd.Series(dtype=float)
    )
    symbol_profit_info = "\n"
    for symbol, profit in symbol_profit.items():
        symbol_profit_info += "{}:{}\n".format(symbol, round(float(profit), 2))

    avg_leverage = round(float(d["multiple"].mean()), 2) if len(d) else 0.0
    if len(profits) > 1 and float(profits.std() or 0) != 0:
        sharpe = round(float(profits.mean() / profits.std() * np.sqrt(len(profits))), 2)
    else:
        sharpe = 0.0

    # ---- 初始资金 & 权益曲线回撤 / 总收益率 ----
    init = float(initial_balance) if initial_balance and initial_balance > 0 else None
    if len(profits):
        cum_pnl = profits.cumsum()
        if init:
            equity = init + cum_pnl
            peak = equity.cummax()
            dd_abs = equity - peak
            max_drawdown_abs = round(float(dd_abs.min()), 2)
            dd_pct = ((equity - peak) / peak.replace(0, np.nan)).fillna(0)
            max_drawdown_pct = str(round(float(dd_pct.min()) * 100, 2)) + "%"
            total_return_pct = str(round(total_profit / init * 100, 2)) + "%"
        else:
            # 无初始资金时退回累计盈亏回撤
            peak = cum_pnl.cummax()
            max_drawdown_abs = round(float((cum_pnl - peak).min()), 2)
            max_drawdown_pct = "-"
            total_return_pct = "-"
    else:
        max_drawdown_abs = 0.0
        max_drawdown_pct = "0%" if init else "-"
        total_return_pct = "0%" if init else "-"

    d["date"] = d["ts"].dt.date
    d["week"] = d["ts"].dt.to_period("W")
    d["month"] = d["ts"].dt.to_period("M")
    daily_freq = round(float(d.groupby("date").size().mean()), 2) if len(d) else 0.0
    weekly_freq = round(float(d.groupby("week").size().mean()), 2) if len(d) else 0.0
    monthly_freq = round(float(d.groupby("month").size().mean()), 2) if len(d) else 0.0

    return {
        "账户初始金额": round(init, 2) if init else None,
        "总收益率": total_return_pct,
        "最大回撤(金额)": max_drawdown_abs,
        "最大回撤(比例)": max_drawdown_pct,
        "成交笔数(按订单算)": trade_count,
        "开仓次数(按订单算)": open_count,
        "平仓次数(按订单算)": close_count,
        "总交易轮次(开平算一次)": total_round_trips,
        "交易性质": trade_type_stats,
        "taker金额": taker_amt,
        "taker金额占比": (str(round(taker_amt / tm * 100, 2)) + "%") if tm else "0%",
        "maker金额": maker_amt,
        "maker金额占比": (str(round(maker_amt / tm * 100, 2)) + "%") if tm else "0%",
        "已实现盈亏": total_profit,
        "刨除手续费总盈亏": round(total_profit - total_fee, 2),
        "总手续费": total_fee,
        "手续费占比": (
            str(round(total_fee / total_profit * 100, 2)) + "%"
            if total_profit
            else "-"
        ),
        "分币对盈亏": symbol_profit_info,
        "最大盈利": max_profit,
        "最大亏损": max_loss,
        "胜率": win_rate,
        "盈亏比": profit_loss_ratio,
        "平均杠杆": avg_leverage,
        "夏普比率": sharpe,
        "期望收益": expect_profit,
        "总成交金额": total_trade_amount,
        "盈利/成交额": profit_trade_ratio,
        "最大持仓时长": max_holding,
        "最短持仓时长": min_holding,
        "平均持仓时长": weighted_avg_holding,
        "中位数持仓时长": median_holding,
        "众数持仓时长(聚合到min)": mode_holding,
        "众数持仓时长(出现次数)": mode_count,
        "众数持仓时长(占比)": mode_ratio,
        "分位数持仓时长": quantiles,
        "持仓时间小于5min的交易轮次占比": holding_round_trips_ratio,
        "最大连续盈利": max_win_streak,
        "最大连续亏损": max_loss_streak,
        "日均交易次数": daily_freq,
        "周均交易次数": weekly_freq,
        "月均交易次数": monthly_freq,
    }


async def run() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    os.makedirs(OUT_DIR, exist_ok=True)
    ts_min, ts_max = _bjt_date_range_to_ts(BEGIN_DATE, END_DATE)
    print("=" * 72)
    print("C组用户成交报告")
    print("时间范围(BJT): [{} ~ {}]".format(BEGIN_DATE, END_DATE))
    print("=" * 72)

    try:
        uids = await load_c_users()
    except Exception as e:
        print("拉取 C 组用户失败: {}".format(e))
        if not MANUAL_UIDS:
            return 1
        uids = [str(u) for u in MANUAL_UIDS]

    if not uids:
        print("无 C 组用户")
        return 1

    import motor.motor_asyncio

    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLL]

    all_stats_rows: List[dict] = []
    rank_rows: List[dict] = []
    all_realized = 0.0

    try:
        unrealized_by_uid, all_unrealized = await fetch_uids_unrealized(uids)
    except Exception as e:
        print("拉取未实现盈亏失败: {}".format(e))
        unrealized_by_uid, all_unrealized = {}, 0.0

    cols = [
        "userid",
        "tsText",
        "symbol",
        "buy_sell",
        "profit_loss",
        "price",
        "amount",
        "amt",
        "multiple",
        "fee",
        "taker_num",
        "maker_num",
        "taker_amt",
        "maker_amt",
        "trade_type_stats",
    ]

    for usr_id in uids:
        uid = str(usr_id)
        deals = await fetch_user_deals(coll, uid, ts_min, ts_max)
        user_list, profit_sum, initial_balance = deals_to_order_rows(uid, deals)
        unreal = round(float(unrealized_by_uid.get(uid, 0.0)), 2)

        if not user_list:
            # 区间内无成交：用区间开始前最近 balance 作初始资金；0/无则不展示
            init_bal = await fetch_user_balance_before(coll, uid, ts_min)
            if init_bal is None or float(init_bal) <= 0:
                continue
            init_equity = round(float(init_bal), 2)
            cur_equity = round(init_equity + unreal, 2)
            rank_rows.append(
                {
                    "uid": uid,
                    "已实现盈亏": 0.0,
                    "未实现盈亏": unreal,
                    "初始权益": init_equity,
                    "当前权益": cur_equity,
                    "收益率": "-",
                }
            )
            all_stats_rows.append(
                {
                    "user_id": uid,
                    "标记组": TAG,
                    "账户初始金额": init_equity,
                    "已实现盈亏": 0.0,
                    "未实现盈亏": unreal,
                    "初始权益": init_equity,
                    "当前权益": cur_equity,
                    "总收益率": "-",
                }
            )
            continue

        all_realized += profit_sum
        df = pd.DataFrame(user_list, columns=cols)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            stats = calc_trade_stats(df, userid=uid, initial_balance=initial_balance)

        realized = float(stats.get("已实现盈亏") or 0)
        ret = stats.get("总收益率") or "-"
        init = stats.get("账户初始金额")
        if init is not None:
            init_equity = round(float(init), 2)
            cur_equity = round(float(init) + realized + unreal, 2)
        else:
            init_equity = "-"
            cur_equity = "-"
        rank_rows.append(
            {
                "uid": uid,
                "已实现盈亏": realized,
                "未实现盈亏": unreal,
                "初始权益": init_equity,
                "当前权益": cur_equity,
                "收益率": ret,
            }
        )

        row = {"user_id": uid, "标记组": TAG}
        row.update({k: v for k, v in stats.items() if k != "分位数持仓时长"})
        row["未实现盈亏"] = unreal
        row["初始权益"] = init_equity
        row["当前权益"] = cur_equity
        if "分位数持仓时长" in stats and hasattr(stats["分位数持仓时长"], "to_dict"):
            row["分位数持仓时长"] = str(dict(stats["分位数持仓时长"]))
        all_stats_rows.append(row)

    print(
        "{}组合计用户={} 已实现盈亏={} 未实现盈亏={}".format(
            TAG, len(uids), int(all_realized), int(all_unrealized)
        )
    )
    print("-" * 72)
    # 有成交用户按已实现盈亏降序
    rank_rows.sort(key=lambda x: x["已实现盈亏"], reverse=True)
    for r in rank_rows:
        print(
            "uid={} 已实现盈亏={} 未实现盈亏={} 初始权益={} 当前权益={} 收益率={}".format(
                r["uid"],
                r["已实现盈亏"],
                r["未实现盈亏"],
                r["初始权益"],
                r["当前权益"],
                r["收益率"],
            )
        )
    if not rank_rows:
        print("(区间内无成交用户)")

    if all_stats_rows:
        for r in all_stats_rows:
            if isinstance(r.get("交易性质"), dict):
                r["交易性质"] = str(r["交易性质"])
            if isinstance(r.get("分币对盈亏"), str):
                r["分币对盈亏"] = r["分币对盈亏"].replace("\n", " | ").strip(" |")
        stats_df = pd.DataFrame(all_stats_rows)
        if "已实现盈亏" in stats_df.columns:
            stats_df = stats_df.sort_values("已实现盈亏", ascending=False)
        front = [
            "user_id",
            "标记组",
            "初始权益",
            "当前权益",
            "已实现盈亏",
            "未实现盈亏",
            "总收益率",
            "最大回撤(金额)",
            "最大回撤(比例)",
        ]
        cols_out = front + [c for c in stats_df.columns if c not in front]
        stats_df = stats_df[[c for c in cols_out if c in stats_df.columns]]
        stats_df.to_csv(STATS_CSV, index=False, encoding="utf-8-sig")
        print("-" * 72)
        print("汇总 CSV: {}".format(STATS_CSV))
    print("=" * 72)
    return 0


def main() -> int:
    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())

