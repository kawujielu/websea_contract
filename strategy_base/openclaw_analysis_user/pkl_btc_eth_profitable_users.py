# -*- coding: utf-8 -*-
"""
从日切 PKL（mongo_daily_deals_dump 格式）统计指定日期范围内，BTC / ETH 永续（USDT 本位）合约上
「整体盈亏」与平仓笔数层面的胜率、盈亏比。

口径（与 pkl_positions_cost_dist.replay_pkl 一致）：
- 多头：开多累加数量与成本；平多按均价减仓，单笔已实现 = (成交价 - 持仓均价) * 平仓币量。
- 空头：开空累加数量与成本；平空按均价减仓，单笔已实现 = (持仓均价 - 成交价) * 平仓币量。
- 整体盈亏 = 区间内所有平仓已实现之和 + 区间末日最后一笔有效成交价上的未实现盈亏（多/空分别估值）。
- 胜率 / 盈亏比仅统计「平仓事件」（平多、平空）；盈亏绝对值小于 eps 的平仓记为和局，不参与胜率分母与盈亏比分子分母。

输出：
1) 整体盈亏 > 阈值（默认 5000 USDT）的用户 id 列表与人数，BTC、ETH 分别统计。
2) 在 1) 前提下，且 胜率 > 0.7、盈亏比 > 2、平仓笔数 > 30（默认，可调）的用户 id 列表与人数，BTC、ETH 分别统计。
   盈亏比 = 盈利平仓盈亏合计 / 亏损平仓盈亏绝对值合计（无亏损平仓且盈利合计 > 0 视为满足 > 2）。
   平仓笔数 = 平多/平空事件总次数（与 close_pnls 长度一致）。
条件 1 与 2 均要求：该用户在该合约（BTC 或 ETH）上最后一笔成交日期落在「以 --end-date 为结束日的连续 N 个自然日」内（默认 N=3，含 end-date；可调 --last-trade-within-days）。

依赖：pandas、numpy。Python 3.9+（已在 3.9.19 下兼容写法）。

示例：
  python pkl_btc_eth_profitable_users.py --data-dir ./data/mongo_daily_deals_pkl
  python pkl_btc_eth_profitable_users.py --data-dir ... --start-date 2026-01-01 --end-date 2026-04-11 --output-dir ./out
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402

DEFAULT_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

BUY_SELL_CN = {"开多", "开空", "平多", "平空"}
NUM_SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}
EPS = 1e-9
EPS_CLOSE = 1e-6  # 平仓盈亏视为和局的阈值（USDT 量级）


def ensure_user_id(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d
    if "taker_user" not in d.columns or "maker_user" not in d.columns:
        raise ValueError("pkl 缺少 user_id / taker_user / maker_user")
    d = d.reset_index(drop=True)
    grp_cols = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    for c in grp_cols:
        if c not in d.columns:
            raise ValueError("pkl 缺少字段 {}".format(c))
    d["_idx_in_pair"] = d.groupby(grp_cols).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    d.loc[(d["_idx_in_pair"] % 2) == 1, "user_id"] = d.loc[
        (d["_idx_in_pair"] % 2) == 1, "maker_user"
    ].astype(str)
    d = d.drop(columns=["_idx_in_pair"])
    return d


def _norm_symbol_key(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", (s or "").strip()).upper()


def symbol_bucket(sym: str) -> Optional[str]:
    """仅识别主站常见 BTC/ETH USDT 永续合约名。"""
    k = _norm_symbol_key(sym)
    if k == "BTCUSDT":
        return "BTC"
    if k == "ETHUSDT":
        return "ETH"
    return None


def list_pkl_paths(data_dir: str) -> List[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError("PKL 目录不存在: {}".format(data_dir))
    out: List[str] = []
    for fn in sorted(os.listdir(data_dir)):
        if not fn.endswith(".pkl"):
            continue
        stem = fn[:-4]
        try:
            datetime.strptime(stem, "%Y-%m-%d")
        except ValueError:
            continue
        out.append(os.path.join(data_dir, fn))
    return out


def filter_paths_by_date(paths: List[str], start_d: date, end_d: date) -> List[str]:
    out: List[str] = []
    for p in paths:
        stem = os.path.basename(p)[:-4]
        try:
            d = datetime.strptime(stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if start_d <= d <= end_d:
            out.append(p)
    return out


def load_concat_pkls(paths: List[str]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    dfs = [pd.read_pickle(p) for p in paths]
    return pd.concat(dfs, ignore_index=True)


def _row_trade_date(row: pd.Series) -> Optional[date]:
    ts = row.get("ts_text")
    if ts is None:
        return None
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return None
    return t.date()


def _last_trade_within_calendar_days(
    last_d: Optional[date], ref_end: date, n: int
) -> bool:
    """last_d 落在 [ref_end - (n-1), ref_end] 闭区间内（共 n 个自然日）。"""
    if last_d is None or n < 1:
        return False
    start = ref_end - timedelta(days=n - 1)
    return start <= last_d <= ref_end


def _normalize_buy_sell(raw: object) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    s = str(raw).strip()
    if s in BUY_SELL_CN:
        return s
    if s in NUM_SIDE:
        return NUM_SIDE[s]
    return None


@dataclass
class LegState:
    lq: float = 0.0
    lc: float = 0.0
    sq: float = 0.0
    sc: float = 0.0
    close_pnls: List[float] = field(default_factory=list)


def replay_btc_eth_pnls(
    df: pd.DataFrame,
) -> Tuple[Dict[Tuple[str, str, str], LegState], Dict[str, float], Dict[Tuple[str, str], date]]:
    """
    仅处理 symbol 属于 BTC/ETH USDT 的腿。返回 state[(uid, bucket, raw_symbol)]、last_px[raw_symbol]、
    last_trade_d[(uid, bucket)]（该用户在 BTC/ETH 桶内任意成交腿的最大成交日，含 buy_sell 无法解析的腿）。
    """
    state: Dict[Tuple[str, str, str], LegState] = {}
    last_px: Dict[str, float] = {}
    last_trade_d: Dict[Tuple[str, str], date] = {}

    work = df.sort_values("ts_text", kind="mergesort").reset_index(drop=True)

    for i in range(work.shape[0]):
        row = work.iloc[i]
        uid = str(row["user_id"])
        sym = str(row["symbol"])
        bucket = symbol_bucket(sym)
        if bucket is None:
            continue

        td = _row_trade_date(row)
        if td is not None:
            kb = (uid, bucket)
            old = last_trade_d.get(kb)
            last_trade_d[kb] = td if old is None else max(old, td)

        side = _normalize_buy_sell(row.get("buy_sell"))
        pr = float(pd.to_numeric(row.get("price"), errors="coerce") or 0.0)
        amt = float(pd.to_numeric(row.get("amount"), errors="coerce") or 0.0)
        fv = float(pd.to_numeric(row.get("face_value"), errors="coerce") or 0.0)
        if fv <= 0:
            fv = 1.0
        coin_qty = amt * fv

        if side is None:
            continue

        key = (uid, bucket, sym)
        st = state.setdefault(key, LegState())

        if side == "开多":
            st.lq += coin_qty
            st.lc += pr * coin_qty
        elif side == "平多":
            q = min(coin_qty, st.lq)
            if st.lq > EPS and q > EPS:
                avg = st.lc / st.lq
                pnl = (pr - avg) * q
                st.close_pnls.append(float(pnl))
                st.lc -= avg * q
                st.lq -= q
        elif side == "开空":
            st.sq += coin_qty
            st.sc += pr * coin_qty
        elif side == "平空":
            q = min(coin_qty, st.sq)
            if st.sq > EPS and q > EPS:
                avg = st.sc / st.sq
                pnl = (avg - pr) * q
                st.close_pnls.append(float(pnl))
                st.sc -= avg * q
                st.sq -= q
        else:
            continue

        if st.lq < EPS:
            st.lq = 0.0
            st.lc = 0.0
        if st.sq < EPS:
            st.sq = 0.0
            st.sc = 0.0

        if pr > 0:
            last_px[sym] = pr

    return state, last_px, last_trade_d


def unrealized_usdt(st: LegState, sym: str, last_px: Dict[str, float]) -> float:
    lp = float(last_px.get(sym) or 0.0)
    if lp <= EPS:
        return 0.0
    u = 0.0
    if st.lq > EPS:
        avg = st.lc / st.lq
        u += (lp - avg) * st.lq
    if st.sq > EPS:
        avg = st.sc / st.sq
        u += (avg - lp) * st.sq
    return float(u)


def aggregate_user_bucket(
    state: Dict[Tuple[str, str, str], LegState],
    last_px: Dict[str, float],
    last_trade_d: Dict[Tuple[str, str], date],
) -> Dict[Tuple[str, str], Dict[str, object]]:
    """
    同一用户在同一 bucket（BTC/ETH）下可能多条 raw_symbol，合并平仓列表与总盈亏。
    """
    out: Dict[Tuple[str, str], Dict[str, object]] = {}
    for (uid, bucket, sym), st in state.items():
        k = (uid, bucket)
        if k not in out:
            out[k] = {"close_pnls": [], "unrealized": 0.0}
        o = out[k]
        pnls: List[float] = o["close_pnls"]  # type: ignore[assignment]
        pnls.extend(st.close_pnls)
        o["unrealized"] = float(o["unrealized"]) + unrealized_usdt(st, sym, last_px)  # type: ignore[arg-type]

    for k, o in out.items():
        realized = float(sum(o["close_pnls"]))  # type: ignore[arg-type]
        o["realized"] = realized
        o["total_pnl"] = realized + float(o["unrealized"])  # type: ignore[arg-type]
        o["last_trade_date"] = last_trade_d.get(k)
    return out


def win_rate_and_pl_ratio(close_pnls: List[float]) -> Tuple[Optional[float], Optional[float]]:
    wins = 0
    losses = 0
    gross_profit = 0.0
    gross_loss = 0.0
    for p in close_pnls:
        if p > EPS_CLOSE:
            wins += 1
            gross_profit += p
        elif p < -EPS_CLOSE:
            losses += 1
            gross_loss += -p
    denom = wins + losses
    if denom == 0:
        return None, None
    wr = wins / denom
    if gross_loss > EPS:
        plr = gross_profit / gross_loss
    else:
        plr = float("inf") if gross_profit > EPS else 0.0
    return wr, plr


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PKL 统计 BTC/ETH 合约盈利用户（总盈亏阈值 + 胜率/盈亏比）"
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="起始日期 YYYY-MM-DD；默认当年 1 月 1 日",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="结束日期 YYYY-MM-DD（含）；默认今天",
    )
    p.add_argument(
        "--min-total-pnl",
        type=float,
        default=5000.0,
        help="条件 1：整体盈亏阈值（USDT），默认 5000",
    )
    p.add_argument(
        "--min-win-rate",
        type=float,
        default=0.7,
        help="条件 2：胜率下限，默认 0.7",
    )
    p.add_argument(
        "--min-pl-ratio",
        type=float,
        default=2.0,
        help="条件 2：盈亏比下限，默认 2",
    )
    p.add_argument(
        "--min-close-trades",
        type=int,
        default=30,
        help="条件 2：平仓笔数须严格大于该值（默认 30 即至少 31 笔平多/平空）",
    )
    p.add_argument(
        "--last-trade-within-days",
        type=int,
        default=3,
        help="条件 1/2：最后一笔 BTC/ETH 成交日须在 end-date 起向前共 N 个自然日内（默认 3，含 end-date）",
    )
    p.add_argument(
        "--no-exclude",
        action="store_true",
        help="不做做市/模拟金剔除（调试）",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default="",
        help="若指定，写入 btc_eth_profit_summary.json 与各名单 txt",
    )
    args, _ = p.parse_known_args()
    return args


def _default_start_date() -> date:
    t = date.today()
    return date(t.year, 1, 1)


def main() -> int:
    args = parse_args()
    today = date.today()
    if (args.start_date or "").strip():
        start_d = datetime.strptime(args.start_date.strip(), "%Y-%m-%d").date()
    else:
        start_d = _default_start_date()
    if (args.end_date or "").strip():
        end_d = datetime.strptime(args.end_date.strip(), "%Y-%m-%d").date()
    else:
        end_d = today
    if end_d < start_d:
        sys.stderr.write("end-date 早于 start-date\n")
        return 2

    last_trade_days = int(args.last_trade_within_days)
    if last_trade_days < 1:
        sys.stderr.write("--last-trade-within-days 须为 >= 1 的整数\n")
        return 2

    data_dir = args.data_dir
    paths_all = list_pkl_paths(data_dir)
    paths = filter_paths_by_date(paths_all, start_d, end_d)
    if not paths:
        sys.stderr.write(
            "无 PKL：{} 在 {} ~ {} 范围内无文件\n".format(data_dir, start_d, end_d)
        )
        return 1

    raw = load_concat_pkls(paths)
    if raw.shape[0] == 0:
        sys.stderr.write("PKL 合并后为空\n")
        return 1

    legs = ensure_user_id(raw)
    if not args.no_exclude:
        ex = get_excluded_user_ids_sync()
        uid = legs["user_id"].astype(str)
        legs = legs[~uid.isin(ex)].copy()
    legs = legs.sort_values("ts_text", kind="mergesort").reset_index(drop=True)

    sys.stderr.write(
        "日期 {} ~ {} 文件数={} 行数={} 过滤后行数={}\n".format(
            start_d, end_d, len(paths), raw.shape[0], legs.shape[0]
        )
    )

    if legs.shape[0] == 0:
        sys.stderr.write("过滤后无数据\n")
        return 1

    state, last_px, last_trade_d = replay_btc_eth_pnls(legs)
    agg = aggregate_user_bucket(state, last_px, last_trade_d)

    min_pnl = float(args.min_total_pnl)
    min_wr = float(args.min_win_rate)
    min_plr = float(args.min_pl_ratio)
    min_close_trades = int(args.min_close_trades)

    result: Dict[str, object] = {
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "min_total_pnl_u": min_pnl,
        "min_win_rate": min_wr,
        "min_pl_ratio": min_plr,
        "cond2_close_trades_must_exceed": min_close_trades,
        "last_trade_within_calendar_days": last_trade_days,
        "last_trade_window_note": "last_trade_date 须落在 [end_date-({}-1), end_date]（自然日）".format(
            last_trade_days
        ),
        "symbols_note": "仅 BTCUSDT、ETHUSDT（规范化后）；条件2要求 len(close_pnls) > cond2_close_trades_must_exceed",
    }

    for bucket in ("BTC", "ETH"):
        cond1_uids: List[str] = []
        cond2_uids: List[str] = []

        for (uid, b), o in agg.items():
            if b != bucket:
                continue
            ltd = o.get("last_trade_date")  # type: ignore[assignment]
            if not _last_trade_within_calendar_days(ltd, end_d, last_trade_days):
                continue
            total = float(o["total_pnl"])  # type: ignore[arg-type]
            if total <= min_pnl:
                continue
            cond1_uids.append(uid)

            pnls: List[float] = o["close_pnls"]  # type: ignore[assignment]
            if len(pnls) <= min_close_trades:
                continue
            wr, plr = win_rate_and_pl_ratio(pnls)
            if wr is None or plr is None:
                continue
            if wr > min_wr and plr > min_plr:
                cond2_uids.append(uid)

        cond1_uids.sort()
        cond2_uids.sort()
        result["{}_condition1_user_count".format(bucket)] = len(cond1_uids)
        result["{}_condition1_user_ids".format(bucket)] = cond1_uids
        result["{}_condition2_user_count".format(bucket)] = len(cond2_uids)
        result["{}_condition2_user_ids".format(bucket)] = cond2_uids

    # 打印
    print(json.dumps(result, ensure_ascii=False, indent=2))

    out_dir = (args.output_dir or "").strip()
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        jp = os.path.join(out_dir, "btc_eth_profit_summary.json")
        with open(jp, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        for bucket in ("BTC", "ETH"):
            for cond, label in (
                (1, "cond1_over_{}u".format(int(min_pnl))),
                (
                    2,
                    "cond2_wr{}_pl{}_gt{}tr_last{}d".format(
                        min_wr, min_plr, min_close_trades, last_trade_days
                    ),
                ),
            ):
                key = "{}_condition{}_user_ids".format(bucket, cond)
                ids = result[key]  # type: ignore[index]
                fp = os.path.join(out_dir, "{}_{}.txt".format(bucket.lower(), label))
                with open(fp, "w", encoding="utf-8") as f:
                    for line in ids:  # type: ignore[assignment]
                        f.write(str(line) + "\n")
        sys.stderr.write("已写目录: {}\n".format(os.path.abspath(out_dir)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
