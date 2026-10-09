# -*- coding: utf-8 -*-
"""
PKL 用户组交易相似度（离线）

从日切 PKL（mongo_daily_deals_dump 格式）读取成交腿，对指定用户列表两两计算「时间差 ≤ 容差秒 + 开平仓方向一致」
下的存在性重合率。默认在全市场所有合约腿上汇总（不区分 symbol）；加 --same-symbol 时仅同合约内匹配。

重合率定义（与典型示例一致）：
- 用户 A 相对 B：A 的每条用户腿中，若存在至少一条 B 的腿满足时间与方向（及可选 symbol）规则，则该 A 腿计为重合；
  重合度 = 重合 A 腿数 / A 总腿数，输出为百分比字符串（如 100.00%）。
- B 相对 A 对称。

依赖：pandas、numpy。Python 3.9.19 可用。

Ubuntu 测试示例：
  export UPM_PKL_DEALS_DIR=/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl
  cd /path/to/用户分析/code
  python3.9 pkl_user_group_trade_similarity.py --data-dir "$UPM_PKL_DEALS_DIR" --users 123456,345678 --print-stdout
  # 仅同合约内匹配：
  python3.9 pkl_user_group_trade_similarity.py --data-dir "$UPM_PKL_DEALS_DIR" --users 123456,345678 --same-symbol --print-stdout

本机：
  python pkl_user_group_trade_similarity.py --users 111111,222222 --print-stdout
"""
from __future__ import annotations

import argparse
import bisect
import csv
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402
from pkl_symbol_net_position_daily import (  # noqa: E402
    ensure_user_id,
    list_pkl_paths,
    load_concat_pkls,
)

DEFAULT_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

BUY_SELL_CN = {"开多", "开空", "平多", "平空"}
NUM_SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}


def _normalize_buy_sell(raw: object) -> Optional[str]:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    s = str(raw).strip()
    if s in BUY_SELL_CN:
        return s
    if s in NUM_SIDE:
        return NUM_SIDE[s]
    return None


def _ts_to_sec(ts_text: object) -> Optional[int]:
    t = pd.to_datetime(ts_text, errors="coerce")
    if pd.isna(t):
        return None
    return int(t.value // 10**9)


def build_user_legs(
    df: pd.DataFrame,
    user_ids: Sequence[str],
    same_symbol: bool,
) -> Dict[str, List[Tuple[int, str, str]]]:
    """
    返回 user_id -> [(ts_sec, symbol, side_norm), ...]。
    same_symbol 为 False 时仍带 symbol，匹配逻辑可忽略。
    """
    want = {str(u).strip() for u in user_ids if str(u).strip()}
    d = df[df["user_id"].astype(str).isin(want)].copy()
    out: Dict[str, List[Tuple[int, str, str]]] = {u: [] for u in want}
    if d.shape[0] == 0:
        return out

    for _, row in d.iterrows():
        uid = str(row["user_id"])
        side = _normalize_buy_sell(row.get("buy_sell"))
        if side is None:
            continue
        ts = _ts_to_sec(row.get("ts_text"))
        if ts is None:
            continue
        sym = str(row.get("symbol", "") or "")
        out.setdefault(uid, []).append((ts, sym, side))
    return out


def _match_count(
    legs_query: List[Tuple[int, str, str]],
    legs_ref_sorted: List[Tuple[int, str, str]],
    tol_sec: int,
    same_symbol: bool,
) -> int:
    """legs_query 中多少条在 legs_ref 中存在满足规则的对手腿。legs_ref_sorted 已按下列方式排序。"""
    if not legs_query or not legs_ref_sorted:
        return 0

    if same_symbol:
        by_sym_side: Dict[Tuple[str, str], List[int]] = defaultdict(list)
        for ts, sym, side in legs_ref_sorted:
            by_sym_side[(sym, side)].append(ts)
        for key in by_sym_side:
            by_sym_side[key].sort()

        matched = 0
        for ts_a, sym_a, side_a in legs_query:
            ts_list = by_sym_side.get((sym_a, side_a))
            if not ts_list:
                continue
            lo = bisect.bisect_left(ts_list, ts_a - tol_sec)
            hi = bisect.bisect_right(ts_list, ts_a + tol_sec)
            if lo < hi:
                matched += 1
        return matched

    # 仅按方向分桶，时间排序
    by_side: Dict[str, List[int]] = defaultdict(list)
    for ts, _sym, side in legs_ref_sorted:
        by_side[side].append(ts)
    for side in by_side:
        by_side[side].sort()

    matched = 0
    for ts_a, _sym_a, side_a in legs_query:
        ts_list = by_side.get(side_a)
        if not ts_list:
            continue
        lo = bisect.bisect_left(ts_list, ts_a - tol_sec)
        hi = bisect.bisect_right(ts_list, ts_a + tol_sec)
        if lo < hi:
            matched += 1
    return matched


def pairwise_rows(
    user_legs: Dict[str, List[Tuple[int, str, str]]],
    tol_sec: int,
    same_symbol: bool,
    base_user: Optional[str] = None,
) -> List[Dict[str, object]]:
    uids = sorted(user_legs.keys())
    rows: List[Dict[str, object]] = []
    if base_user:
        b = str(base_user).strip()
        if b not in user_legs:
            return rows
        for ub in uids:
            if ub == b:
                continue
            ua = b
            la = user_legs.get(ua) or []
            lb = user_legs.get(ub) or []
            na, nb = len(la), len(lb)
            ma = _match_count(la, list(lb), tol_sec, same_symbol)
            mb = _match_count(lb, list(la), tol_sec, same_symbol)
            ra = round(100.0 * ma / na, 2) if na else 0.0
            rb = round(100.0 * mb / nb, 2) if nb else 0.0
            rows.append(
                {
                    "user_a": ua,
                    "user_b": ub,
                    "trades_a": na,
                    "trades_b": nb,
                    "matched_a": ma,
                    "matched_b": mb,
                    "rate_a_vs_b_pct": ra,
                    "rate_b_vs_a_pct": rb,
                }
            )
        return rows
    for ua, ub in combinations(uids, 2):
        la = user_legs.get(ua) or []
        lb = user_legs.get(ub) or []
        na, nb = len(la), len(lb)
        ma = _match_count(la, list(lb), tol_sec, same_symbol)
        mb = _match_count(lb, list(la), tol_sec, same_symbol)
        ra = round(100.0 * ma / na, 2) if na else 0.0
        rb = round(100.0 * mb / nb, 2) if nb else 0.0
        rows.append(
            {
                "user_a": ua,
                "user_b": ub,
                "trades_a": na,
                "trades_b": nb,
                "matched_a": ma,
                "matched_b": mb,
                "rate_a_vs_b_pct": ra,
                "rate_b_vs_a_pct": rb,
            }
        )
    return rows


def _pct_display(v: object) -> str:
    """百分比展示，保留两位小数。"""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "0.00%"
    return "{:.2f}%".format(x)


def format_table(rows: List[Dict[str, object]], same_symbol: bool) -> str:
    mode = "同合约" if same_symbol else "全市场汇总"
    header = (
        "模式: {}\n"
        "user_a\tuser_b\ttrades_a\ttrades_b\tmatched_a\tmatched_b\t"
        "重合度_A相对B\t重合度_B相对A\n"
    ).format(mode)
    lines = [header.rstrip()]
    for r in rows:
        lines.append(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
                r["user_a"],
                r["user_b"],
                r["trades_a"],
                r["trades_b"],
                r["matched_a"],
                r["matched_b"],
                _pct_display(r["rate_a_vs_b_pct"]),
                _pct_display(r["rate_b_vs_a_pct"]),
            )
        )
    return "\n".join(lines)


def parse_users_arg(s: str) -> List[str]:
    parts = [p.strip() for p in (s or "").replace("，", ",").split(",")]
    return [p for p in parts if p]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PKL 用户组交易相似度（离线）")
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录（YYYY-MM-DD.pkl）",
    )
    p.add_argument(
        "--users",
        type=str,
        default="",
        help="逗号分隔用户 ID；留空时自动使用时间窗内所有用户",
    )
    p.add_argument(
        "--users-file",
        type=str,
        default="",
        help="每行一个用户 ID，与 --users 二选一或合并（合并后去重）",
    )
    p.add_argument("--tolerance-sec", type=int, default=60, help="时间容差（秒），默认 60")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--weeks", type=int, default=None, help="仅统计近 N 周（按7*N天）")
    g.add_argument("--months", type=int, default=None, help="仅统计近 N 月（按30*N天）")
    p.add_argument("--start", type=str, default="", help="起始日期 YYYY-MM-DD（含）")
    p.add_argument("--end", type=str, default="", help="结束日期 YYYY-MM-DD（含）")
    p.add_argument(
        "--min-rate-pct",
        type=float,
        default=0.0,
        help="仅保留双向重合度都 >= 该阈值(%%) 的用户对，默认 0",
    )
    p.add_argument(
        "--same-symbol",
        action="store_true",
        help="仅当 symbol 与方向、时间均匹配时计为重合（默认不限制合约）",
    )
    p.add_argument("--print-stdout", action="store_true", help="将结果表打印到 stdout")
    p.add_argument("--csv", type=str, default="", help="可选：写入 CSV 路径")
    p.add_argument(
        "--no-exclude",
        action="store_true",
        help="不做做市/模拟金剔除（调试用）",
    )
    args, _ = p.parse_known_args()
    return args


def _to_date(s: str) -> Optional[datetime.date]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _resolve_window(args: argparse.Namespace) -> Tuple[Optional[datetime.date], Optional[datetime.date], str]:
    """
    返回 (start_date, end_date, label)。
    users 留空且未指定时间时，默认近 7 天。
    """
    end_d = _to_date(args.end)
    start_d = _to_date(args.start)
    if start_d and end_d and start_d > end_d:
        raise ValueError("--start 不能晚于 --end")

    if args.months is not None:
        m = int(args.months)
        if m <= 0:
            raise ValueError("--months 必须为正整数")
        end_d = end_d or datetime.now().date()
        start_d = end_d - timedelta(days=m * 30 - 1)
        return start_d, end_d, "近{}月".format(m)
    if args.weeks is not None:
        w = int(args.weeks)
        if w <= 0:
            raise ValueError("--weeks 必须为正整数")
        end_d = end_d or datetime.now().date()
        start_d = end_d - timedelta(days=w * 7 - 1)
        return start_d, end_d, "近{}周".format(w)

    if start_d or end_d:
        if start_d is None or end_d is None:
            raise ValueError("使用 --start/--end 时需同时提供")
        return start_d, end_d, "{}~{}".format(start_d.isoformat(), end_d.isoformat())

    return None, None, "全部时间(默认)"


def _filter_by_date(df: pd.DataFrame, start_d: Optional[datetime.date], end_d: Optional[datetime.date]) -> pd.DataFrame:
    if df.shape[0] == 0 or start_d is None or end_d is None:
        return df
    d = df.copy()
    ts = pd.to_datetime(d.get("ts_text"), errors="coerce")
    m = ts.notna()
    if not m.any():
        return d.iloc[0:0].copy()
    dd = ts[m].dt.date
    keep = (dd >= start_d) & (dd <= end_d)
    idx = ts[m].index[keep]
    return d.loc[idx].copy()


def main() -> int:
    args = parse_args()
    users: List[str] = []
    if args.users.strip():
        users.extend(parse_users_arg(args.users))
    if (args.users_file or "").strip():
        with open(args.users_file.strip(), "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    users.append(line)
    # 去重保持顺序
    seen = set()
    uniq: List[str] = []
    for u in users:
        u = str(u).strip()
        if u and u not in seen:
            seen.add(u)
            uniq.append(u)
    users = uniq

    if len(users) < 1:
        sys.stderr.write("至少需要 1 个基准用户：使用 --users id1 或 --users-file\n")
        return 2
    base_user = users[0]
    start_d, end_d, win_label = _resolve_window(args)

    tol = max(0, int(args.tolerance_sec))
    min_rate = max(0.0, min(100.0, float(args.min_rate_pct)))
    paths = list_pkl_paths(args.data_dir)
    raw = load_concat_pkls(paths)
    sys.stderr.write("PKL 文件数={} 原始行数={} 时间窗={}\n".format(len(paths), raw.shape[0], win_label))
    if raw.shape[0] == 0:
        sys.stderr.write("无数据\n")
        return 1

    legs_df = ensure_user_id(raw)
    legs_df = _filter_by_date(legs_df, start_d, end_d)
    if not args.no_exclude:
        ex = get_excluded_user_ids_sync()
        uid = legs_df["user_id"].astype(str)
        legs_df = legs_df[~uid.isin(ex)].copy()

    all_users = sorted(legs_df["user_id"].astype(str).dropna().unique().tolist())
    if base_user not in set(all_users):
        sys.stderr.write("基准用户 {} 在该时间窗内无有效成交\n".format(base_user))
        return 1
    compare_users = all_users if len(users) == 1 else list(dict.fromkeys([base_user] + users[1:]))
    if len(compare_users) < 2:
        sys.stderr.write("可用于对比的用户不足 2 个\n")
        return 2
    sys.stderr.write("基准用户={} 对比用户数={}\n".format(base_user, len(compare_users) - 1))

    user_legs = build_user_legs(legs_df, compare_users, args.same_symbol)

    if len(user_legs.get(base_user) or []) == 0:
        sys.stderr.write("指定用户在过滤后无任何有效腿（方向/时间解析失败或无成交）\n")
        return 1

    rows = pairwise_rows(user_legs, tol, args.same_symbol, base_user=base_user)
    if min_rate > 0:
        rows = [
            r for r in rows
            if float(r.get("rate_a_vs_b_pct") or 0.0) >= min_rate
            and float(r.get("rate_b_vs_a_pct") or 0.0) >= min_rate
        ]
    text = format_table(rows, args.same_symbol)

    csv_p = (args.csv or "").strip()
    if csv_p:
        parent = os.path.dirname(os.path.abspath(csv_p))
        if parent:
            os.makedirs(parent, exist_ok=True)
        fieldnames = [
            "user_a",
            "user_b",
            "trades_a",
            "trades_b",
            "matched_a",
            "matched_b",
            "重合度_A相对B",
            "重合度_B相对A",
        ]
        with open(csv_p, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(
                    {
                        "user_a": r["user_a"],
                        "user_b": r["user_b"],
                        "trades_a": r["trades_a"],
                        "trades_b": r["trades_b"],
                        "matched_a": r["matched_a"],
                        "matched_b": r["matched_b"],
                        "重合度_A相对B": _pct_display(r["rate_a_vs_b_pct"]),
                        "重合度_B相对A": _pct_display(r["rate_b_vs_a_pct"]),
                    }
                )
        sys.stderr.write("已写 CSV: {}\n".format(os.path.abspath(csv_p)))

    if args.print_stdout or not csv_p:
        print(text)

    return 0


if __name__ == "__main__":
    sys.exit(main())
