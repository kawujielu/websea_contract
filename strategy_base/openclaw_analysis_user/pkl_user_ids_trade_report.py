# -*- coding: utf-8 -*-
"""
指定用户 ID 组 + 北京时间自然日区间，从日切 PKL 读取成交并输出**简化交易报告**（文本）。

- 配置区与 ``pkl_user_ids_trades_kline_to_html.py`` 一致：``USER_IDS``、``START_DATE``、``END_DATE``、``PKL_DATA_DIR``。
- 报告正文格式对齐 ``daily_profit_top10_kline_report`` 的 Telegram 文本（``run_profit_topn`` 单行摘要 + 可选两两重合度）。
- 不生成 K 线 HTML。Python 3.9+。

使用：修改下方配置区后执行::

  python pkl_user_ids_trade_report.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta
from typing import List, Sequence, Set, Tuple

import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

import daily_profit_top10_kline_report as dptr  # noqa: E402
from mongdb_order_stats import calc_trade_stats  # noqa: E402
from pkl_user_query_analyzer import (  # noqa: E402
    _format_profit_top_user_line_mongdb_style,
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
)

# ---------------------------------------------------------------------------
# 配置区（与 pkl_user_ids_trades_kline_to_html.py 对齐）
# ---------------------------------------------------------------------------

USER_IDS: Sequence[str] = (
    '556111', '618036', '618773', '619114', '621479', '628195', '638679', '639695', '642756', '643000', '644428', '647306', '669940', '669943', '669946', '671472', '671720'
)

START_DATE = "2026-05-01"  # YYYY-MM-DD（北京时间自然日，含首尾）
END_DATE = "2026-05-17"

PKL_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

# 报告输出目录（其下子目录：{START_DATE}_{END_DATE}/report.txt）
OUTPUT_REPORT_BASE_DIR = os.path.join(_DIR, "reports", "user_ids_trade_report")

# 仅统计 U 本位永续（symbol 以 -USDT 结尾）
ONLY_U_PERP_USDT = True

# 是否打印到 stdout
PRINT_STDOUT = True

# 用户≥2 时是否附加两两交易重合（口径同 daily_profit_top10_kline_report）
PAIR_OVERLAP_ENABLED = True
PAIR_TRADE_TIME_TOLERANCE_SEC = 60
PAIR_TRADE_OVERLAP_THRESHOLD = 0.5
PAIR_OVERLAP_MAX_LINES = 120

# ---------------------------------------------------------------------------


def _parse_day(s: str) -> date:
    return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()


def _iter_days_inclusive(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _load_pkls_concat(data_dir: str, start_d: date, end_d: date) -> Tuple[pd.DataFrame, List[str], List[str]]:
    dfs: List[pd.DataFrame] = []
    loaded: List[str] = []
    missing: List[str] = []
    for d in _iter_days_inclusive(start_d, end_d):
        stem = d.strftime("%Y-%m-%d")
        fp = os.path.join(data_dir, stem + ".pkl")
        if not os.path.isfile(fp):
            missing.append(stem)
            continue
        dfs.append(pd.read_pickle(fp))
        loaded.append(stem)
    if not dfs:
        return pd.DataFrame(), loaded, missing
    return pd.concat(dfs, ignore_index=True), loaded, missing


def _filter_trades_by_time_naive_bjt(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    d = df.copy()
    ts = pd.to_datetime(d["ts_text"], errors="coerce")
    t0 = pd.Timestamp(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0))
    t1 = pd.Timestamp(datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59))
    m = ts.notna() & (ts >= t0) & (ts <= t1)
    return d.loc[m].copy()


def _build_header(start_d: date, end_d: date, loaded: List[str], missing: List[str], n_users: int) -> List[str]:
    total_days = len(_iter_days_inclusive(start_d, end_d))
    lines = [
        "查询语句: 指定用户组 {}~{} 简化交易报告（共 {} 个 user_id）".format(
            start_d.strftime("%Y-%m-%d"),
            end_d.strftime("%Y-%m-%d"),
            n_users,
        ),
        "窗口天数: {}".format(total_days),
        "数据覆盖: {}/{}".format(len(loaded), total_days),
    ]
    if missing:
        lines.append("缺失日期: {}".format(", ".join(missing)))
    else:
        lines.append("缺失日期: 无")
    lines.append("")
    return lines


def _ordered_user_ids_by_profit(df: pd.DataFrame, want: Sequence[str]) -> List[str]:
    """有成交的按总盈利降序；配置里无成交的 user_id 排在末尾。"""
    want_list = [str(x).strip() for x in want if str(x).strip()]
    if not want_list:
        return []
    want_set = set(want_list)
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    d = d[d["user_id"].astype(str).isin(want_set)]
    ranked: List[str] = []
    if d.shape[0] > 0:
        gp = d.groupby("user_id", as_index=False)["profit_loss"].sum()
        gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True])
        ranked = [str(x) for x in gp["user_id"].tolist()]
    seen = set(ranked)
    for uid in want_list:
        if uid not in seen:
            ranked.append(uid)
    return ranked


def build_user_stats_lines(df: pd.DataFrame, user_ids: Sequence[str]) -> List[str]:
    """与 ``run_profit_topn`` 相同的单行摘要，但对象是指定 user_id 列表。"""
    lines = ["查询结果: 指定用户组交易统计（按总盈利降序）"]
    ranked = _ordered_user_ids_by_profit(df, user_ids)
    if not ranked:
        lines.append("无结果")
        return lines

    want_set: Set[str] = {str(x).strip() for x in user_ids if str(x).strip()}
    d = df.copy()
    d = d[d["user_id"].astype(str).isin(want_set)]

    any_stats = False
    for uid in ranked:
        sub = d[d["user_id"].astype(str) == str(uid)].copy()
        if sub.shape[0] == 0:
            lines.append("id:{} 无成交".format(uid))
            continue
        s_df = _pkl_legs_to_calc_trade_stats_df(sub, uid)
        if s_df.shape[0] == 0:
            lines.append("id:{} 无数据".format(uid))
            continue
        try:
            res = calc_trade_stats(s_df, userid=str(uid))
            lines.append(_format_profit_top_user_line_mongdb_style(uid, res))
            any_stats = True
        except Exception as e:
            lines.append("id:{} 统计失败: {}".format(uid, e))

    if not any_stats:
        lines.append("（区间内指定用户均无有效成交腿）")
    return lines


def build_pair_overlap_section(df: pd.DataFrame, user_ids: Sequence[str]) -> List[str]:
    uids = [str(x).strip() for x in user_ids if str(x).strip()]
    if len(uids) < 2:
        return []
    lines = [
        "",
        "--- 指定用户组两两交易重合（同 daily_profit_top10：同合约+方向一致+时间≤{}s，双向均≥ {:.0%}）---".format(
            int(PAIR_TRADE_TIME_TOLERANCE_SEC),
            float(PAIR_TRADE_OVERLAP_THRESHOLD),
        ),
    ]
    ol = dptr.build_top_user_pair_overlap_lines(
        df,
        uids,
        threshold=float(PAIR_TRADE_OVERLAP_THRESHOLD),
        tol_sec=int(PAIR_TRADE_TIME_TOLERANCE_SEC),
        max_lines=int(PAIR_OVERLAP_MAX_LINES),
    )
    if ol:
        lines.extend(ol)
    else:
        lines.append("无满足阈值的用户对/交易对")
    return lines


def build_full_report(
    df: pd.DataFrame,
    user_ids: Sequence[str],
    start_d: date,
    end_d: date,
    loaded: List[str],
    missing: List[str],
) -> str:
    want = [str(x).strip() for x in user_ids if str(x).strip()]
    lines: List[str] = []
    lines.extend(_build_header(start_d, end_d, loaded, missing, len(want)))
    lines.extend(build_user_stats_lines(df, want))
    if PAIR_OVERLAP_ENABLED and len(want) >= 2:
        lines.extend(build_pair_overlap_section(df, want))
    return "\n".join(lines) + "\n"


def main() -> int:
    start_d = _parse_day(START_DATE)
    end_d = _parse_day(END_DATE)
    if start_d > end_d:
        print("START_DATE 不能晚于 END_DATE", file=sys.stderr)
        return 2

    want: Set[str] = {str(x).strip() for x in USER_IDS if str(x).strip()}
    if not want:
        print("USER_IDS 为空", file=sys.stderr)
        return 2

    if not os.path.isdir(PKL_DATA_DIR):
        print("PKL_DATA_DIR 不存在: {}".format(PKL_DATA_DIR), file=sys.stderr)
        return 1

    raw, loaded, missing = _load_pkls_concat(PKL_DATA_DIR, start_d, end_d)
    if raw.shape[0] == 0:
        print("未读到任何 PKL 行。已尝试日期: {}，缺失: {}".format(loaded, missing), file=sys.stderr)
        return 1

    df = ensure_user_id(raw)
    df = _filter_trades_by_time_naive_bjt(df, start_d, end_d)
    if ONLY_U_PERP_USDT:
        df = dptr.filter_u_perp_symbols(df)
    df = df[df["user_id"].astype(str).isin(want)].copy()

    report = build_full_report(df, sorted(want, key=lambda x: (len(x), x)), start_d, end_d, loaded, missing)

    sub_dir = "{}_{}".format(start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))
    out_dir = os.path.join(OUTPUT_REPORT_BASE_DIR, sub_dir)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "report.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)

    print("PKL 目录: {}".format(os.path.abspath(PKL_DATA_DIR)))
    print("载入 PKL 天数: {}/{} 缺失: {}".format(len(loaded), len(_iter_days_inclusive(start_d, end_d)), len(missing)))
    print("用户数量: {}".format(len(want)))
    print("成交腿数（过滤后）: {}".format(df.shape[0]))
    print("报告已写入: {}".format(os.path.abspath(out_path)))
    if PRINT_STDOUT:
        print("---")
        print(report, end="" if report.endswith("\n") else "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
