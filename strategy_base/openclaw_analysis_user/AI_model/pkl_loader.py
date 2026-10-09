# -*- coding: utf-8 -*-
"""从日切 PKL 加载成交并组装 Gemini 数据块。"""
from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

_CODE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)

from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402
from mongdb_order_stats import calc_trade_stats  # noqa: E402
from pkl_user_query_analyzer import (  # noqa: E402
    _format_profit_top_user_line_mongdb_style,
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
)

DEFAULT_DATA_DIR = os.environ.get(
    "UPM_PKL_DEALS_DIR",
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl",
).strip()

MAX_TRADE_ROWS_PER_USER = int(os.environ.get("AI_PROFILE_MAX_TRADE_ROWS", "8000"))


def _iter_days_inclusive(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def list_available_pkl_days(data_dir: str) -> List[date]:
    """扫描目录内 YYYY-MM-DD.pkl，升序返回。"""
    if not os.path.isdir(data_dir):
        return []
    pat = re.compile(r"^(\d{4}-\d{2}-\d{2})\.pkl$")
    days: List[date] = []
    for name in os.listdir(data_dir):
        m = pat.match(name)
        if not m:
            continue
        try:
            days.append(datetime.strptime(m.group(1), "%Y-%m-%d").date())
        except ValueError:
            continue
    days.sort()
    return days


def load_window_df(
    data_dir: str,
    start_d: date,
    end_d: date,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    dfs: List[pd.DataFrame] = []
    exists: List[str] = []
    miss: List[str] = []
    for d in _iter_days_inclusive(start_d, end_d):
        stem = d.strftime("%Y-%m-%d")
        fp = os.path.join(data_dir, stem + ".pkl")
        if os.path.isfile(fp):
            exists.append(stem)
            dfs.append(pd.read_pickle(fp))
        else:
            miss.append(stem)
    if not dfs:
        return pd.DataFrame(), exists, miss
    return pd.concat(dfs, ignore_index=True), exists, miss


def load_all_available_df(data_dir: str) -> Tuple[pd.DataFrame, List[str], List[str]]:
    days = list_available_pkl_days(data_dir)
    if not days:
        return pd.DataFrame(), [], []
    return load_window_df(data_dir, days[0], days[-1])


def _apply_excluded(df: pd.DataFrame) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    excluded = get_excluded_user_ids_sync()
    if not excluded:
        return df
    return df[~df["user_id"].astype(str).isin(excluded)].copy()


def _format_stats_dict(res: Dict) -> str:
    lines = []
    order = list(calc_trade_stats(pd.DataFrame()).keys())
    seen = set()
    for k in order:
        if k in res:
            lines.append("{}: {}".format(k, res[k]))
            seen.add(k)
    for k in sorted(k for k in res if k not in seen):
        lines.append("{}: {}".format(k, res[k]))
    return "\n".join(lines)


def _df_to_csv_text(df: pd.DataFrame) -> str:
    if df is None or df.shape[0] == 0:
        return ""
    d = df.copy()
    if "ts_text" in d.columns:
        d = d.sort_values("ts_text", kind="mergesort").reset_index(drop=True)
    buf = StringIO()
    d.to_csv(buf, index=False)
    return buf.getvalue()


def _calc_stats_for_user(sub: pd.DataFrame, uid: str) -> str:
    s_df = _pkl_legs_to_calc_trade_stats_df(sub, uid)
    if s_df.shape[0] == 0:
        return "(无有效腿，无法计算统计)"
    try:
        res = calc_trade_stats(s_df, userid=str(uid))
        return _format_stats_dict(res)
    except Exception as e:
        return "统计失败: {}".format(e)


def _build_meta_lines(
    original_query: str,
    user_ids: Sequence[str],
    start_d: Optional[date],
    end_d: Optional[date],
    exists: List[str],
    miss: List[str],
    mode_note: str,
) -> List[str]:
    lines = [
        "--- 元数据 ---",
        "原始提问: {}".format(original_query.strip()),
        "数据模式: {}".format(mode_note),
    ]
    if user_ids:
        lines.append("user_ids: {}".format(", ".join(user_ids)))
    else:
        lines.append("user_ids: (时间范围内全部用户，仅汇总指标)")
    if start_d and end_d:
        lines.append("日期区间: {} 到 {}".format(start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d")))
    lines.append("数据覆盖: {} 天".format(len(exists)))
    if exists:
        lines.append("已有日期: {}".format(", ".join(exists[:20]) + (" ..." if len(exists) > 20 else "")))
    if miss:
        lines.append("缺失日期: {}".format(", ".join(miss[:20]) + (" ..." if len(miss) > 20 else "")))
    lines.append("")
    return lines


def build_user_detail_section(
    df: pd.DataFrame,
    uid: str,
    include_trades: bool = True,
) -> str:
    sub = df[df["user_id"].astype(str) == str(uid)].copy()
    lines = [
        "===== user_id: {} =====".format(uid),
        "成交腿行数: {}".format(sub.shape[0]),
        "",
        "--- calc_trade_stats 全量 ---",
        _calc_stats_for_user(sub, uid),
    ]
    if include_trades:
        truncated = False
        trade_df = sub
        if sub.shape[0] > MAX_TRADE_ROWS_PER_USER:
            if "ts_text" in sub.columns:
                trade_df = sub.sort_values("ts_text", kind="mergesort").tail(MAX_TRADE_ROWS_PER_USER)
            else:
                trade_df = sub.tail(MAX_TRADE_ROWS_PER_USER)
            truncated = True
        lines.extend(["", "--- 逐笔成交 (CSV) ---"])
        if truncated:
            lines.append(
                "(注: 原始 {} 行，已截断为最近 {} 行)".format(sub.shape[0], MAX_TRADE_ROWS_PER_USER)
            )
        lines.append(_df_to_csv_text(trade_df) or "(无)")
    lines.append("")
    return "\n".join(lines)


def build_all_users_stats_section(df: pd.DataFrame) -> str:
    """时间范围内全部用户：仅 calc_trade_stats 汇总。"""
    if df.shape[0] == 0:
        return "(无成交数据)"
    d = df.copy()
    d["profit_loss"] = pd.to_numeric(d["profit_loss"], errors="coerce").fillna(0.0)
    gp = d.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True])
    lines = ["--- 全部用户汇总（按总盈利降序，仅统计指标）---", "用户数: {}".format(gp.shape[0]), ""]
    for _, r in gp.iterrows():
        uid = str(r["user_id"])
        sub = d[d["user_id"].astype(str) == uid].copy()
        s_df = _pkl_legs_to_calc_trade_stats_df(sub, uid)
        if s_df.shape[0] == 0:
            lines.append("id:{} 无数据".format(uid))
            continue
        try:
            res = calc_trade_stats(s_df, userid=uid)
            lines.append(_format_profit_top_user_line_mongdb_style(uid, res))
        except Exception as e:
            lines.append("id:{} 统计失败: {}".format(e))
    return "\n".join(lines)


def load_and_build_data_block(
    data_dir: str,
    original_query: str,
    user_ids: Sequence[str],
    date_range: Optional[Tuple[date, date]],
    raw_mode: bool,
) -> str:
    """
    按路由规则加载 PKL 并组装文本块。
    date_range=None 且 user_ids 非空 → 目录内全部 pkl。
    date_range 有值、user_ids 空 → 时间范围内全部用户 stats only。
    """
    if date_range is not None:
        start_d, end_d = date_range
        df_raw, exists, miss = load_window_df(data_dir, start_d, end_d)
        mode_note = "指定日期区间"
    elif user_ids:
        df_raw, exists, miss = load_all_available_df(data_dir)
        start_d = datetime.strptime(exists[0], "%Y-%m-%d").date() if exists else None
        end_d = datetime.strptime(exists[-1], "%Y-%m-%d").date() if exists else None
        mode_note = "指定用户，目录内全部可用 PKL 日文件"
    else:
        return "(无数据：未指定用户且未解析到时间)"

    if df_raw.shape[0] == 0:
        meta = _build_meta_lines(original_query, user_ids, start_d, end_d, exists, miss, mode_note)
        meta.append("查询结果: 窗口内无可用 PKL 数据")
        return "\n".join(meta)

    df = _apply_excluded(ensure_user_id(df_raw))

    if user_ids:
        want = {str(u) for u in user_ids}
        df = df[df["user_id"].astype(str).isin(want)].copy()

    meta = _build_meta_lines(original_query, user_ids, start_d, end_d, exists, miss, mode_note)
    body_parts: List[str] = []

    if not user_ids:
        body_parts.append(build_all_users_stats_section(df))
    else:
        for uid in user_ids:
            body_parts.append(
                build_user_detail_section(
                    df,
                    uid,
                    include_trades=True,
                )
            )

    if raw_mode:
        trade_parts = []
        for uid in user_ids:
            sub = df[df["user_id"].astype(str) == str(uid)].copy()
            if sub.shape[0] == 0:
                trade_parts.append("# user_id={} (无成交)".format(uid))
                continue
            truncated = False
            trade_df = sub
            if sub.shape[0] > MAX_TRADE_ROWS_PER_USER:
                if "ts_text" in sub.columns:
                    trade_df = sub.sort_values("ts_text", kind="mergesort").tail(MAX_TRADE_ROWS_PER_USER)
                else:
                    trade_df = sub.tail(MAX_TRADE_ROWS_PER_USER)
                truncated = True
            header = "# user_id={}".format(uid)
            if truncated:
                header += " (截断为最近{}行)".format(MAX_TRADE_ROWS_PER_USER)
            trade_parts.append(header)
            trade_parts.append(_df_to_csv_text(trade_df) or "(无)")
        return "\n".join(trade_parts)

    return "\n".join(meta + body_parts)
