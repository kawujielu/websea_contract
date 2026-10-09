# -*- coding: utf-8 -*-
"""
基于与 pkl_target_users_trade_stats.py 相同的 mongo_daily_deals 日切 PKL 数据口径：
- 按自然日（ts_text 解析出的日期）、交易对汇总全体目标用户的 profit_loss；
- 输出每日「全用户合计盈亏最高的 3 个交易对」与「合计盈亏最低的 3 个交易对」及金额；
- 全量数据按交易对汇总盈亏（目标用户合计），输出排名前 10 与后 10 的交易对及金额；
- 结果写入 .xlsx（需 openpyxl）。

Python 3.9.19 兼容。

用法:
  python pkl_daily_symbol_pnl_top_xlsx.py
  python pkl_daily_symbol_pnl_top_xlsx.py --data-dir D:\\path\\to\\pkl
  python pkl_daily_symbol_pnl_top_xlsx.py -o 自定义路径.xlsx
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

import pandas as pd

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from pkl_target_users_trade_stats import (  # noqa: E402
    DEFAULT_DATA_DIR,
    TARGET_USER_IDS,
    ensure_user_id,
    filter_target_users_raw,
    list_pkl_paths,
    load_concat_pkls,
)
from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="日切 PKL：每日全用户汇总前三盈亏交易对 + 全量交易对 Top10/Bottom10，导出 xlsx"
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录（YYYY-MM-DD.pkl）",
    )
    p.add_argument(
        "--output",
        "-o",
        type=str,
        default=os.path.join(os.getcwd(), "symbol_pnl_rank.xlsx"),
        help="输出 .xlsx 路径，默认当前目录 symbol_pnl_rank.xlsx",
    )
    args, _ = p.parse_known_args()
    return args


def _prepare_excel_path(path: str) -> str:
    path = os.path.abspath(path.strip())
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.splitext(path)[1].lower() not in (".xlsx", ".xlsm"):
        path = path + ".xlsx"
    try:
        import openpyxl  # noqa: F401
    except ImportError as e:
        raise ImportError("导出 .xlsx 需要 openpyxl：pip install openpyxl") from e
    return path


def load_target_user_legs(data_dir: str) -> Tuple[pd.DataFrame, List[str]]:
    notes: List[str] = []
    notes.append("PKL 目录: {}".format(os.path.abspath(data_dir)))
    paths = list_pkl_paths(data_dir)
    notes.append("载入文件数: {}".format(len(paths)))
    if not paths:
        notes.append("警告: 未找到任何 YYYY-MM-DD.pkl")
        return pd.DataFrame(), notes

    raw = load_concat_pkls(paths)
    notes.append("原始行数: {}".format(raw.shape[0]))
    excluded = get_excluded_user_ids_sync()
    effective_uids = tuple(str(u) for u in TARGET_USER_IDS if str(u) not in excluded)
    if len(effective_uids) != len(TARGET_USER_IDS):
        notes.append(
            "剔除模拟金/做市后目标用户数: {} -> {}".format(
                len(TARGET_USER_IDS), len(effective_uids)
            )
        )
    if not effective_uids:
        notes.append("目标用户全部命中模拟金/做市剔除名单，无可分析用户")
        return pd.DataFrame(), notes

    raw = filter_target_users_raw(raw, effective_uids)
    notes.append("目标用户相关行数: {}".format(raw.shape[0]))
    if raw.shape[0] == 0:
        return pd.DataFrame(), notes

    legs = ensure_user_id(raw)
    uid_set = set(effective_uids)
    legs = legs[legs["user_id"].astype(str).isin(uid_set)].copy()
    notes.append("目标用户腿行数: {}".format(legs.shape[0]))

    legs["profit_loss"] = pd.to_numeric(legs["profit_loss"], errors="coerce").fillna(
        0.0
    )
    ts = pd.to_datetime(legs["ts_text"], errors="coerce")
    legs = legs.loc[ts.notna()].copy()
    legs["统计日期"] = ts.dt.date
    return legs, notes


def build_daily_market_top3_wide(daily_by_symbol: pd.DataFrame) -> pd.DataFrame:
    """
    daily_by_symbol 列: 统计日期, symbol, profit_loss（已按日、交易对汇总全体用户）
    返回宽表：每个统计日期一行，盈/亏各 3 档交易对及当日全用户合计金额。
    """
    if daily_by_symbol.shape[0] == 0:
        return pd.DataFrame()

    win_cols: Dict[str, List] = {}
    lose_cols: Dict[str, List] = {}
    for i in range(1, 4):
        win_cols["盈利第{}_交易对".format(i)] = []
        win_cols["盈利第{}_全用户合计盈亏".format(i)] = []
        lose_cols["亏损第{}_交易对".format(i)] = []
        lose_cols["亏损第{}_全用户合计盈亏".format(i)] = []

    dates: List = []
    for d, sub in daily_by_symbol.groupby("统计日期", sort=True):
        dates.append(d)
        sub = sub.sort_values("profit_loss", ascending=False, kind="mergesort")
        top3 = sub.head(3)
        bot3 = sub.tail(3).sort_values("profit_loss", ascending=True, kind="mergesort")
        for i in range(3):
            if i < len(top3):
                win_cols["盈利第{}_交易对".format(i + 1)].append(
                    str(top3.iloc[i]["symbol"])
                )
                win_cols["盈利第{}_全用户合计盈亏".format(i + 1)].append(
                    float(top3.iloc[i]["profit_loss"])
                )
            else:
                win_cols["盈利第{}_交易对".format(i + 1)].append("")
                win_cols["盈利第{}_全用户合计盈亏".format(i + 1)].append(None)
            if i < len(bot3):
                lose_cols["亏损第{}_交易对".format(i + 1)].append(
                    str(bot3.iloc[i]["symbol"])
                )
                lose_cols["亏损第{}_全用户合计盈亏".format(i + 1)].append(
                    float(bot3.iloc[i]["profit_loss"])
                )
            else:
                lose_cols["亏损第{}_交易对".format(i + 1)].append("")
                lose_cols["亏损第{}_全用户合计盈亏".format(i + 1)].append(None)

    out = pd.DataFrame({"统计日期": dates})
    for k, v in win_cols.items():
        out[k] = v
    for k, v in lose_cols.items():
        out[k] = v
    return out


def symbol_totals_top_bottom(legs: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if legs.shape[0] == 0:
        return pd.DataFrame(), pd.DataFrame()
    g = legs.groupby("symbol", sort=False)["profit_loss"].sum()
    g = g.sort_values(ascending=False)
    top10 = g.head(10).reset_index()
    top10.columns = ["交易对", "全量盈亏合计"]
    top10.insert(0, "排名", range(1, len(top10) + 1))

    bot10 = g.tail(10).sort_values(ascending=True).reset_index()
    bot10.columns = ["交易对", "全量盈亏合计"]
    bot10.insert(0, "排名（从亏到盈）", range(1, len(bot10) + 1))
    return top10, bot10


def main() -> int:
    args = parse_args()
    legs, notes = load_target_user_legs(args.data_dir)
    out_path = _prepare_excel_path(args.output)

    daily_by_symbol = pd.DataFrame()
    if legs.shape[0] > 0:
        daily_by_symbol = (
            legs.groupby(["统计日期", "symbol"], sort=True)["profit_loss"]
            .sum()
            .reset_index()
        )

    daily_wide = build_daily_market_top3_wide(daily_by_symbol)
    top10_sym, bot10_sym = symbol_totals_top_bottom(legs)

    notes_df = pd.DataFrame(
        {"说明行号": range(1, len(notes) + 1), "内容": notes}
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        daily_wide.to_excel(writer, sheet_name="每日全用户前三交易对", index=False)
        top10_sym.to_excel(writer, sheet_name="交易对全量盈利Top10", index=False)
        bot10_sym.to_excel(writer, sheet_name="交易对全量亏损Bottom10", index=False)
        notes_df.to_excel(writer, sheet_name="运行说明", index=False)

    sys.stderr.write("已写入: {}\n".format(out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
