# -*- coding: utf-8 -*-
"""
将指定用户在一段时间内的成交（来自日切 PKL）与 1min K 线叠加，输出为本地 HTML。

绘图与落盘方式与 daily_profit_top10_kline_report.render_user_symbol_html /
fetch_1m_klines 一致（直接复用该模块实现）。

使用方式：仅修改下方「配置区」变量，然后在脚本所在目录执行：

  python pkl_user_ids_trades_kline_to_html.py

无需命令行参数。依赖：pandas、requests、plotly（与报表脚本相同）。Python 3.9+。
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

# 复用报表中的 K 线拉取与 HTML 写出（含 plotly 蜡烛图 + 开平标记）
import daily_profit_top10_kline_report as dptr  # noqa: E402

from pkl_user_query_analyzer import ensure_user_id  # noqa: E402

# ---------------------------------------------------------------------------
# 配置区：请按需修改后保存，再运行本脚本（不要改变量名，便于维护）
# ---------------------------------------------------------------------------

# 用户 ID 列表（字符串）
USER_IDS: Sequence[str] = (
    ['1504167','1500098','1495449','1499345','1499438','1506247'])

# 统计区间（北京时间自然日，含首尾）
START_DATE = "2026-09-10"  # YYYY-MM-DD
END_DATE = "2026-09-23"  # YYYY-MM-DD

# 日切 PKL 目录（文件名形如 YYYY-MM-DD.pkl）
PKL_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

# HTML 输出根目录（其下会再建子目录：{START_DATE}_{END_DATE}/）
OUTPUT_HTML_BASE_DIR = os.path.join(_DIR, "reports", "user_trades_kline_html")

# 仅生成 U 本位永续（与报表一致）；若改为 False 则不过滤 symbol
ONLY_U_PERP_USDT = True

# 若某 (用户, 合约) 在区间内无 K 线数据，是否跳过写文件（True=跳过）
SKIP_WHEN_NO_KLINE = True

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


def _window_ts_bjt(start_d: date, end_d: date) -> Tuple[int, int]:
    bjt = dptr.BJT
    t0 = datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0, tzinfo=bjt)
    t1 = datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59, tzinfo=bjt)
    return int(t0.timestamp()), int(t1.timestamp())


def _filter_trades_by_time_naive_bjt(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    """按 ts_text 落在 [start_d 00:00, end_d 23:59:59] 的墙钟时间过滤（与 PKL 常见 naive 时间一致）。"""
    if df.shape[0] == 0:
        return df
    d = df.copy()
    ts = pd.to_datetime(d["ts_text"], errors="coerce")
    t0 = pd.Timestamp(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0))
    t1 = pd.Timestamp(datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59))
    m = ts.notna() & (ts >= t0) & (ts <= t1)
    return d.loc[m].copy()


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
    if df.shape[0] == 0:
        print("区间内、指定用户、过滤后无成交腿。", file=sys.stderr)
        return 1

    start_ts, end_ts = _window_ts_bjt(start_d, end_d)
    sub_dir = "{}_{}".format(start_d.strftime("%Y-%m-%d"), end_d.strftime("%Y-%m-%d"))
    out_dir = os.path.join(OUTPUT_HTML_BASE_DIR, sub_dir)
    os.makedirs(out_dir, exist_ok=True)

    print("PKL 目录: {}".format(os.path.abspath(PKL_DATA_DIR)))
    print("载入 PKL 天数: {}/{} 缺失: {}".format(len(loaded), len(_iter_days_inclusive(start_d, end_d)), len(missing)))
    if missing:
        print("缺失日期样例: {}".format(", ".join(missing[:8]) + (" ..." if len(missing) > 8 else "")))
    print("用户: {}".format(", ".join(sorted(want))))
    print("HTML 输出: {}".format(os.path.abspath(out_dir)))
    print("---")

    made = 0
    for uid in sorted(want, key=lambda x: (len(x), x)):
        udf = df[df["user_id"].astype(str) == str(uid)].copy()
        if udf.shape[0] == 0:
            print("[SKIP] uid={} 无成交".format(uid))
            continue
        symbols = sorted(udf["symbol"].astype(str).unique().tolist())
        for sym in symbols:
            us = udf[udf["symbol"].astype(str) == sym].copy()
            try:
                kline_df = dptr.fetch_1m_klines(sym, start_ts, end_ts)
                if kline_df.shape[0] == 0:
                    msg = "[WARN] uid={} symbol={} 无 K 线数据".format(uid, sym)
                    if SKIP_WHEN_NO_KLINE:
                        print(msg + " -> 跳过")
                        continue
                    print(msg + " -> 仍尝试写 HTML（可能无 K 线）")
                fn = "{}+{}.html".format(uid, dptr.safe_symbol_for_filename(sym))
                fp = os.path.join(out_dir, fn)
                title = "user={} symbol={} {}~{} 1minK线+开平信号".format(
                    uid,
                    sym,
                    start_d.strftime("%Y-%m-%d"),
                    end_d.strftime("%Y-%m-%d"),
                )
                dptr.render_user_symbol_html(uid, sym, kline_df, us, fp, title_text=title)
                made += 1
                print("[OK] {}".format(fp))
            except Exception as e:
                print("[ERROR] uid={} symbol={} err={}".format(uid, sym, e), file=sys.stderr)

    print("---\n完成，共生成 {} 个 HTML。".format(made))
    return 0 if made else 1


if __name__ == "__main__":
    raise SystemExit(main())
