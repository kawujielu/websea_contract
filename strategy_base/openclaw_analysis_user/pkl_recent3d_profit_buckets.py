# -*- coding: utf-8 -*-
"""
最近 3 个已结束自然日（北京时间）日切 PKL 扫描：
- 组 A：三日各自 calc_trade_stats「总盈亏」均 > 0；三日「总交易轮次(开平算一次)」之和 > 5；
        三日「总盈亏」之和满足 100 < sum < 1000。
- 组 B：三日「总盈亏」之和 > 1000（不要求每日盈利）。

口径与 pkl_target_users_trade_stats + mongdb_order_stats.calc_trade_stats 一致。
Python 3.9+。

示例:
  export TG_BOT_TOKEN="你的bot_token"
  python pkl_recent3d_profit_buckets.py --data-dir ./data/mongo_daily_deals_pkl
  python pkl_recent3d_profit_buckets.py --end-date 2026-05-04 --csv out.csv --omit-ids

Telegram: 默认将同控制台一致的报告发到群 chat_id=1843312449；需 TG_BOT_TOKEN 或 UPM_TG_BOT_TOKEN。
加 --no-telegram 可仅打印不推送。

每组用户在「近3日合并窗口」下输出一行交易分析（与 pkl_user_query_analyzer 盈利榜格式一致）：
总盈利、开平仓轮次、胜率、盈亏比、持仓时间中位数。使用 --omit-ids 时不输出 id 列表与上述逐行分析。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import warnings
from datetime import date, datetime, timedelta
from typing import Dict, List, Set, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402
from mongdb_order_stats import calc_trade_stats  # noqa: E402
from pkl_user_query_analyzer import (  # noqa: E402
    _format_profit_top_user_line_mongdb_style,
    _pkl_legs_to_calc_trade_stats_df,
)
from pkl_target_users_trade_stats import (  # noqa: E402
    ensure_user_id,
    pkl_legs_to_calc_frame,
)

warnings.filterwarnings(
    "ignore",
    message="overflow encountered",
    category=RuntimeWarning,
)

BJT = ZoneInfo("Asia/Shanghai")
DEFAULT_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

# 结果推送 Telegram 群（与 daily_profit_top10_kline_report 等脚本同群）
TG_CHAT_ID = 1843312449
TG_CHUNK_MAX = 3800

KEY_PNL = "总盈亏"
KEY_ROUNDS = "总交易轮次(开平算一次)"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="近 3 日日切 PKL：连盈+轮次+盈亏区间用户 与 三日总盈利>1000U 用户"
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录（YYYY-MM-DD.pkl）",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="窗口末日 YYYY-MM-DD，默认北京时间「昨天」；窗口为该日及往前共 3 个自然日",
    )
    p.add_argument(
        "--omit-ids",
        action="store_true",
        help="仅输出人数；不打印 user_id 列表及逐用户交易分析行",
    )
    p.add_argument(
        "--csv",
        type=str,
        default="",
        help="可选：写入 CSV（列 user_id, bucket）",
    )
    p.add_argument(
        "--include-excluded",
        action="store_true",
        help="不过滤做市/模拟金等排除名单（默认与既有 PKL 脚本一致会剔除）",
    )
    p.add_argument(
        "--no-telegram",
        action="store_true",
        help="不发送到 Telegram（默认发送；需配置环境变量 TG_BOT_TOKEN 或 UPM_TG_BOT_TOKEN）",
    )
    args, _ = p.parse_known_args()
    return args


def parse_day(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def window_three_days(end_d: date) -> Tuple[date, date, date]:
    return (end_d - timedelta(days=2), end_d - timedelta(days=1), end_d)


def pkl_path(data_dir: str, d: date) -> str:
    return os.path.join(data_dir, "{}.pkl".format(d.strftime("%Y-%m-%d")))


def load_three_days(data_dir: str, days: Tuple[date, date, date]) -> pd.DataFrame:
    missing: List[str] = []
    dfs: List[pd.DataFrame] = []
    for d in days:
        fp = pkl_path(data_dir, d)
        if not os.path.isfile(fp):
            missing.append(d.strftime("%Y-%m-%d"))
            continue
        dfs.append(pd.read_pickle(fp))
    if missing:
        raise FileNotFoundError(
            "以下日期 PKL 缺失（需连续 3 日文件齐全）: {}".format(", ".join(missing))
        )
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _one_user_day_stats(sub: pd.DataFrame, uid: str) -> Tuple[float, int]:
    if sub is None or sub.shape[0] == 0:
        return 0.0, 0
    cdf = pkl_legs_to_calc_frame(sub)
    if cdf.shape[0] == 0:
        return 0.0, 0
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        stats = calc_trade_stats(cdf, userid=str(uid))
    pnl = float(stats.get(KEY_PNL) or 0.0)
    r = stats.get(KEY_ROUNDS)
    rounds = int(r) if r is not None else 0
    return pnl, rounds


def compute_daily_grid(
    df: pd.DataFrame, days: Tuple[date, date, date]
) -> Dict[str, Dict[date, Tuple[float, int]]]:
    """user_id -> day -> (pnl, rounds)"""
    if df.shape[0] == 0:
        return {}
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["ts_text"], errors="coerce").dt.date
    out: Dict[str, Dict[date, Tuple[float, int]]] = {}
    for uid, g in d.groupby("user_id", sort=False):
        su = str(uid)
        inner: Dict[date, Tuple[float, int]] = {}
        for day in days:
            sub = g[g["trade_date"] == day]
            inner[day] = _one_user_day_stats(sub, su)
        out[su] = inner
    return out


def classify(
    grid: Dict[str, Dict[date, Tuple[float, int]]], days: Tuple[date, date, date]
) -> Tuple[List[str], List[str]]:
    bucket_a: List[str] = []
    bucket_b: List[str] = []
    for uid, day_map in grid.items():
        pnls = [day_map[d][0] for d in days]
        rounds = [day_map[d][1] for d in days]
        sum_pnl = sum(pnls)
        sum_rounds = sum(rounds)
        if all(p > 0.0 for p in pnls) and sum_rounds > 5 and (sum_pnl > 100.0) and (sum_pnl < 1000.0):
            bucket_a.append(uid)
        if sum_pnl > 1000.0:
            bucket_b.append(uid)
    def _uid_sort_key(u: str) -> Tuple:
        if u.isdigit():
            return (0, int(u))
        return (1, u)

    bucket_a.sort(key=lambda x: _uid_sort_key(x))
    bucket_b.sort(key=lambda x: _uid_sort_key(x))
    return bucket_a, bucket_b


def _telegram_token() -> str:
    return (os.environ.get("TG_BOT_TOKEN") or os.environ.get("UPM_TG_BOT_TOKEN") or "").strip()


def _chunk_text_for_telegram(text: str, max_len: int) -> List[str]:
    lines = text.split("\n")
    chunks: List[str] = []
    buf: List[str] = []
    size = 0
    for line in lines:
        add = len(line) + (1 if buf else 0)
        if buf and size + add > max_len:
            chunks.append("\n".join(buf))
            buf = [line]
            size = len(line)
        else:
            if buf:
                size += 1
            buf.append(line)
            size += len(line)
    if buf:
        chunks.append("\n".join(buf))
    return chunks


def send_telegram_report(text: str, chat_id: int) -> None:
    token = _telegram_token()
    if not token:
        raise RuntimeError("未设置 TG_BOT_TOKEN 或 UPM_TG_BOT_TOKEN，无法发送 Telegram")
    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    for part in _chunk_text_for_telegram(text, TG_CHUNK_MAX):
        body = urllib.parse.urlencode(
            {
                "chat_id": str(chat_id),
                "text": part,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            raise RuntimeError(
                "Telegram HTTP {}: {}".format(e.code, err_body or str(e))
            ) from e
        data = json.loads(raw) if raw else {}
        if not data.get("ok"):
            raise RuntimeError("Telegram API 返回非 ok: {}".format(raw[:500]))


def _lines_trade_analysis_window(
    legs: pd.DataFrame, uids: List[str], window_label: str
) -> List[str]:
    """
    与 pkl_user_query_analyzer.run_profit_topn 一致：窗口内该用户全部腿合并后 calc_trade_stats，
    输出总盈利、开平仓轮次、胜率、盈亏比、持仓时间中位数（单行 mongdb 风格）。
    """
    out: List[str] = []
    if not uids:
        return out
    out.append("交易分析（{}，口径同 pkl_user_query_analyzer 盈利榜）:".format(window_label))
    for uid in uids:
        su = str(uid)
        sub = legs[legs["user_id"].astype(str) == su].copy()
        s_df = _pkl_legs_to_calc_trade_stats_df(sub, su)
        if s_df.shape[0] == 0:
            out.append("id:{} 无数据".format(su))
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                res = calc_trade_stats(s_df, userid=su)
            out.append(_format_profit_top_user_line_mongdb_style(su, res))
        except Exception as e:
            out.append("id:{} 统计失败: {}".format(su, e))
    return out


def format_report_text(
    legs: pd.DataFrame,
    data_dir: str,
    day_str: str,
    bucket_a: List[str],
    bucket_b: List[str],
    omit_ids: bool,
) -> str:
    lines: List[str] = [
        "近3日 PKL 用户分桶",
        "PKL 目录: {}".format(data_dir),
        "窗口(北京时间自然日): {}".format(day_str),
        "",
        "【组 A】三日各自总盈亏>0，开平仓轮次(三日和)>5，100 < 三日总盈亏和 < 1000",
        "人数: {}".format(len(bucket_a)),
    ]
    if not omit_ids:
        lines.append(
            "user_id: {}".format(",".join(bucket_a)) if bucket_a else "user_id: (无)"
        )
        lines.extend(_lines_trade_analysis_window(legs, bucket_a, "组 A 用户近3日合并"))
    lines.extend(
        [
            "",
            "【组 B】三日总盈亏和 > 1000 U",
            "人数: {}".format(len(bucket_b)),
        ]
    )
    if not omit_ids:
        lines.append(
            "user_id: {}".format(",".join(bucket_b)) if bucket_b else "user_id: (无)"
        )
        lines.extend(_lines_trade_analysis_window(legs, bucket_b, "组 B 用户近3日合并"))
    return "\n".join(lines)


def write_csv(path: str, bucket_a: List[str], bucket_b: List[str]) -> None:
    rows: List[Tuple[str, str]] = []
    for u in bucket_a:
        rows.append((u, "streak_100_1000"))
    for u in bucket_b:
        rows.append((u, "total_gt_1000"))
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    pd.DataFrame(rows, columns=["user_id", "bucket"]).to_csv(
        path, index=False, encoding="utf-8-sig"
    )


def main() -> int:
    args = parse_args()
    data_dir = os.path.abspath(args.data_dir)
    if args.end_date.strip():
        end_d = parse_day(args.end_date)
    else:
        end_d = datetime.now(BJT).date() - timedelta(days=1)
    days = window_three_days(end_d)

    try:
        raw = load_three_days(data_dir, days)
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 2

    if raw.shape[0] == 0:
        print("窗口内 PKL 合并后无行", file=sys.stderr)
        return 1

    legs = ensure_user_id(raw)
    excluded: Set[str] = set()
    if not args.include_excluded:
        excluded = get_excluded_user_ids_sync()
        if excluded:
            legs = legs[~legs["user_id"].astype(str).isin(excluded)].copy()

    grid = compute_daily_grid(legs, days)
    bucket_a, bucket_b = classify(grid, days)

    day_str = ", ".join(d.strftime("%Y-%m-%d") for d in days)
    report = format_report_text(legs, data_dir, day_str, bucket_a, bucket_b, args.omit_ids)
    print(report)

    if not args.no_telegram:
        try:
            send_telegram_report(report, TG_CHAT_ID)
            print("", flush=True)
            print("已发送至 Telegram chat_id={}".format(TG_CHAT_ID), flush=True)
        except Exception as e:
            print("Telegram 发送失败: {}".format(e), file=sys.stderr, flush=True)
            return 3

    csv_p = (args.csv or "").strip()
    if csv_p:
        write_csv(csv_p, bucket_a, bucket_b)
        print("")
        print("已写入 CSV: {}".format(os.path.abspath(csv_p)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
