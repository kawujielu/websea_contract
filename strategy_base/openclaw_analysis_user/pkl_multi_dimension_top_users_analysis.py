# -*- coding: utf-8 -*-
"""
从日切 PKL 按时间范围汇总用户，在三个维度各取 TopN：
  - 总收益（与 calc_trade_stats「总盈亏」一致）
  - 成交笔数（按订单算）
  - 总交易轮次（开平仓算一次）

对入选用户输出 calc_trade_stats 全量指标（与 mongdb_order_stats empty 字典键一致）。

用法:
  python pkl_multi_dimension_top_users_analysis.py --start 2026-05-01 --end 2026-05-07 --print-stdout
  python pkl_multi_dimension_top_users_analysis.py --query-text "查询近7天总收益交易笔数开平仓轮次各前10用户详细分析" --print-stdout
  python pkl_multi_dimension_top_users_analysis.py --start 2026-05-01 --end 2026-05-07 --no-ai --print-stdout  # 跳过 Gemini
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import get_excluded_user_ids_sync
from mongdb_order_stats import calc_trade_stats
from pkl_user_query_analyzer import (
    DEFAULT_DATA_DIR,
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
    expected_dates,
    load_window_df,
    parse_window_days,
)

try:
    from AI_model.gemini_client import DEFAULT_GEMINI_MODEL, generate_content, get_api_key
except ImportError:
    DEFAULT_GEMINI_MODEL = "models/gemini-2.5-flash"

    def get_api_key() -> str:  # type: ignore
        return (os.environ.get("GOOGLE_API_KEY") or "AIzaSyBvJJUIqoQ93CZCOUSwMRcOLBt6FmWkWF8").strip()

    def generate_content(prompt: str, model_name: Optional[str] = None) -> str:  # type: ignore
        try:
            import google.generativeai as genai
        except ImportError as e:
            raise RuntimeError(
                "未安装 google-generativeai，请执行: pip install google-generativeai"
            ) from e
        api_key = get_api_key()
        if not api_key:
            raise RuntimeError("无可用 GOOGLE_API_KEY")
        genai.configure(api_key=api_key)
        model_id = (model_name or os.environ.get("GEMINI_MODEL") or DEFAULT_GEMINI_MODEL).strip()
        model = genai.GenerativeModel(model_id)
        response = model.generate_content(prompt)
        text_out = getattr(response, "text", None)
        if text_out:
            return str(text_out).strip()
        return str(response).strip()

BJT = ZoneInfo("Asia/Shanghai")
MAX_TOP = 50

GEMINI_PREAMBLE = (
    "你是一名量化交易分析员。以下是一批用户的成交统计信息，请基于事实做客观分析"
    "用户的交易风格、策略类型、风险等信息，不要编造数据中不存在的字段或内容。"
)

# calc_trade_stats empty 字典键顺序（用于详细输出）
DETAIL_STAT_KEYS: Tuple[str, ...] = (
    "交易开始时间",
    "交易结束时间",
    "成交笔数(按订单算)",
    "交易性质",
    "开仓次数(按订单算)",
    "平仓次数(按订单算)",
    "总交易轮次(开平算一次)",
    "是否重点标签套利用户",
    "taker金额",
    "taker金额占比",
    "maker金额",
    "maker金额占比",
    "手续费总和",
    "总盈亏",
    "刨除手续费总盈亏",
    "总手续费",
    "手续费占比",
    "分币对盈亏",
    "最大盈利",
    "最大亏损",
    "总成交金额",
    "盈利/成交额",
    "胜率",
    "盈亏比",
    "期望收益",
    "夏普比率",
    "平均杠杆",
    "最大持仓时长",
    "最短持仓时长",
    "平均持仓时长",
    "中位数持仓时长",
    "众数持仓时长(聚合到min)",
    "持仓时间小于5min的交易轮次占比",
    "最大连续盈利",
    "最大连续亏损",
    "日均交易次数",
    "周均交易次数",
    "月均交易次数",
    "收益率年化标准差",
    "平均仓位规模",
    "平均交易间隔",
    "统计指标",
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PKL 时间范围内三维度 TopN 用户 + calc_trade_stats 详细分析"
    )
    p.add_argument("--data-dir", type=str, default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR))
    p.add_argument("--start", type=str, default="", help="起始自然日 YYYY-MM-DD（含）")
    p.add_argument("--end", type=str, default="", help="结束自然日 YYYY-MM-DD（含）")
    p.add_argument(
        "--query-text",
        type=str,
        default="",
        help="自然语言时间范围（近N天/周/月 或 YYYY-MM-DD到YYYY-MM-DD），与 --start/--end 二选一",
    )
    p.add_argument("--top", type=int, default=10, help="各维度 Top N，默认 10")
    p.add_argument("--print-stdout", action="store_true")
    p.add_argument("--no-ai", action="store_true", help="不调用 Gemini，仅输出统计报告")
    p.add_argument(
        "--gemini-model",
        type=str,
        default=os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL),
        help="Gemini 模型 ID，默认 models/gemini-2.5-flash",
    )
    args, _ = p.parse_known_args()
    return args


def _parse_day(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def parse_top_n_from_query(query: str, default_n: int = 10) -> int:
    q = (query or "").replace(" ", "")
    m = re.search(r"前(\d+)(?:名|个|位)?", q)
    if m:
        return max(1, min(MAX_TOP, int(m.group(1))))
    return default_n


def resolve_date_range(
    start_s: str,
    end_s: str,
    query_text: str,
) -> Tuple[date, date, str]:
    """返回 (start_d, end_d, 描述标签)。"""
    start_s = (start_s or "").strip()
    end_s = (end_s or "").strip()
    if start_s and end_s:
        start_d = _parse_day(start_s)
        end_d = _parse_day(end_s)
        if start_d > end_d:
            raise ValueError("--start 不能晚于 --end")
        return start_d, end_d, "{} ~ {}".format(start_d.isoformat(), end_d.isoformat())

    q = (query_text or "").strip()
    if not q:
        raise ValueError("请提供 --start/--end 或 --query-text 时间范围")

    q_ns = q.replace(" ", "")
    m = re.search(r"(\d{4}-\d{2}-\d{2})\s*到\s*(\d{4}-\d{2}-\d{2})", q)
    if m:
        start_d = _parse_day(m.group(1))
        end_d = _parse_day(m.group(2))
        if start_d > end_d:
            raise ValueError("起始日期不能晚于结束日期")
        return start_d, end_d, "{} ~ {}".format(start_d.isoformat(), end_d.isoformat())

    window_days = parse_window_days(q)
    days = expected_dates(window_days)
    if not days:
        raise ValueError("无法解析时间窗口")
    return days[0], days[-1], "近{}天(至昨日)".format(window_days)


def dates_inclusive(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def filter_df_by_date_range(df: pd.DataFrame, start_d: date, end_d: date) -> pd.DataFrame:
    if df.shape[0] == 0:
        return df
    d = df.copy()
    ts = pd.to_datetime(d["ts_text"], errors="coerce")
    lo = pd.Timestamp(datetime(start_d.year, start_d.month, start_d.day, 0, 0, 0))
    hi = pd.Timestamp(datetime(end_d.year, end_d.month, end_d.day, 23, 59, 59))
    return d[(ts >= lo) & (ts <= hi)].copy()


def _format_trade_nature(tn) -> str:
    if isinstance(tn, dict):
        return "自主{} 跟单{} 其他{}".format(
            tn.get("自主交易", 0),
            tn.get("跟单交易", 0),
            tn.get("其他", 0),
        )
    return str(tn)


def _format_stat_value(key: str, val) -> str:
    if key == "交易性质":
        return _format_trade_nature(val)
    if key == "分币对盈亏":
        s = str(val or "").strip()
        return s if s else "(无)"
    if key == "统计指标":
        s = str(val or "").strip()
        return s if s else "(无)"
    if val is None:
        return ""
    return str(val)


def format_detail_block(uid: str, rank_tags: List[str], stats: Dict) -> List[str]:
    lines = [
        "=" * 60,
        "user_id={}  入选: {}".format(uid, "、".join(rank_tags) if rank_tags else "-"),
    ]
    for k in DETAIL_STAT_KEYS:
        lines.append("{}: {}".format(k, _format_stat_value(k, stats.get(k))))
    return lines


def compute_user_summaries(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    """对每个 user_id 跑 calc_trade_stats，得到三维度排序用字段；并缓存完整 stats。"""
    rows = []
    stats_cache: Dict[str, Dict] = {}
    for uid, grp in df.groupby("user_id", sort=False):
        su = str(uid)
        sub = grp.copy()
        s_df = _pkl_legs_to_calc_trade_stats_df(sub, su)
        if s_df.shape[0] == 0:
            continue
        try:
            st = calc_trade_stats(s_df, userid=su)
            stats_cache[su] = st
        except Exception as e:
            rows.append(
                {
                    "user_id": su,
                    "总盈亏": 0.0,
                    "成交笔数(按订单算)": 0,
                    "总交易轮次(开平算一次)": 0,
                    "_error": str(e),
                }
            )
            continue
        rows.append(
            {
                "user_id": su,
                "总盈亏": float(st.get("总盈亏") or 0.0),
                "成交笔数(按订单算)": int(st.get("成交笔数(按订单算)") or 0),
                "总交易轮次(开平算一次)": int(st.get("总交易轮次(开平算一次)") or 0),
                "_error": "",
            }
        )
    if not rows:
        return pd.DataFrame(), stats_cache
    return pd.DataFrame(rows), stats_cache


def pick_top_ids(
    summary: pd.DataFrame,
    col: str,
    top_n: int,
) -> List[str]:
    if summary.shape[0] == 0:
        return []
    s = summary.sort_values([col, "user_id"], ascending=[False, True]).head(top_n)
    return [str(x) for x in s["user_id"].tolist()]


def format_rank_section(title: str, summary: pd.DataFrame, col: str, top_n: int) -> List[str]:
    lines = [title]
    if summary.shape[0] == 0:
        lines.append("(无数据)")
        return lines
    s = summary.sort_values([col, "user_id"], ascending=[False, True]).head(top_n)
    for i, (_, r) in enumerate(s.iterrows(), start=1):
        err = str(r.get("_error") or "").strip()
        extra = " 统计异常:{}".format(err) if err else ""
        lines.append(
            "{:02d}. user_id={} {}={}{}".format(
                i,
                r["user_id"],
                col,
                r[col],
                extra,
            )
        )
    return lines


def analyze(
    data_dir: str,
    start_d: date,
    end_d: date,
    window_label: str,
    top_n: int,
) -> str:
    top_n = max(1, min(int(top_n), MAX_TOP))
    day_list = dates_inclusive(start_d, end_d)
    df_raw, exists, miss = load_window_df(data_dir, day_list)
    lines: List[str] = [
        "PKL 多维度 Top{} 用户详细分析".format(top_n),
        "时间范围: {}（北京时间自然日）".format(window_label),
        "数据覆盖: {}/{} 日".format(len(exists), len(day_list)),
    ]
    if miss:
        lines.append("缺失日期: {}".format(", ".join(miss)))
    else:
        lines.append("缺失日期: 无")
    lines.append("")

    if df_raw.shape[0] == 0:
        lines.append("查询结果: 窗口内无可用 PKL 数据")
        return "\n".join(lines)

    df = ensure_user_id(df_raw)
    df = filter_df_by_date_range(df, start_d, end_d)
    excluded = get_excluded_user_ids_sync()
    if excluded:
        df = df[~df["user_id"].astype(str).isin(excluded)].copy()

    if df.shape[0] == 0:
        lines.append("查询结果: 过滤做市/模拟金后无成交记录")
        return "\n".join(lines)

    summary, stats_cache = compute_user_summaries(df)
    if summary.shape[0] == 0:
        lines.append("查询结果: 无有效用户统计")
        return "\n".join(lines)

    col_profit = "总盈亏"
    col_trades = "成交笔数(按订单算)"
    col_rounds = "总交易轮次(开平算一次)"

    top_profit = pick_top_ids(summary, col_profit, top_n)
    top_trades = pick_top_ids(summary, col_trades, top_n)
    top_rounds = pick_top_ids(summary, col_rounds, top_n)

    lines.extend(format_rank_section("【总收益 Top{}】".format(top_n), summary, col_profit, top_n))
    lines.append("")
    lines.extend(format_rank_section("【成交笔数 Top{}】".format(top_n), summary, col_trades, top_n))
    lines.append("")
    lines.extend(format_rank_section("【开平仓轮次 Top{}】".format(top_n), summary, col_rounds, top_n))
    lines.append("")
    lines.append("【详细分析】（calc_trade_stats 全量指标）")
    lines.append("")

    detail_uids: Set[str] = set(top_profit) | set(top_trades) | set(top_rounds)
    uid_to_tags: Dict[str, List[str]] = {u: [] for u in detail_uids}

    def _tag(uid: str, label: str) -> None:
        if uid not in uid_to_tags:
            uid_to_tags[uid] = []
        if label not in uid_to_tags[uid]:
            uid_to_tags[uid].append(label)

    for u in top_profit:
        _tag(u, "总收益Top{}".format(top_n))
    for u in top_trades:
        _tag(u, "成交笔数Top{}".format(top_n))
    for u in top_rounds:
        _tag(u, "开平仓轮次Top{}".format(top_n))

    for uid in sorted(detail_uids, key=lambda x: (x not in top_profit, x)):
        su = str(uid)
        stats = stats_cache.get(su)
        if stats is None:
            sub = df[df["user_id"].astype(str) == su].copy()
            s_df = _pkl_legs_to_calc_trade_stats_df(sub, su)
            if s_df.shape[0] == 0:
                lines.append("user_id={} 无成交腿数据".format(uid))
                lines.append("")
                continue
            try:
                stats = calc_trade_stats(s_df, userid=su)
            except Exception as e:
                lines.append("user_id={} 详细统计失败: {}".format(uid, e))
                lines.append("")
                continue
        lines.extend(format_detail_block(su, uid_to_tags.get(su, []), stats))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_gemini_prompt(report_text: str, window_label: str, top_n: int) -> str:
    """将统计报告拼成传给 Gemini 的 prompt（含分析员前提）。"""
    return "\n".join(
        [
            GEMINI_PREAMBLE,
            "",
            "--- 元数据 ---",
            "时间范围: {}（北京时间自然日）".format(window_label),
            "各维度 Top: {}".format(top_n),
            "",
            "--- 三榜排名与用户 calc_trade_stats 详细指标 ---",
            report_text.strip(),
            "",
            "--- 请输出 ---",
            "1) 对用户群体整体的交易特征归纳（频率、持仓、盈亏结构、手续费等）；",
            "2) 按 user_id 分别简要点评交易风格/策略倾向/主要风险（仅依据上文数据）；",
            "3) 若多用户入选不同榜单，说明其差异；结论须可追溯到上文指标，勿臆测。",
        ]
    )


def run_gemini_analysis(
    report_text: str,
    window_label: str,
    top_n: int,
    model_name: str,
) -> Tuple[str, str]:
    """
    调用 Gemini。返回 (ai_body, status_line)。
    ai_body 为空表示未调用或失败（status 含原因）。
    """
    if not report_text.strip():
        return "", "[INFO] 报告为空，跳过 Gemini。"
    api_key = get_api_key()
    if not api_key:
        return "", "[INFO] 无可用 GOOGLE_API_KEY，跳过 Gemini。"
    prompt = build_gemini_prompt(report_text, window_label, top_n)
    try:
        ai_text = generate_content(prompt, model_name=model_name)
        return ai_text, "[INFO] Gemini 分析完成（prompt 长度 {} 字符）。".format(len(prompt))
    except Exception as e:
        return "", "[ERROR] Gemini 调用失败: {}".format(e)


def append_gemini_to_report(
    report_text: str,
    window_label: str,
    top_n: int,
    model_name: str,
    no_ai: bool,
) -> str:
    if no_ai:
        return report_text
    ai_body, status = run_gemini_analysis(report_text, window_label, top_n, model_name)
    parts = [report_text.rstrip(), "", "=" * 60, status, ""]
    if ai_body.strip():
        parts.extend(["--- Gemini AI 分析 ---", "", ai_body.strip(), ""])
    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    args = parse_args()
    top_n = int(args.top)
    if args.query_text.strip():
        top_n = parse_top_n_from_query(args.query_text, top_n)
    try:
        start_d, end_d, label = resolve_date_range(args.start, args.end, args.query_text)
    except ValueError as e:
        sys.stderr.write("{}\n".format(e))
        return 2
    report = analyze(args.data_dir, start_d, end_d, label, top_n)
    text = append_gemini_to_report(
        report,
        label,
        top_n,
        str(args.gemini_model or DEFAULT_GEMINI_MODEL).strip(),
        bool(args.no_ai),
    )
    if args.print_stdout:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
