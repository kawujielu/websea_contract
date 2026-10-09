# -*- coding: utf-8 -*-
"""
近 3 个已结束自然日（北京时间，窗口末日默认「昨天」）日切 PKL：
找出窗口内总盈利（profit_loss 合计）最高的 1 名用户（排除名单逻辑与 daily_profit_top10_kline_report 一致），
打印该用户 mongdb_order_stats.calc_trade_stats 全量指标 + 窗口内全部逐笔成交（原始 PKL 行），
并将上述内容拼成 deal_data（与 gemini_test.py 中传入 generate_content 的字符串角色一致），
若已安装 google-generativeai 且设置 GOOGLE_API_KEY，则调用 Gemini 做简要分析。

不修改 gemini_test.py。Python 3.9+。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Dict, List, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from exclude_user_ids import clear_excluded_user_ids_cache, get_excluded_user_ids_async
from mongdb_order_stats import calc_trade_stats
from pkl_user_query_analyzer import (
    DEFAULT_DATA_DIR,
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
    load_day_df,
)

try:
    import google.generativeai as genai
except ImportError:
    genai = None

BJT = ZoneInfo("Asia/Shanghai")
SPAN_DAYS = 3
DEFAULT_GEMINI_MODEL = "models/gemini-2.0-flash"

# 运行 ``python top1_user_3d_profit_full_report_and_ai.py`` 结束后与 gemini_test.py 中同名变量语义一致
deal_data: str = ""


def pick_stat_day(stat_day_str: str) -> date:
    if stat_day_str.strip():
        return datetime.strptime(stat_day_str.strip(), "%Y-%m-%d").date()
    return datetime.now(BJT).date() - timedelta(days=1)


def load_span_days_df(data_dir: str, end_day: date, span_days: int) -> pd.DataFrame:
    span_days = max(1, int(span_days))
    dfs: List[pd.DataFrame] = []
    for i in range(span_days):
        d = end_day - timedelta(days=span_days - 1 - i)
        try:
            dfs.append(load_day_df(data_dir, d))
        except FileNotFoundError:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


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


def _trades_detail_block(user_df: pd.DataFrame) -> Tuple[str, str]:
    """返回 (控制台打印用多行文本, 写入 deal_data 的 CSV 文本)。"""
    if user_df is None or user_df.shape[0] == 0:
        return "(无成交行)", ""
    d = user_df.copy()
    if "ts_text" in d.columns:
        d = d.sort_values("ts_text", kind="mergesort").reset_index(drop=True)
    buf = StringIO()
    d.to_csv(buf, index=False)
    csv_text = buf.getvalue()
    # 控制台：除极大宽表外直接打印 DataFrame；列过多时仍 to_csv 更稳
    try:
        with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
            table_text = d.to_string(index=False)
    except Exception:
        table_text = csv_text
    return table_text, csv_text


def build_deal_data(
    end_day: date,
    span_days: int,
    exists_days: List[str],
    miss_days: List[str],
    winner_uid: str,
    window_total_profit: float,
    stats_text: str,
    trades_csv: str,
) -> str:
    header = "\n".join(
        [
            "你是一名量化交易分析助手。以下是一名用户在最近若干自然日内的成交与统计信息，请基于事实做客观分析（风险、风格、频率等），不要编造数据中不存在的字段。",
            "",
            "--- 元数据 ---",
            # "窗口末日(北京时间): {}".format(end_day.strftime("%Y-%m-%d")),
            "数据日期: {}".format(", ".join(exists_days) if exists_days else "无"),
            "窗口天数: {}".format(span_days),
            # "缺失日期: {}".format(", ".join(miss_days) if miss_days else "无"),
            "user_id: {}".format(winner_uid),
            "窗口内 profit_loss 合计: {:.6f}".format(float(window_total_profit)),
            "",
            "--- 交易统计指标 (calc_trade_stats 全量) ---",
            stats_text,
            "",
            "--- 逐笔成交明细 (CSV) ---",
            trades_csv if trades_csv.strip() else "(无)",
        ]
    )
    return header


async def _maybe_refresh_sim_users(force: bool) -> None:
    if not force:
        return
    clear_excluded_user_ids_cache()
    os.environ["UPM_FETCH_SIM_USERS"] = "1"
    try:
        from ToolBoxNew import ToolBox

        tb = ToolBox()
        await tb.get_sim_user()
    except Exception as e:
        print("[WARN] get_sim_user 预拉取失败（后续 exclude 仍会重试）: {}".format(e))


def _expected_day_strings(end_day: date, span_days: int) -> List[str]:
    span_days = max(1, int(span_days))
    out: List[str] = []
    for i in range(span_days):
        d = end_day - timedelta(days=span_days - 1 - i)
        out.append(d.strftime("%Y-%m-%d"))
    return out


def main() -> str:
    ap = argparse.ArgumentParser(description="近3日盈利最高用户：全量统计+逐笔明细+可选 Gemini")
    ap.add_argument("--data-dir", type=str, default=os.environ.get("UPM_PKL_DEALS_DIR", "").strip() or DEFAULT_DATA_DIR)
    ap.add_argument("--stat-day", type=str, default="", help="窗口末日 YYYY-MM-DD，默认北京时间昨天")
    ap.add_argument("--span-days", type=int, default=SPAN_DAYS, help="自然日窗口长度，默认 3")
    ap.add_argument("--no-ai", action="store_true", help="不调用 Gemini，仅打印并组装 deal_data")
    ap.add_argument("--gemini-model", type=str, default=os.environ.get("GEMINI_MODEL", DEFAULT_GEMINI_MODEL))
    ap.add_argument(
        "--force-refresh-sim-users",
        action="store_true",
        help="与 daily_profit_top10_kline_report 一致：清空 exclude 缓存并预拉模拟金",
    )
    args = ap.parse_args()

    stat_day = pick_stat_day(args.stat_day)
    data_dir = args.data_dir
    span_days = max(1, int(args.span_days))
    day_keys = _expected_day_strings(stat_day, span_days)

    asyncio.run(_maybe_refresh_sim_users(bool(args.force_refresh_sim_users)))

    span_df_all = load_span_days_df(data_dir, stat_day, span_days)
    exists_days: List[str] = []
    miss_days: List[str] = []
    for ds in day_keys:
        fp = os.path.join(data_dir, "{}.pkl".format(ds))
        if os.path.isfile(fp):
            exists_days.append(ds)
        else:
            miss_days.append(ds)

    if span_df_all.shape[0] == 0:
        print("[ERROR] 窗口内无可用 PKL 数据。目录={} 缺失={}".format(data_dir, ",".join(miss_days) or "全部"))
        out = build_deal_data(stat_day, span_days, exists_days, miss_days, "", 0.0, "(无数据)", "")
        print("\n--- deal_data ---\n{}".format(out))
        return out

    span_df_all = ensure_user_id(span_df_all)
    excluded_ids = asyncio.run(get_excluded_user_ids_async())
    if excluded_ids:
        span_df_all = span_df_all[~span_df_all["user_id"].astype(str).isin(excluded_ids)].copy()

    d_rank = span_df_all.copy()
    d_rank["profit_loss"] = pd.to_numeric(d_rank["profit_loss"], errors="coerce").fillna(0.0)
    gp = d_rank.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True]).head(1)
    if gp.shape[0] == 0:
        print("[ERROR] 聚合后无用户（可能全部为排除账号或 profit_loss 无有效数据）")
        out = build_deal_data(stat_day, span_days, exists_days, miss_days, "", 0.0, "(无用户)", "")
        print("\n--- deal_data ---\n{}".format(out))
        return out

    winner_uid = str(gp.iloc[0]["user_id"])
    window_total_profit = float(gp.iloc[0]["profit_loss"])
    user_raw = span_df_all[span_df_all["user_id"].astype(str) == winner_uid].copy()

    stats_df = _pkl_legs_to_calc_trade_stats_df(user_raw, winner_uid)
    if stats_df.shape[0] == 0:
        stats_text = "(无有效腿，无法计算统计)"
    else:
        try:
            stats_res = calc_trade_stats(stats_df, userid=str(winner_uid))
        except Exception as e:
            stats_text = "统计失败: {}".format(e)
        else:
            stats_text = _format_stats_dict(stats_res)

    table_text, trades_csv = _trades_detail_block(user_raw)

    print("=" * 60)
    print("近{}日盈利最高用户 (窗口末日 {} 北京时间)".format(span_days, stat_day.strftime("%Y-%m-%d")))
    print("user_id={}  window_total_profit_loss_sum={:.6f}".format(winner_uid, window_total_profit))
    print("PKL: 有 {}/{} 天  缺失: {}".format(len(exists_days), span_days, ", ".join(miss_days) if miss_days else "无"))
    print("=" * 60)
    print("\n--- 交易统计指标 (全量) ---\n{}".format(stats_text))
    print("\n--- 逐笔成交明细 (共 {} 行) ---\n{}".format(user_raw.shape[0], table_text))

    out = build_deal_data(
        stat_day,
        span_days,
        exists_days,
        miss_days,
        winner_uid,
        window_total_profit,
        stats_text,
        trades_csv,
    )

    print("\n" + "=" * 60)
    print("deal_data 已组装（长度 {} 字符，与 gemini_test.py 中传入模型的字符串同用途）".format(len(out)))
    print("=" * 60)

    if args.no_ai:
        print("[INFO] 已指定 --no-ai，跳过 Gemini。")
        return out

    api_key = (os.environ.get("GOOGLE_API_KEY") or "AIzaSyBvJJUIqoQ93CZCOUSwMRcOLBt6FmWkWF8").strip()
    if not api_key:
        print("[INFO] 未设置环境变量 GOOGLE_API_KEY，跳过 AI 调用。可将 deal_data 复制到 gemini_test.py 使用。")
        return out
    if genai is None:
        print("[WARN] 未安装 google-generativeai，跳过 AI。请: pip install google-generativeai")
        return out

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(args.gemini_model)
    try:
        response = model.generate_content(out)
        text_out = getattr(response, "text", None) or str(response)
        print("\n--- Gemini 分析 ---\n{}".format(text_out))
    except Exception as e:
        print("[ERROR] Gemini 调用失败: {}".format(e))
    return out


if __name__ == "__main__":
    deal_data = main()
    # print(deal_data)

