from __future__ import annotations

import asyncio
import os
import re
import zipfile
from datetime import date, datetime, timedelta
from typing import Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ToolBoxNew import ToolBox
from exclude_user_ids import clear_excluded_user_ids_cache, get_excluded_user_ids_async
from pkl_user_group_overlap_by_ids import clusters_from_edges, pairwise_global_edges
from pkl_user_query_analyzer import (
    DEFAULT_DATA_DIR,
    ensure_user_id,
    load_day_df,
    run_profit_topn,
)


# ==============================
# 调试参数（先写死，方便调试）
# ==============================
BJT = ZoneInfo("Asia/Shanghai")
STAT_DAY = ""  # 为空默认昨天，格式 YYYY-MM-DD
TOP_N = 30
# 默认本地 data 目录；mongo_daily_deals_dump 子进程会设置 UPM_PKL_DEALS_DIR 指向 dump 输出目录
DATA_DIR = DEFAULT_DATA_DIR
TG_CHAT_ID = "-5223829567"
DRY_RUN = False
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"

# 每次执行本脚本：清空 exclude 缓存、强制拉模拟金，并先调用 ToolBoxNew.get_sim_user
FORCE_REFRESH_SIM_USERS_ON_RUN = True

# HTML 输出目录（会自动拼接 YYYY-MM-DD 子目录）
HTML_BASE_DIR = os.path.join(os.path.dirname(__file__), "reports", "top10_user_symbol_kline_html")

# 永续U本位 1min K线接口（公共接口，无需鉴权）
KLINE_BASE_URL = "https://coqv.websea.work"
KLINE_API = "/qapi-v1/market/kline"
KLINE_PERIOD = "1min"
REQUEST_TIMEOUT_SEC = 15
TG_DOCUMENT_TIMEOUT_SEC = 120
# 单次拉取窗口（秒）：按整天拆分多次请求，规避单次返回条数上限
KLINE_CHUNK_SECONDS = 60 * 60

# TopN 两两分析：与 pkl_user_group_overlap_by_ids 一致（全体 U 本位腿、不区分合约）
PAIR_TRADE_TIME_TOLERANCE_SEC = 60
PAIR_TRADE_OVERLAP_THRESHOLD = 0.5  # 双向重合度均须 ≥ 50% 才连边并归组


def pick_stat_day() -> date:
    if STAT_DAY:
        return datetime.strptime(STAT_DAY, "%Y-%m-%d").date()
    return datetime.now(BJT).date() - timedelta(days=1)


def day_start_end_ts(d: date) -> (int, int):
    day_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=BJT)
    day_end = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=BJT)
    return int(day_start.timestamp()), int(day_end.timestamp())


def day_window_ts(end_day: date, span_days: int) -> (int, int):
    span_days = max(1, int(span_days))
    start_day = end_day - timedelta(days=span_days - 1)
    day_start = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT)
    day_end = datetime(end_day.year, end_day.month, end_day.day, 23, 59, 59, tzinfo=BJT)
    return int(day_start.timestamp()), int(day_end.timestamp())


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


def safe_symbol_for_filename(symbol: str) -> str:
    return re.sub(r"[^A-Za-z0-9._+-]", "_", str(symbol).strip())


def filter_u_perp_symbols(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["symbol"] = d["symbol"].astype(str)
    # 当前业务按 U 本位永续处理：symbol 需以 -USDT 结尾
    return d[d["symbol"].str.endswith("-USDT", na=False)].copy()


def _cluster_min_overlap(
    members: List[str],
    edges: List[tuple],
) -> float | None:
    """组内达阈值边的最低 min(双向重合度)。"""
    member_set = set(members)
    mins: List[float] = []
    for mmin, _ra, _rb, u1, u2 in edges:
        if u1 in member_set and u2 in member_set:
            mins.append(float(mmin))
    return min(mins) if mins else None


def build_top_user_global_overlap_lines(
    day_trades: pd.DataFrame,
    top_user_ids: List[str],
    threshold: float = PAIR_TRADE_OVERLAP_THRESHOLD,
    tol_sec: int = PAIR_TRADE_TIME_TOLERANCE_SEC,
) -> List[str]:
    """
    与 pkl_user_group_overlap_by_ids --show-clusters 一致：
    全体 U 本位腿、不区分合约；双向均≥ threshold 的用户对传递合并为组；
    每组输出 user_id 列表，末尾附组内最低重合度（必 > threshold×100%）。
    """
    top_user_ids = [str(x) for x in top_user_ids]
    if len(top_user_ids) < 2:
        return []

    edges = pairwise_global_edges(day_trades, top_user_ids, threshold=threshold, tol_sec=tol_sec)
    clusters = clusters_from_edges(top_user_ids, edges)
    if not clusters:
        return [
            "无相似组：任意两用户均未同时达到双向重合度均 ≥ {:.0f}%（时间容差 {} 秒）。".format(
                float(threshold) * 100.0,
                int(tol_sec),
            )
        ]

    lines: List[str] = ["相似用户组（双向重合度均 ≥ {:.0f}%，传递合并）：".format(float(threshold) * 100.0)]
    for g in clusters:
        g_min = _cluster_min_overlap(g, edges)
        inner = ", ".join(g)
        if g_min is not None:
            lines.append("【{}】 组内最低重合度: {:.2f}%".format(inner, g_min))
        else:
            lines.append("【{}】".format(inner))
    return lines


def map_signal_color(side_text: str) -> str:
    if side_text in ("开多", "平空"):
        return "red"
    return "green"


def map_signal_text(side_text: str) -> str:
    return "开" if side_text in ("开多", "开空") else "平"


def build_signal_text(side_text: str, close_pnl_sum: float) -> str:
    if side_text in ("平多", "平空"):
        return "平{:+.2f}".format(float(close_pnl_sum or 0.0))
    return "开"


def _parse_kline_payload_to_df(payload: Dict) -> pd.DataFrame:
    if int(payload.get("errno", -1)) != 0:
        raise RuntimeError("kline接口返回错误: {}".format(payload))
    result = payload.get("result") or {}
    rows = result.get("data") or []
    if not rows:
        return pd.DataFrame(columns=["dt", "open", "high", "low", "close", "volume"])
    out = pd.DataFrame(rows)
    out["open"] = pd.to_numeric(out.get("open"), errors="coerce")
    out["high"] = pd.to_numeric(out.get("high"), errors="coerce")
    out["low"] = pd.to_numeric(out.get("low"), errors="coerce")
    out["close"] = pd.to_numeric(out.get("close"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("vol"), errors="coerce").fillna(0.0)
    if "id" in out.columns:
        out["dt"] = pd.to_datetime(out["id"], unit="s", utc=True).dt.tz_convert(BJT)
    elif "ts" in out.columns:
        out["dt"] = pd.to_datetime(out["ts"], unit="s", utc=True).dt.tz_convert(BJT)
    else:
        raise ValueError("kline数据缺少时间字段: {}".format(list(out.columns)))
    return out[["dt", "open", "high", "low", "close", "volume"]].dropna().sort_values("dt")


def fetch_1m_klines(symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    url = "{}{}".format(KLINE_BASE_URL.rstrip("/"), KLINE_API)
    all_parts: List[pd.DataFrame] = []
    cur = int(start_ts)
    end_ts = int(end_ts)
    while cur <= end_ts:
        seg_end = min(end_ts, cur + KLINE_CHUNK_SECONDS - 1)
        params = {
            "symbol": symbol,
            "period": KLINE_PERIOD,
            "start": cur,
            "end": seg_end,
        }
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
        r.raise_for_status()
        part = _parse_kline_payload_to_df(r.json())
        if part.shape[0] > 0:
            all_parts.append(part)
        cur = seg_end + 1
    if not all_parts:
        return pd.DataFrame(columns=["dt", "open", "high", "low", "close", "volume"])
    out = pd.concat(all_parts, ignore_index=True)
    out = out.drop_duplicates(subset=["dt"], keep="last").sort_values("dt").reset_index(drop=True)
    return out


def render_user_symbol_html(
    uid: str,
    symbol: str,
    kline_df: pd.DataFrame,
    user_symbol_deals: pd.DataFrame,
    html_path: str,
    title_text: str = "",
):
    import plotly.graph_objects as go

    kline_df = kline_df.copy()
    kline_df["ma20"] = kline_df["close"].rolling(window=20, min_periods=1).mean()
    kline_df["ma60"] = kline_df["close"].rolling(window=60, min_periods=1).mean()
    kline_df["ma120"] = kline_df["close"].rolling(window=120, min_periods=1).mean()

    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=kline_df["dt"],
            open=kline_df["open"],
            high=kline_df["high"],
            low=kline_df["low"],
            close=kline_df["close"],
            name="1mK线",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=kline_df["dt"],
            y=kline_df["ma20"],
            mode="lines",
            name="MA20",
            line={"width": 1.2, "color": "#1f77b4"},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=kline_df["dt"],
            y=kline_df["ma60"],
            mode="lines",
            name="MA60",
            line={"width": 1.2, "color": "#ff7f0e"},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=kline_df["dt"],
            y=kline_df["ma120"],
            mode="lines",
            name="MA120",
            line={"width": 1.2, "color": "#9467bd"},
        )
    )

    marks = user_symbol_deals.copy()
    marks["dt"] = pd.to_datetime(marks["ts_text"]).dt.tz_localize(BJT, nonexistent="shift_forward", ambiguous="infer")
    marks["price"] = pd.to_numeric(marks["price"], errors="coerce")
    marks["profit_loss"] = pd.to_numeric(marks["profit_loss"], errors="coerce").fillna(0.0)
    marks = marks.dropna(subset=["dt", "price"])
    marks["minute_bucket"] = marks["dt"].dt.floor("min")

    close_pnl = (
        marks[marks["buy_sell"].astype(str).isin(["平多", "平空"])]
        .groupby(["minute_bucket", "buy_sell"], as_index=False)["profit_loss"]
        .sum()
        .rename(columns={"profit_loss": "close_pnl_sum"})
    )
    # 同一分钟内同方向信号仅展示一次（不区分价格和数量）
    marks = marks.drop_duplicates(subset=["minute_bucket", "buy_sell"], keep="first")
    marks = marks.merge(close_pnl, on=["minute_bucket", "buy_sell"], how="left")
    marks["close_pnl_sum"] = pd.to_numeric(marks.get("close_pnl_sum"), errors="coerce").fillna(0.0)
    if marks.shape[0] > 0:
        marks["dot_color"] = marks["buy_sell"].astype(str).map(map_signal_color)
        # 仅非零盈亏的平仓：圆点红绿与开平侧向色对调（亏红盈绿）
        _close_nz = marks["buy_sell"].astype(str).isin(["平多", "平空"]) & (marks["close_pnl_sum"] != 0)
        marks.loc[_close_nz, "dot_color"] = marks.loc[_close_nz, "dot_color"].map({"red": "green", "green": "red"})
        marks["dot_text"] = marks.apply(
            lambda r: build_signal_text(str(r["buy_sell"]), float(r["close_pnl_sum"])),
            axis=1,
        )
        long_marks = marks[marks["dot_color"] == "red"].copy()
        short_marks = marks[marks["dot_color"] == "green"].copy()
        if long_marks.shape[0] > 0:
            fig.add_trace(
                go.Scatter(
                    x=long_marks["dt"],
                    y=long_marks["price"],
                    mode="markers+text",
                    text=long_marks["dot_text"],
                    textposition="middle center",
                    marker={"size": 16, "color": "red", "opacity": 0.9},
                    name="做多信号",
                )
            )
        if short_marks.shape[0] > 0:
            fig.add_trace(
                go.Scatter(
                    x=short_marks["dt"],
                    y=short_marks["price"],
                    mode="markers+text",
                    text=short_marks["dot_text"],
                    textposition="middle center",
                    marker={"size": 16, "color": "green", "opacity": 0.9},
                    name="做空信号",
                )
            )

    if not title_text:
        title_text = "user={} symbol={} 昨日1minK线+开平信号".format(uid, symbol)
    fig.update_layout(
        title=title_text,
        xaxis_title="北京时间",
        yaxis_title="价格",
        xaxis_rangeslider_visible=False,
        template="plotly_white",
    )
    fig.write_html(html_path, include_plotlyjs="cdn")


async def send_tg_text(text: str):
    if DRY_RUN:
        print(text)
        return
    tb = ToolBox()
    await tb.send_tg(TG_CHAT_ID, text)


def send_tg_document_sync(file_path: str, caption: str, mime_type: str = "application/octet-stream"):
    if DRY_RUN:
        print("[DRY-RUN] send file: {} | {}".format(file_path, caption))
        return
    url = "https://api.telegram.org/bot{}/sendDocument".format(TG_BOT_TOKEN)
    with open(file_path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": TG_CHAT_ID, "caption": caption},
            files={"document": (os.path.basename(file_path), f, mime_type)},
            timeout=TG_DOCUMENT_TIMEOUT_SEC,
        )
    resp.raise_for_status()
    data = resp.json()
    if not bool(data.get("ok")):
        raise RuntimeError("sendDocument failed: {}".format(data))


async def send_tg_document(file_path: str, caption: str, mime_type: str = "application/octet-stream"):
    await asyncio.to_thread(send_tg_document_sync, file_path, caption, mime_type)


def build_zip_from_html_dir(html_dir: str, day_str: str) -> str:
    zip_path = os.path.join(html_dir, "top{}_kline_html_{}.zip".format(TOP_N, day_str))
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for fn in sorted(os.listdir(html_dir)):
            if not fn.lower().endswith(".html"):
                continue
            full_path = os.path.join(html_dir, fn)
            if os.path.isfile(full_path):
                zf.write(full_path, arcname=fn)
    return zip_path


async def run_once():
    if FORCE_REFRESH_SIM_USERS_ON_RUN:
        clear_excluded_user_ids_cache()
        os.environ["UPM_FETCH_SIM_USERS"] = "1"
        tb_sim = ToolBox()
        try:
            await tb_sim.get_sim_user()
        except Exception as e:
            print("[WARN] get_sim_user 预拉取失败（后续 exclude 仍会重试）: {}".format(e))

    stat_day = pick_stat_day()
    data_dir = (os.environ.get("UPM_PKL_DEALS_DIR") or "").strip() or DATA_DIR
    start_ts, end_ts = day_start_end_ts(stat_day)
    start_ts_3d, end_ts_3d = day_window_ts(stat_day, 3)
    day_str = stat_day.strftime("%Y-%m-%d")
    html_dir = os.path.join(HTML_BASE_DIR, day_str)
    os.makedirs(html_dir, exist_ok=True)

    day_df_all = ensure_user_id(load_day_df(data_dir, stat_day))
    span3_df_all = load_span_days_df(data_dir, stat_day, 3)
    if span3_df_all.shape[0] > 0:
        span3_df_all = ensure_user_id(span3_df_all)
    excluded_ids = await get_excluded_user_ids_async()
    if excluded_ids:
        day_df_all = day_df_all[~day_df_all["user_id"].astype(str).isin(excluded_ids)].copy()
        if isinstance(span3_df_all, pd.DataFrame) and span3_df_all.shape[0] > 0:
            span3_df_all = span3_df_all[~span3_df_all["user_id"].astype(str).isin(excluded_ids)].copy()

    d_rank = day_df_all.copy()
    d_rank["profit_loss"] = pd.to_numeric(d_rank["profit_loss"], errors="coerce").fillna(0.0)
    gp = d_rank.groupby("user_id", as_index=False)["profit_loss"].sum()
    gp = gp.sort_values(["profit_loss", "user_id"], ascending=[False, True]).head(max(1, int(TOP_N)))
    top_users = [str(x) for x in gp["user_id"].tolist()]

    lines = [
        "查询语句: 昨日盈利排名前{}用户".format(max(1, int(TOP_N))),
        "窗口天数: 1",
        "数据覆盖: 1/1",
        "缺失日期: 无",
        "",
    ]
    profit_lines = run_profit_topn(day_df_all, max(1, int(TOP_N)))
    if profit_lines:
        lines.append(profit_lines[0])
        lines.append(repr(top_users))
        lines.extend(profit_lines[1:])
    else:
        lines.extend(profit_lines)
    if len(top_users) >= 2:
        lines.append("")
        lines.append(
            "--- 昨日U本位 Top{} 交易重合用户组（全体腿不区分合约，双向均≥50%，时间≤{}s）---".format(
                max(1, int(TOP_N)),
                PAIR_TRADE_TIME_TOLERANCE_SEC,
            )
        )
        sub_day = day_df_all[day_df_all["user_id"].astype(str).isin(set(top_users))].copy()
        lines.extend(build_top_user_global_overlap_lines(sub_day, top_users))
    await send_tg_text("\n".join(lines))

    if not top_users:
        print("[INFO] {} Top{} 无用户，跳过K线图生成".format(day_str, TOP_N))
        return

    day_df = filter_u_perp_symbols(day_df_all)
    day_df = day_df[day_df["user_id"].astype(str).isin(set(top_users))].copy()
    span3_df = pd.DataFrame()
    if isinstance(span3_df_all, pd.DataFrame) and span3_df_all.shape[0] > 0:
        span3_df = filter_u_perp_symbols(span3_df_all)
        span3_df = span3_df[span3_df["user_id"].astype(str).isin(set(top_users))].copy()
    if day_df.shape[0] == 0:
        print("[WARN] {} Top{} 用户无 U本位永续成交，未生成HTML".format(day_str, TOP_N))
        return

    made = 0
    for uid in top_users:
        user_df = day_df[day_df["user_id"].astype(str) == str(uid)].copy()
        if user_df.shape[0] == 0:
            continue
        symbols = sorted(user_df["symbol"].astype(str).unique().tolist())
        for symbol in symbols:
            us = user_df[user_df["symbol"].astype(str) == symbol].copy()
            try:
                kline_df = fetch_1m_klines(symbol, start_ts, end_ts)
                if kline_df.shape[0] == 0:
                    print("[WARN] {} {} 无K线，跳过".format(uid, symbol))
                    continue
                fn = "{}+{}.html".format(uid, safe_symbol_for_filename(symbol))
                fp = os.path.join(html_dir, fn)
                render_user_symbol_html(uid, symbol, kline_df, us, fp)
                made += 1
                print("[OK] html={}".format(fp))

                kline_df_3d = fetch_1m_klines(symbol, start_ts_3d, end_ts_3d)
                if kline_df_3d.shape[0] == 0:
                    print("[WARN] {} {} 近3天无K线，跳过3d图".format(uid, symbol))
                    continue
                fn_3d = "{}+{}_3d.html".format(uid, safe_symbol_for_filename(symbol))
                fp_3d = os.path.join(html_dir, fn_3d)
                us_3d = us
                if isinstance(span3_df, pd.DataFrame) and span3_df.shape[0] > 0:
                    us_3d = span3_df[
                        (span3_df["user_id"].astype(str) == str(uid))
                        & (span3_df["symbol"].astype(str) == symbol)
                    ].copy()
                    if us_3d.shape[0] == 0:
                        us_3d = us
                render_user_symbol_html(
                    uid,
                    symbol,
                    kline_df_3d,
                    us_3d,
                    fp_3d,
                    title_text="user={} symbol={} 近3天1minK线+开平信号".format(uid, symbol),
                )
                made += 1
                print("[OK] html={}".format(fp_3d))
            except Exception as e:
                print("[ERROR] uid={} symbol={} 生成失败: {}".format(uid, symbol, e))

    sent_zip = 0
    if made > 0:
        try:
            zip_path = build_zip_from_html_dir(html_dir, day_str)
            zip_caption = "昨日盈利Top{} K线HTML打包 {}".format(TOP_N, day_str)
            await send_tg_document(zip_path, zip_caption, "application/zip")
            sent_zip = 1
        except Exception as e:
            print("[ERROR] zip发送TG失败 err={}".format(e))

    print("[DONE] day={} top_users={} html_count={} dir={}".format(day_str, len(top_users), made, html_dir))


def main():
    asyncio.run(run_once())


if __name__ == "__main__":
    main()
