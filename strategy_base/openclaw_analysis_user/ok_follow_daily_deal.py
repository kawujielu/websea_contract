# -*- coding: utf-8 -*-
"""OK跟单成交日报：统计北京时间最近24小时成交，发TG。
crontab 示例（北京时间每天8点）：
  0 8 * * * TZ='Asia/Shanghai' /path/to/python /path/to/ok_follow_daily_deal.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

ACCOUNTS = [
    ("557471", "quant_sky_002@126.com"),
    ("557472", "quant_sky_003@126.com"),
    ("557476", "quant_sky_004@126.com"),
    ("557478", "quant_sky_005@126.com"),
]
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = "-5149924609"
SEND_TG = True

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PKL_DATA_DIR = os.environ.get(
    "UPM_PKL_DEALS_DIR",
    os.environ.get(
        "UPM_DAILY_PKL_DIR",
        os.path.join(_SCRIPT_DIR, "data", "mongo_daily_deals_pkl"),
    ),
)
CSV_DIR = os.path.join(_SCRIPT_DIR, "data", "daily_stats_csv")
CODE_DIRS = [
    _SCRIPT_DIR,
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user",
    r"c:\Users\linji\OneDrive\websea\AI AGENT\sky量化工作的skills\用户分析\code",
]

BJT = ZoneInfo("Asia/Shanghai")
CSV_COLUMNS = [
    "账户", "user_id",
    "总盈亏(扣手续费)", "手续费", "手续费占比",
    "成交笔数", "开平仓轮次", "胜率", "盈亏比",
    "分交易对盈亏", "持仓时间中位数",
]


def _ensure_code_path() -> None:
    for d in CODE_DIRS:
        p = os.path.abspath(d)
        if os.path.isdir(p) and os.path.isfile(os.path.join(p, "pkl_user_query_analyzer.py")):
            if p not in sys.path:
                sys.path.insert(0, p)
            return
    raise SystemExit("找不到用户分析 code 目录，请改 CODE_DIRS")


def last_24h_window():
    end = datetime.now(BJT).replace(microsecond=0)
    start = end - timedelta(hours=24)
    days: list[date] = []
    cur = start.date()
    while cur <= end.date():
        days.append(cur)
        cur += timedelta(days=1)
    return days, start, end


def _clean_symbol_pnl(v) -> str:
    if v is None:
        return ""
    return str(v).replace("\r\n", " | ").replace("\n", " | ").strip()


def row_from_stats(uid: str, name: str, net: float, fee: float, stats: dict) -> dict:
    fee_v = stats.get("总手续费", stats.get("手续费总和", fee))
    return {
        "账户": name,
        "user_id": uid,
        "总盈亏(扣手续费)": round(float(net), 4),
        "手续费": round(float(fee_v or 0), 4),
        "手续费占比": stats.get("手续费占比", "N/A"),
        "成交笔数": stats.get("成交笔数(按订单算)", 0),
        "开平仓轮次": stats.get("总交易轮次(开平算一次)", 0),
        "胜率": stats.get("胜率", "0%"),
        "盈亏比": stats.get("盈亏比", 0.0),
        "分交易对盈亏": _clean_symbol_pnl(stats.get("分币对盈亏", "")),
        "持仓时间中位数": stats.get("中位数持仓时长", "0 days 00:00:00"),
    }


def send_tg_document(file_path: str, caption: str = "") -> None:
    import requests
    url = "https://api.telegram.org/bot{}/sendDocument".format(TG_BOT_TOKEN)
    with open(file_path, "rb") as f:
        data = {"chat_id": TG_CHAT_ID}
        if caption:
            data["caption"] = caption
        r = requests.post(
            url, data=data,
            files={"document": (os.path.basename(file_path), f, "text/csv")},
            timeout=120,
        )
    r.raise_for_status()
    body = r.json()
    if not body.get("ok"):
        raise RuntimeError("TG sendDocument failed: {}".format(body))
    print("TG已发送 chat_id={}".format(TG_CHAT_ID))


def main() -> int:
    _ensure_code_path()
    import pandas as pd
    from mongdb_order_stats import calc_trade_stats
    from pkl_user_query_analyzer import (
        DEFAULT_DATA_DIR,
        _pkl_legs_to_calc_trade_stats_df,
        ensure_user_id,
        load_window_df,
    )

    data_dir = PKL_DATA_DIR if os.path.isdir(PKL_DATA_DIR) else DEFAULT_DATA_DIR
    days, start, end = last_24h_window()
    time_range = "{} ~ {}".format(
        start.strftime("%Y-%m-%d %H:%M"),
        end.strftime("%Y-%m-%d %H:%M"),
    )
    print("PKL={}  窗口={}".format(os.path.abspath(data_dir), time_range))

    df_raw, exists, miss = load_window_df(data_dir, days)
    print("命中PKL={}天 缺失={}天".format(len(exists), len(miss)))

    rows = []
    if df_raw is None or df_raw.empty:
        rows = [row_from_stats(uid, name, 0.0, 0.0, {}) for uid, name in ACCOUNTS]
    else:
        df_u = ensure_user_id(df_raw)
        ts = pd.to_datetime(df_u["ts_text"], errors="coerce")
        lo = pd.Timestamp(start.replace(tzinfo=None))
        hi = pd.Timestamp(end.replace(tzinfo=None))
        df_u = df_u[(ts >= lo) & (ts < hi)].copy()
        want = {str(uid) for uid, _ in ACCOUNTS}
        df_u = df_u[df_u["user_id"].astype(str).isin(want)].copy()

        for uid, name in ACCOUNTS:
            su = str(uid)
            sub = df_u[df_u["user_id"].astype(str) == su]
            if sub.empty:
                rows.append(row_from_stats(uid, name, 0.0, 0.0, {}))
                continue
            try:
                s_df = _pkl_legs_to_calc_trade_stats_df(sub, su)
                if s_df.empty:
                    rows.append(row_from_stats(uid, name, 0.0, 0.0, {}))
                    continue
                fee = float(pd.to_numeric(s_df["fee"], errors="coerce").fillna(0).sum())
                gross = float(pd.to_numeric(s_df["profit_loss"], errors="coerce").fillna(0).sum())
                stats = calc_trade_stats(s_df, userid=su)
                net = stats.get("刨除手续费总盈亏")
                if net is None:
                    net = gross - fee
                rows.append(row_from_stats(uid, name, float(net), fee, stats))
            except Exception as e:
                print("account={} user_id={} 失败: {}".format(name, uid, e))
                rows.append(row_from_stats(uid, name, 0.0, 0.0, {"分币对盈亏": "ERROR:{}".format(e)}))

    out = pd.DataFrame(rows, columns=CSV_COLUMNS)
    os.makedirs(CSV_DIR, exist_ok=True)
    fp = os.path.join(
        CSV_DIR,
        "OK跟单成交日报{}_{}.csv".format(
            start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"),
        ),
    )
    with open(fp, "w", encoding="utf-8-sig", newline="") as f:
        f.write("时间范围,{}\n".format(time_range))
        out.to_csv(f, index=False)
    print("时间范围: {}".format(time_range))
    print(out.to_string(index=False))
    print("CSV: {}".format(os.path.abspath(fp)))

    if SEND_TG:
        try:
            send_tg_document(fp, caption="OK跟单成交日报\n时间范围: {}".format(time_range))
        except Exception as e:
            print("TG发送失败: {}".format(e))
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

