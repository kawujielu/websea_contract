#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1) 拉取并展示 Z1/Z2 用户 ID 及近1个月基本交易情况
2) 从近1个月 PKL 全量用户中筛总盈亏/胜率/盈亏比 Top20
排除：做市账户 + 模拟金（口径同 user_floating_profit.py）

用法:
  cd /home/ubuntu/strategy_base/strategy/openclaw_analysis_user
  python3 /path/to/z1z2_month_top20.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")
DATA_DIR = os.environ.get(
    "UPM_PKL_DEALS_DIR",
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl",
)
ACCOUNT_JSON = os.environ.get(
    "WEBSEA_ACCOUNT_CONFIG",
    os.environ.get("WEBSEA_ACCOUNT_JSON", "/home/ubuntu/CCGo/resources/account.config.json"),
)
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = -5223829567
TOP_N = 20
WINDOW_DAYS = 30
TAGS = ("Z1", "Z2")

_CWD = os.getcwd()
if _CWD not in sys.path:
    sys.path.insert(0, _CWD)

from mongdb_order_stats import calc_trade_stats  # noqa: E402
from pkl_user_query_analyzer import (  # noqa: E402
    _pkl_legs_to_calc_trade_stats_df,
    ensure_user_id,
    expected_dates,
    load_window_df,
)


def send_telegram(text: str, chat_id: int = TG_CHAT_ID) -> None:
    """按行分片发送 TG 群消息（同 daily_symbol_fund_cost_alert）。"""
    url = "https://api.telegram.org/bot{}/sendMessage".format(TG_BOT_TOKEN)
    buf, chunks = "", []
    for line in text.split("\n"):
        piece = line + "\n"
        if len(buf) + len(piece) > 4000:
            if buf:
                chunks.append(buf)
            buf = piece
        else:
            buf += piece
    if buf:
        chunks.append(buf)
    for chunk in chunks:
        data = urllib.parse.urlencode({"chat_id": str(chat_id), "text": chunk}).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                if resp.status != 200 or not json.loads(body).get("ok"):
                    raise RuntimeError(body)
        except urllib.error.URLError as exc:
            raise RuntimeError("发送 Telegram 失败: {}".format(exc)) from exc


def load_risk_api_keys(cfg_path: str) -> Tuple[str, str]:
    with open(cfg_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    api_key_list: Dict[str, List[str]] = {}
    for _k, v in data.get("websea", {}).items():
        if "测试" in (v.get("description") or ""):
            continue
        uid = v.get("uid", "")
        uid = uid.split(",")[0] if "," in uid else uid
        api_key_list[uid or "risk"] = [v["apikey"], v["secret"]]
    if "risk" not in api_key_list:
        raise KeyError("account.config.json 中未找到 risk 账户")
    return str(api_key_list["risk"][0]), str(api_key_list["risk"][1])


def load_mm_uids(path: str) -> Set[str]:
    """做市等业务 uid（同 user_floating_profit.load_mm_uids_from_account_json）。"""
    uids: Set[str] = set()
    if path and os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for _k, v in (data.get("websea") or {}).items():
            if "测试" in (v.get("description") or ""):
                continue
            raw = v.get("uid") or ""
            uid = raw.split(",")[0].strip() if "," in str(raw) else str(raw).strip()
            if uid and uid != "risk":
                uids.add(uid)
    for x in os.environ.get("MM_EXTRA_UIDS", "").split(","):
        x = x.strip()
        if x:
            uids.add(x)
    return uids


async def collect_sim_uids(rest: Any) -> Set[str]:
    """E1~E99 中 tag 含 E 的 user_id（同 user_floating_profit.collect_sim_uids）。"""
    rows: List[Any] = []
    for num in range(1, 100):
        try:
            first = await rest.fetch_tag_list(tag="E{}".format(num), page=1, page_size=1000)
            data = first.get("data") or []
            rows.extend(data)
            total = int((first.get("pager") or {}).get("total_page", 1))
            for page in range(2, total + 1):
                await asyncio.sleep(0.5)
                more = await rest.fetch_tag_list(tag="E{}".format(num), page=page, page_size=1000)
                rows.extend(more.get("data") or [])
        except Exception as e:
            print("[WARN] 拉取 E{} 失败: {}".format(num, e))
    return {
        str(r.get("user_id"))
        for r in rows
        if isinstance(r, dict) and r.get("user_id") is not None and "E" in str(r.get("tag") or "")
    }


async def fetch_tag_uids(rest: Any, tag: str, page_sleep: float = 0.5) -> List[str]:
    seen, out = set(), []

    def consume(rows: Any) -> None:
        for row in rows or []:
            if not isinstance(row, dict) or row.get("user_id") is None:
                continue
            s = str(row["user_id"])
            if s not in seen:
                seen.add(s)
                out.append(s)

    first = await rest.fetch_tag_list(tag=tag, page=1, page_size=1000)
    consume(first.get("data"))
    for page in range(2, int((first.get("pager") or {}).get("total_page", 1)) + 1):
        await asyncio.sleep(page_sleep)
        consume((await rest.fetch_tag_list(tag=tag, page=page, page_size=1000)).get("data"))
    return sorted(out)


def _parse_pct(v: Any) -> float:
    try:
        return float(str(v).strip().replace("%", ""))
    except Exception:
        return 0.0


def _parse_ratio(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def _win_rate01(r: Dict[str, Any]) -> float:
    """胜率转为 0~1。"""
    return _parse_pct(r.get("胜率", 0)) / 100.0


def edge_score(r: Dict[str, Any]) -> float:
    """胜率*盈亏比 - (1-胜率)。"""
    wr = _win_rate01(r)
    return wr * _parse_ratio(r.get("盈亏比", 0)) - (1.0 - wr)


EMPTY_ROW = {
    "总盈亏": 0.0,
    "开平仓次数": 0,
    "胜率": "0%",
    "盈亏比": 0.0,
    "持仓时间中位数": "0 days 00:00:00",
}


def user_metrics(uid: str, sub: pd.DataFrame) -> Dict[str, Any]:
    st = calc_trade_stats(_pkl_legs_to_calc_trade_stats_df(sub, uid), userid=uid)
    return {
        "user_id": uid,
        "总盈亏": float(st.get("总盈亏") or 0.0),
        "开平仓次数": int(st.get("总交易轮次(开平算一次)") or 0),
        "胜率": st.get("胜率", "0%"),
        "盈亏比": st.get("盈亏比", 0.0),
        "持仓时间中位数": st.get("中位数持仓时长", "0 days 00:00:00"),
    }


def calc_all(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for uid, sub in df.groupby(df["user_id"].astype(str)):
        try:
            out[str(uid)] = user_metrics(str(uid), sub)
        except Exception as e:
            print("[WARN] uid={} 统计失败: {}".format(uid, e))
    return out


def fmt_row(r: Dict[str, Any], i: int = 0) -> str:
    prefix = "{:>2}. ".format(i) if i else "    "
    return (
        "{}uid={:<12} 总盈亏={:>12.2f} 开平仓次数={:<5} 胜率={:<8} "
        "盈亏比={:<10} 持仓时间中位数={}"
    ).format(
        prefix, r["user_id"], r["总盈亏"], r["开平仓次数"], r["胜率"], r["盈亏比"], r["持仓时间中位数"]
    )


def print_group(tag: str, uids: List[str], metrics: Dict[str, Dict[str, Any]]) -> None:
    print("\n===== {} 用户ID（{}个）=====".format(tag, len(uids)))
    print(", ".join(uids) if uids else "(空)")
    print("\n===== {} 近1个月基本交易情况 =====".format(tag))
    for uid in uids:
        r = metrics.get(uid) or dict(EMPTY_ROW, user_id=uid)
        print(fmt_row(r))


def print_top(title: str, rows: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    ranked = sorted(rows, key=key_fn, reverse=True)[:TOP_N]
    print("\n【全量PKL {} Top{}】".format(title, TOP_N))
    for i, r in enumerate(ranked, 1):
        print(fmt_row(r, i))
    return ranked


def print_conclusion(
    tag_map: Dict[str, List[str]],
    metrics: Dict[str, Dict[str, Any]],
    top_rows: List[Dict[str, Any]],
) -> str:
    tagged = sorted({u for uids in tag_map.values() for u in uids})
    lines = [
        "===== Z1/Z2 结论 {} =====".format(datetime.now(BJT).strftime("%Y-%m-%d %H:%M")),
        "【建议从 Z1/Z2 删除】总盈亏<5000 且 胜率*盈亏比-(1-胜率)<2",
    ]
    n_del = 0
    for uid in tagged:
        r = metrics.get(uid) or dict(EMPTY_ROW, user_id=uid)
        sc = edge_score(r)
        if r["总盈亏"] < 5000 and sc < 2:
            n_del += 1
            lines.append("  删除 uid={} 总盈亏={:.2f} 指标={:.4f}".format(uid, r["总盈亏"], sc))
    if not n_del:
        lines.append("  (无)")

    tagged_set = set(tagged)
    seen = set()
    lines.append("【建议加入标记组】三榜Top20去重；总盈亏>5000 且 胜率*盈亏比-(1-胜率)>2")
    n_add = 0
    for r in top_rows:
        uid = str(r["user_id"])
        if uid in seen or uid in tagged_set:
            continue
        seen.add(uid)
        sc = edge_score(r)
        if r["总盈亏"] > 5000 and sc > 2:
            n_add += 1
            lines.append("  加入 uid={} 总盈亏={:.2f} 指标={:.4f}".format(uid, r["总盈亏"], sc))
    if not n_add:
        lines.append("  (无)")

    text = "\n".join(lines)
    print("\n" + text)
    return text


async def async_main() -> None:
    root = os.environ.get("WEBSEA_REPO_ROOT", "").strip()
    if root and root not in sys.path:
        sys.path.insert(0, root)
    from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract

    ak, sk = load_risk_api_keys(ACCOUNT_JSON)
    rest = Contract(ak, sk, dev=False)
    rest.DEBUG = False

    mm_uids = load_mm_uids(ACCOUNT_JSON)
    try:
        sim_uids = await collect_sim_uids(rest)
    except Exception as e:
        print("[WARN] 拉取模拟金失败: {}，仅排除做市".format(e))
        sim_uids = set()
    excluded = mm_uids | sim_uids
    print("排除做市uid={} 模拟金uid={} 合计={}".format(len(mm_uids), len(sim_uids), len(excluded)))

    tag_map = {
        tag: [u for u in await fetch_tag_uids(rest, tag) if u not in excluded]
        for tag in TAGS
    }
    for tag, uids in tag_map.items():
        print("{} 用户数(已过滤): {}".format(tag, len(uids)))

    days = expected_dates(WINDOW_DAYS)
    raw, exists, miss = load_window_df(DATA_DIR, days)
    print(
        "近{}天 PKL {}~{} 覆盖{}/{} 缺失:{} | {}".format(
            WINDOW_DAYS, days[0], days[-1], len(exists), len(days),
            ",".join(miss) if miss else "无",
            datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S"),
        )
    )
    if raw.empty:
        print("无 PKL 数据")
        return

    df = ensure_user_id(raw)
    df = df[~df["user_id"].astype(str).isin(excluded)]
    metrics = calc_all(df)
    all_rows = list(metrics.values())
    print("窗口内有成交用户数(已过滤): {}".format(len(all_rows)))

    for tag in TAGS:
        print_group(tag, tag_map[tag], metrics)

    top_rows: List[Dict[str, Any]] = []
    top_rows += print_top("总盈亏最大", all_rows, lambda r: r["总盈亏"])
    top_rows += print_top("胜率最高", all_rows, lambda r: (_parse_pct(r["胜率"]), r["开平仓次数"]))
    top_rows += print_top("盈亏比最大", all_rows, lambda r: (_parse_ratio(r["盈亏比"]), r["总盈亏"]))
    msg = print_conclusion(tag_map, metrics, top_rows)
    try:
        send_telegram(msg)
        print("已发送 TG chat_id={}".format(TG_CHAT_ID))
    except Exception as e:
        print("[WARN] TG 发送失败: {}".format(e))


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

