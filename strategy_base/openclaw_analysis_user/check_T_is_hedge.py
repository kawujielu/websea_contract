# -*- coding: utf-8 -*-
"""拉取标记组 T/Z1/Z2 用户 ID，按近1个月 / 全量PKL 两窗口输出交易统计。"""
from __future__ import annotations

import asyncio
import os
import sys
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

# ========== 参数（均写在脚本内）==========
TAGS = ("T", "Z1", "Z2")
RISK_TOKEN = "c1cf4185b2bed317aeb6e6674491fbef"
RISK_SECRET = ""
WEBSEA_REPO_ROOT = "/home/ubuntu/strategy_base"  # 含 crypto_center
_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")
CSV_PATH = os.path.join(_DIR, "data", "tag_tz_users_trade_stats.csv")
RECENT_DAYS = 30  # 近1个月
PRINT_STDOUT = True
PAGE_SLEEP = 0.5
# ========================================

BJT = ZoneInfo("Asia/Shanghai")
for p in (_DIR, WEBSEA_REPO_ROOT):
    if p and p not in sys.path:
        sys.path.insert(0, p)

from mongdb_order_stats import calc_trade_stats  # noqa: E402
from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402
from pkl_target_users_trade_stats import (  # noqa: E402
    OUTPUT_METRICS,
    ensure_user_id,
    extract_metrics,
    filter_bjt_date_range,
    filter_target_users_raw,
    list_pkl_paths,
    load_concat_pkls,
    pkl_legs_to_calc_frame,
)


async def fetch_tag_uids(rest, tag: str) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []

    def consume(rows):
        for row in rows or []:
            uid = row.get("user_id") if isinstance(row, dict) else None
            if uid is None:
                continue
            s = str(uid)
            if s not in seen:
                seen.add(s)
                out.append(s)

    first = await rest.fetch_tag_list(tag=tag, page=1, page_size=1000)
    consume(first.get("data"))
    total = int((first.get("pager") or {}).get("total_page", 1))
    for page in range(2, total + 1):
        await asyncio.sleep(PAGE_SLEEP)
        consume((await rest.fetch_tag_list(tag=tag, page=page, page_size=1000)).get("data"))
    return out


async def load_tag_users() -> Tuple[Tuple[str, ...], Dict[str, str]]:
    from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract

    rest = Contract(RISK_TOKEN, RISK_SECRET, dev=False)
    rest.DEBUG = False
    uid_tags: Dict[str, List[str]] = {}
    for tag in TAGS:
        uids = await fetch_tag_uids(rest, tag)
        print("{} 用户数: {}  ids={}".format(tag, len(uids), uids))
        for u in uids:
            uid_tags.setdefault(u, []).append(tag)
    ordered = tuple(sorted(uid_tags.keys()))
    tag_str = {u: ",".join(ts) for u, ts in uid_tags.items()}
    return ordered, tag_str


def run_analysis_recent_full(data_dir: str, uids: Tuple[str, ...], recent_days: int):
    """两窗口：近 recent_days 天 + 全量 PKL。"""
    paths = list_pkl_paths(data_dir)
    notes = ["PKL 目录: {}".format(os.path.abspath(data_dir)), "载入文件数: {}".format(len(paths))]
    if not paths:
        notes.append("警告: 未找到任何 YYYY-MM-DD.pkl")
        return pd.DataFrame(), notes

    raw = load_concat_pkls(paths)
    notes.append("原始行数: {}".format(raw.shape[0]))
    excluded = get_excluded_user_ids_sync()
    effective = tuple(u for u in uids if str(u) not in excluded)
    if len(effective) != len(uids):
        notes.append("剔除模拟金/做市后: {} -> {}".format(len(uids), len(effective)))
    if not effective:
        notes.append("目标用户全部被剔除")
        return pd.DataFrame(), notes

    raw = filter_target_users_raw(raw, effective)
    notes.append("目标用户相关行数: {}".format(raw.shape[0]))
    if raw.shape[0] == 0:
        return pd.DataFrame(), notes

    legs = ensure_user_id(raw)
    legs = legs[legs["user_id"].isin(set(effective))].copy()

    end_d = datetime.now(BJT).date()
    start_d = end_d - timedelta(days=recent_days - 1)
    legs_recent = filter_bjt_date_range(legs, start_d, end_d)
    label_recent = "近{}天({}~{})".format(recent_days, start_d.isoformat(), end_d.isoformat())
    notes.append("窗口「{}」行数: {}".format(label_recent, legs_recent.shape[0]))

    rows = []
    for uid in effective:
        for label, sub in (
            (label_recent, legs_recent[legs_recent["user_id"] == uid]),
            ("全量PKL", legs[legs["user_id"] == uid]),
        ):
            cdf = pkl_legs_to_calc_frame(sub)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                stats = calc_trade_stats(cdf, userid=str(uid))
            m = extract_metrics(stats)
            m["user_id"] = uid
            m["窗口"] = label
            rows.append(m)
    return pd.DataFrame(rows), notes


def main() -> int:
    uids, tag_str = asyncio.run(load_tag_users())
    if not uids:
        print("未拉到任何标记组用户")
        return 1

    print("目标用户合计去重: {}  DATA_DIR={}".format(len(uids), os.path.abspath(DATA_DIR)))
    df, notes = run_analysis_recent_full(DATA_DIR, uids, RECENT_DAYS)
    if df.shape[0]:
        df.insert(1, "标记组", df["user_id"].map(lambda x: tag_str.get(str(x), "")))
        cols = ["user_id", "标记组", "窗口"] + [b for _, b in OUTPUT_METRICS]
        df = df[[c for c in cols if c in df.columns]]

    text = "\n".join(notes + [""] + ([df.to_string(index=False)] if df.shape[0] else ["无输出"]))
    if PRINT_STDOUT:
        print(text)

    path = os.path.abspath(CSV_PATH)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print("CSV 已写入: {}".format(path))
    return 0


if __name__ == "__main__":
    sys.exit(main())

