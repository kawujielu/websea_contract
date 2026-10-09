# -*- coding: utf-8 -*-
"""前一天 PKL：找出双向相反方向 AB 仓关联用户组（仅开仓重合，忽略平仓），结果发 Telegram。

crontab 示例（每天 10:00 北京时间）:
  0 10 * * * cd /path/to/code && /usr/bin/python3 find_ab_warehouse_groups_30d.py
"""
from __future__ import annotations

import bisect
import itertools
import json
import os
import sys
import types
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import DefaultDict, List, Sequence
from zoneinfo import ZoneInfo

# 服务器部分脚本会 import scopefi_log；缺失时用空模块占位，避免 ModuleNotFoundError
try:
    import scopefi_log  # noqa: F401
except ImportError:
    sys.modules["scopefi_log"] = types.ModuleType("scopefi_log")

import pandas as pd

# ========== 参数 ==========
_DIR = os.path.dirname(os.path.abspath(__file__))
PKL_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")
RECENT_DAYS = 1  # 仅前一天
MIN_OVERLAP = 0.1
TOLERANCE_SEC = 60
EXCLUDE_MM_SIM = True
CSV_PATH = os.path.join(_DIR, "data", "ab_warehouse_groups_1d.csv")
PRINT_STDOUT = True
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = -5223829567
# ==========================

BJT = ZoneInfo("Asia/Shanghai")
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from pkl_user_group_trade_similarity import build_user_legs  # noqa: E402
from pkl_user_query_analyzer import ensure_user_id  # noqa: E402

# 仅开仓相反：开多↔开空（平多/平空不参与）
OPEN_SIDES = {"开多", "开空"}
OPPOSITE_SIDE = {"开多": "开空", "开空": "开多"}


def load_range_df(data_dir: str, start_d: date, end_d: date):
    dfs, loaded, missing = [], [], []
    cur = start_d
    while cur <= end_d:
        stem = cur.isoformat()
        fp = os.path.join(data_dir, stem + ".pkl")
        if os.path.isfile(fp):
            dfs.append(pd.read_pickle(fp))
            loaded.append(stem)
        else:
            missing.append(stem)
        cur += timedelta(days=1)
    if not dfs:
        return pd.DataFrame(), loaded, missing
    return pd.concat(dfs, ignore_index=True), loaded, missing


def _match_opposite(legs_q, legs_ref, tol_sec: int) -> int:
    by_side: DefaultDict[str, List[int]] = defaultdict(list)
    for ts, _sym, side in legs_ref:
        by_side[side].append(ts)
    for side in by_side:
        by_side[side].sort()
    matched, tol = 0, max(0, int(tol_sec))
    for ts_a, _s, side_a in legs_q:
        opp = OPPOSITE_SIDE.get(side_a)
        if not opp or opp not in by_side:
            continue
        ts_list = by_side[opp]
        lo = bisect.bisect_left(ts_list, ts_a - tol)
        hi = bisect.bisect_right(ts_list, ts_a + tol)
        if lo < hi:
            matched += 1
    return matched


class _UF:
    def __init__(self, nodes: Sequence[str]):
        self._p = {str(x): str(x) for x in nodes}

    def find(self, x: str) -> str:
        while self._p[x] != x:
            self._p[x] = self._p[self._p[x]]
            x = self._p[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._p[ra] = rb


def send_telegram(text: str, chat_id: int = TG_CHAT_ID) -> None:
    """按行分片发送 TG 群消息。"""
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


def main() -> int:
    end_d = datetime.now(BJT).date() - timedelta(days=1)
    start_d = end_d - timedelta(days=RECENT_DAYS - 1)
    raw, loaded, missing = load_range_df(PKL_DIR, start_d, end_d)
    header = "【AB仓日报】日期 {}  载入{}天  缺失{}天".format(
        end_d.isoformat(), len(loaded), len(missing)
    )
    print(header)
    if raw.empty:
        msg = "{}\n无 PKL 数据".format(header)
        print(msg)
        send_telegram(msg)
        return 1

    df = ensure_user_id(raw)
    if EXCLUDE_MM_SIM:
        try:
            from exclude_user_ids import get_excluded_user_ids_sync
            excl = get_excluded_user_ids_sync()
            if excl:
                df = df[~df["user_id"].astype(str).isin(excl)].copy()
        except Exception as e:
            print("[WARN] exclude 失败: {}".format(e))

    df = df[df["symbol"].astype(str).str.endswith("-USDT", na=False)].copy()
    users = sorted(df["user_id"].astype(str).unique().tolist(), key=lambda x: (len(x), x))
    print("用户数={}  成交腿={}".format(len(users), len(df)))
    if len(users) < 2:
        msg = "{}\n用户不足 2，无法配对".format(header)
        print(msg)
        send_telegram(msg)
        return 1

    user_legs = build_user_legs(df, users, same_symbol=False)
    # 只保留开仓腿
    user_legs = {
        u: [leg for leg in legs if leg[2] in OPEN_SIDES]
        for u, legs in user_legs.items()
    }
    thr_pct = MIN_OVERLAP * 100.0
    edges = []
    for u1, u2 in itertools.combinations(users, 2):
        legs1, legs2 = user_legs.get(u1) or [], user_legs.get(u2) or []
        na, nb = len(legs1), len(legs2)
        if na == 0 or nb == 0 or na != nb:
            continue
        ma = _match_opposite(legs1, legs2, TOLERANCE_SEC)
        mb = _match_opposite(legs2, legs1, TOLERANCE_SEC)
        ra = 100.0 * ma / na
        rb = 100.0 * mb / nb
        if ra >= thr_pct and rb >= thr_pct:
            edges.append((min(ra, rb), ra, rb, min(ma, mb), u1, u2))

    uf = _UF(users)
    for _m, _ra, _rb, _cnt, u1, u2 in edges:
        uf.union(u1, u2)
    buckets: DefaultDict[str, List[str]] = defaultdict(list)
    for u in users:
        buckets[uf.find(u)].append(u)
    clusters = [sorted(m, key=lambda x: (len(x), x)) for m in buckets.values() if len(m) >= 2]
    clusters.sort(key=lambda g: (-len(g), g[0]))

    lines = [
        header,
        "用户数={}  成交腿={}  达阈值边数={}  AB仓关联组数={}".format(
            len(users), len(df), len(edges), len(clusters)
        ),
    ]
    rows = []
    for i, g in enumerate(clusters, 1):
        ms = set(g)
        intra = [(m, cnt) for m, _a, _b, cnt, u1, u2 in edges if u1 in ms and u2 in ms]
        g_min, g_cnt = min(intra, key=lambda x: x[0]) if intra else (0.0, 0)
        line = "组{} 【{}】 人数={} 重合度={:.2f}% 重合笔数={}".format(
            i, ", ".join(g), len(g), g_min, g_cnt
        )
        lines.append(line)
        rows.append(
            {
                "组号": i,
                "用户": ",".join(g),
                "人数": len(g),
                "重合度%": round(g_min, 2),
                "重合笔数": g_cnt,
            }
        )

    if not clusters:
        lines.append("前一天未发现仅开仓双向相反方向 AB 仓关联组")

    report = "\n".join(lines)
    if PRINT_STDOUT:
        print(report)

    out = pd.DataFrame(rows, columns=["组号", "用户", "人数", "重合度%", "重合笔数"])
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)
    out.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print("CSV: {}".format(os.path.abspath(CSV_PATH)))

    send_telegram(report)
    print("已发送 TG chat_id={}".format(TG_CHAT_ID))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

