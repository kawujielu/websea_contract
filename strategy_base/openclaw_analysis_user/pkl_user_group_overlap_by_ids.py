# -*- coding: utf-8 -*-
"""
给定一组用户 ID（见下方 USER_IDS），在日切 PKL 成交数据上计算两两「交易重合度」（不区分交易对，全体成交腿汇总）：

- 仅 U 本位永续（symbol 以 -USDT 结尾）；
- 与 pkl_user_group_trade_similarity 在「全市场、不按合约」模式下一致：两腿开平方向一致、
  时间差 ≤ tolerance_sec 即计为一条重合（不要求同一 symbol）；
- 对用户 A、B：重合度_A_rel_B = 100 * matched / len(A 的全部腿)，B 相对 A 对称；
- 双向重合度均 ≥ MIN_OVERLAP（默认 50%）的用户对连边，再经传递性合并为相似用户组；
- 默认输出：各组 user_id 列表，组末附组内最低重合度（仅统计达阈值边，故必 > MIN_OVERLAP×100%）。

可用 --min-overlap 调整阈值；加 --print-pairs 可列出全部用户对明细。

Python 3.9+。
"""
from __future__ import annotations

import argparse
import itertools
import os
import sys
from datetime import date, datetime, timedelta
from collections import defaultdict
from typing import DefaultDict, Dict, List, Sequence, Set, Tuple

import pandas as pd
from zoneinfo import ZoneInfo

_DIR = os.path.dirname(os.path.abspath(__file__))
if _DIR not in sys.path:
    sys.path.insert(0, _DIR)

from pkl_user_group_trade_similarity import (  # noqa: E402
    _match_count,
    build_user_legs,
)
from pkl_user_query_analyzer import ensure_user_id  # noqa: E402

BJT = ZoneInfo("Asia/Shanghai")
DEFAULT_DATA_DIR = os.path.join(_DIR, "data", "mongo_daily_deals_pkl")

# 待分析的用户 ID（直接改这里，无需命令行传 --users）
USER_IDS: List[str] = [
    '632478','640288','575415','573572','613028','584084','585466','515951','643913','491475','501530','533273','640691','610652','574045','619168','640692','674219','679991','572926','642765','537616','538033','614992','670245','573455','674743','489086','543264','596824','598048','608069','619152','632879','642771','644907','558876','617175','633209','643126','643138','646278','670660','514921','537718','578752','583746','594388','598511','599936','630430','647040','674744','485999','491041','553190','558255','599110','601768','611840','624951','624952','629151','638600','647046','1481626','492202','609347','647928','680202','420262','469302','471491','479345','479490','479610','480960','481049','481068','484069','484354','485969','486091','487577','489262','490054','490243','490591','490772','502112','503031','512589','514649','519533','520049','520235','520265','520908','523656','523696','524375','524725','525411','527889','531145','532489','534253','534634','536741','538732','538813','539910','540369','540446','540485','541119','541414','543539','543628','545873','547103','548630','550826','551598','551676','551757','552040','552262','552447','552768','554277','554713','556061','556088','556426','556599','556651','556728','557053','557786','557850','558314','558943','559181','559217','559533','559926','560205','560647','560673','570425','570488','571774','572521','572834','573528','573651','573824','573826','573827','574067','574142','574243','574595','575106','575108','575166','575176','575586','576547','576622','577343','577375','577980','578059','578638','578811','579077','579233','579287','579431','579485','579568','579694','579707','580097','580208','580379','580408','580609','580624','580708','580964','581349','581376','581380','581548','581738','582075','582101','582160','582414','582421','582635','582656','582695','582926','583012','583170','584004','584067','584128','584328','584333','584500','584866','585334','586319','586364','586379','586463','586572','587195','587580','587672','587824','588256','588529','588781','588986','589126','589151','589791','589980','590553','590648','590697','591645','591702','591934','592348','592851','593521','593621','593637','594271','594976','595355','595428','595767','596594','596675','598171','599056','599059','599685','599971','600156','600335','600597','600709','600785','601086','601514','602171','602943','603118','603371','603839','604207','604632','604810','605091','605409','605431','607746','607814','608196','609256','609432','610038','612788','615162','615577','616380','616742','617288','617492','617624','618590','618658','618916','619114','621961','624565','624919','625828','625958','626386','626506','629039','629273','629425','629788','631379','632314','632681','632948','633002','638566','639031','639163','639346','639657','639668','639882','640051','640504','640731','640736','640752','640968','641415','642228','642231','645571','646296','646484','646830','647009','647068','647212','647323','649931','649951','670160','670536','671302','674315','674453','680018','680279','680389','680477','680631','680646','680662','680713','680759','681158','1481243','1481348','1481405','1481457','1481625','1481800','1481830','1481866','1481957','1482124','1482266','1482273','1482277','1482524','1482700','1483480'
]

# 双向重合度均须 ≥ 该比例才归为相似组（0.5 = 50%）；可用 --min-overlap 覆盖
MIN_OVERLAP = 0.5


def filter_u_perp_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """与 daily_profit_top10_kline_report.filter_u_perp_symbols 一致。"""
    d = df.copy()
    d["symbol"] = d["symbol"].astype(str)
    return d[d["symbol"].str.endswith("-USDT", na=False)].copy()


def parse_day(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def iter_days_inclusive(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def load_range_df(data_dir: str, start_d: date, end_d: date) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """合并 [start_d, end_d] 内存在的日切 PKL；返回 (df, loaded_stems, missing_stems)。"""
    dfs: List[pd.DataFrame] = []
    loaded: List[str] = []
    missing: List[str] = []
    for d in iter_days_inclusive(start_d, end_d):
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


class _UnionFind:
    """并查集：用于把「双向重合度均达阈值」的用户对做传递合并。"""

    def __init__(self, nodes: Sequence[str]) -> None:
        self._p: Dict[str, str] = {str(x): str(x) for x in nodes}

    def find(self, x: str) -> str:
        x = str(x)
        while self._p[x] != x:
            self._p[x] = self._p[self._p[x]]
            x = self._p[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._p[ra] = rb


def pairwise_global_overlaps(
    day_trades: pd.DataFrame,
    user_ids: Sequence[str],
    tol_sec: int,
) -> List[Tuple[float, float, float, str, str]]:
    """
    不按合约拆分：全体 U 本位腿上统计（same_symbol=False）。

    返回全部用户对: (min(ra,rb), ra, rb, u1, u2)，按 min 升序。
    任一方无成交腿时，该对双向重合度记为 0。
    """
    uids = [str(x) for x in user_ids]
    if len(uids) < 2:
        return []

    d = filter_u_perp_symbols(day_trades)
    if d.shape[0] == 0:
        return []

    user_legs = build_user_legs(d, uids, same_symbol=False)
    tol = max(0, int(tol_sec))

    pairs: List[Tuple[float, float, float, str, str]] = []

    for u1, u2 in itertools.combinations(uids, 2):
        legs1 = user_legs.get(u1) or []
        legs2 = user_legs.get(u2) or []
        na, nb = len(legs1), len(legs2)
        if na == 0 or nb == 0:
            ra, rb = 0.0, 0.0
        else:
            ma = _match_count(legs1, list(legs2), tol, False)
            mb = _match_count(legs2, list(legs1), tol, False)
            ra = 100.0 * float(ma) / float(na)
            rb = 100.0 * float(mb) / float(nb)
        mmin = min(ra, rb)
        pairs.append((mmin, ra, rb, u1, u2))

    pairs.sort(key=lambda t: (t[0], t[3], t[4]))
    return pairs


def pairwise_global_edges(
    day_trades: pd.DataFrame,
    user_ids: Sequence[str],
    threshold: float,
    tol_sec: int,
) -> List[Tuple[float, float, float, str, str]]:
    """仅含双向均达阈值的边，按 min 降序。"""
    thr_pct = float(threshold) * 100.0
    edges = [
        t for t in pairwise_global_overlaps(day_trades, user_ids, tol_sec) if t[1] >= thr_pct and t[2] >= thr_pct
    ]
    edges.sort(key=lambda t: (-t[0], t[3], t[4]))
    return edges


def clusters_from_edges(all_uids: Sequence[str], edges: List[Tuple[float, float, float, str, str]]) -> List[List[str]]:
    """对输入列表中的用户做并查集合并，返回成员数 ≥2 的组（组内 id 升序）。"""
    uids = [str(x) for x in all_uids]
    uf = _UnionFind(uids)
    for _m, _ra, _rb, u1, u2 in edges:
        uf.union(u1, u2)
    buckets: DefaultDict[str, List[str]] = defaultdict(list)
    for u in uids:
        buckets[uf.find(u)].append(u)
    out: List[List[str]] = []
    for _root, members in buckets.items():
        if len(members) < 2:
            continue
        out.append(sorted(members, key=lambda x: (len(x), x)))
    out.sort(key=lambda g: (min(g, key=lambda x: (len(x), x)), -len(g)))
    return out


def cluster_min_overlap(
    members: List[str],
    edges: List[Tuple[float, float, float, str, str]],
) -> float | None:
    """组内达阈值边的最低 min(双向重合度)。"""
    member_set = set(members)
    mins: List[float] = []
    for mmin, _ra, _rb, u1, u2 in edges:
        if u1 in member_set and u2 in member_set:
            mins.append(float(mmin))
    return min(mins) if mins else None


def format_similar_cluster_lines(
    clusters: List[List[str]],
    edges: List[Tuple[float, float, float, str, str]],
    threshold: float,
    tol_sec: int,
) -> List[str]:
    """格式化相似用户组输出（与 daily_profit_top10_kline_report 一致）。"""
    thr_pct = float(threshold) * 100.0
    if not clusters:
        return [
            "无相似组：任意两用户均未同时达到双向重合度均 ≥ {:.0f}%（时间容差 {} 秒）。".format(
                thr_pct,
                int(tol_sec),
            )
        ]
    lines = ["相似用户组（双向重合度均 ≥ {:.0f}%，传递合并）：".format(thr_pct)]
    for g in clusters:
        g_min = cluster_min_overlap(g, edges)
        inner = ", ".join(g)
        if g_min is not None:
            lines.append("【{}】 组内最低重合度: {:.2f}%".format(inner, g_min))
        else:
            lines.append("【{}】".format(inner))
    return lines


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="给定用户 ID：日切 PKL 上按全体成交腿（不区分合约）算两两重合度，并合并传递相似组",
    )
    p.add_argument(
        "--data-dir",
        type=str,
        default=os.environ.get("UPM_PKL_DEALS_DIR", DEFAULT_DATA_DIR),
        help="日切 PKL 目录（YYYY-MM-DD.pkl）",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default="",
        help="起始自然日 YYYY-MM-DD（含）；与 --end-date 同时省略时仅使用 --end-date 单日",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default="",
        help="结束自然日 YYYY-MM-DD（含）；省略则默认北京时间「昨天」",
    )
    p.add_argument(
        "--min-overlap",
        type=float,
        default=MIN_OVERLAP,
        help="双向重合度均须 ≥ 该值（0~1，脚本内 MIN_OVERLAP 默认 0.5=50%%）",
    )
    p.add_argument(
        "--tolerance-sec",
        type=int,
        default=60,
        help="时间容差（秒），默认 60，与报表 PAIR_TRADE_TIME_TOLERANCE_SEC 一致",
    )
    p.add_argument(
        "--print-details",
        action="store_true",
        help="额外打印载入窗口、缺失日期等运行信息（# 注释行）",
    )
    p.add_argument(
        "--print-pairs",
        action="store_true",
        help="列出全部用户对及双向重合度（仍不区分合约）",
    )
    p.add_argument(
        "--no-exclude",
        action="store_true",
        help="不剔除做市/模拟金用户（默认剔除）",
    )
    args, _ = p.parse_known_args()
    return args


def _normalize_user_ids(raw: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in raw:
        u = str(u).strip()
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def main() -> None:
    args = parse_args()
    users = _normalize_user_ids(USER_IDS)

    if len(users) < 2:
        print("请在脚本顶部 USER_IDS 中配置至少 2 个用户 ID", file=sys.stderr)
        sys.exit(2)

    end_d = parse_day(args.end_date) if args.end_date.strip() else datetime.now(BJT).date() - timedelta(days=1)
    if args.start_date.strip():
        start_d = parse_day(args.start_date)
    else:
        start_d = end_d

    if start_d > end_d:
        print("start-date 不能晚于 end-date", file=sys.stderr)
        sys.exit(2)

    raw, loaded, missing = load_range_df(args.data_dir.strip(), start_d, end_d)
    if raw.shape[0] == 0:
        print("未加载到任何 PKL 行，请检查目录与日期", file=sys.stderr)
        sys.exit(1)

    df = ensure_user_id(raw)
    if not args.no_exclude:
        try:
            from exclude_user_ids import get_excluded_user_ids_sync  # noqa: E402

            excluded = get_excluded_user_ids_sync()
            if excluded:
                df = df[~df["user_id"].astype(str).isin(excluded)].copy()
        except Exception as e:
            print("[WARN] 加载 exclude 失败，继续不排除: {}".format(e), file=sys.stderr)

    # 只保留本脚本关心的用户，减轻 build_user_legs 扫描量
    want = set(users)
    df = df[df["user_id"].astype(str).isin(want)].copy()
    if df.shape[0] == 0:
        print("所选日期范围内，这些用户在 PKL 中无成交腿", file=sys.stderr)
        sys.exit(1)

    thr = float(args.min_overlap)
    if thr < 0 or thr > 1:
        print("--min-overlap 应在 [0,1]", file=sys.stderr)
        sys.exit(2)

    tol = int(args.tolerance_sec)
    edges = pairwise_global_edges(df, users, threshold=thr, tol_sec=tol)
    clusters = clusters_from_edges(users, edges)

    if args.print_details:
        print(
            "# 窗口 {} ~ {} | 载入 {} 天 PKL | 缺失 {} 天".format(
                start_d.isoformat(),
                end_d.isoformat(),
                len(loaded),
                len(missing),
            )
        )
        if missing:
            print("# 缺失日期: {}".format(", ".join(missing[:20]) + (" ..." if len(missing) > 20 else "")))
        print("# min_overlap={} tolerance_sec={}".format(thr, tol))

    for line in format_similar_cluster_lines(clusters, edges, thr, tol):
        print(line)

    if args.print_pairs:
        all_pairs = pairwise_global_overlaps(df, users, tol_sec=tol)
        print("---")
        print("全部用户对明细（全体腿、不区分合约，按重合度升序）：")
        for _m, pra, prb, pu1, pu2 in all_pairs:
            print(
                "  id={} 与 id={}：min={:.2f}%；id={} 相对 id={} {:.2f}%；id={} 相对 id={} {:.2f}%".format(
                    pu1,
                    pu2,
                    _m,
                    pu1,
                    pu2,
                    pra,
                    pu2,
                    pu1,
                    prb,
                )
            )


if __name__ == "__main__":
    main()
