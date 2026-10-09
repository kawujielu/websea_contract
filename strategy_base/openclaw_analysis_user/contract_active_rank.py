from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

MYSQL_HOST = os.environ.get("UPM_CONTRACT_MYSQL_HOST", "10.0.208.249")
MYSQL_USER = os.environ.get("UPM_CONTRACT_MYSQL_USER", "lh_sky")
MYSQL_PASSWORD = os.environ.get("UPM_CONTRACT_MYSQL_PASSWORD", "sky_123456")
MYSQL_DATABASE = os.environ.get("UPM_CONTRACT_MYSQL_DATABASE", "contract_dws")
MYSQL_PORT = int(os.environ.get("UPM_CONTRACT_MYSQL_PORT", "33306"))

DATE_COL = os.environ.get("UPM_CONTRACT_RANK_DATE_COLUMN", "date")

PKL_DIR = os.environ.get(
    "UPM_PKL_DEALS_DIR",
    "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl",
)


def parse_args():
    p = argparse.ArgumentParser(description="合约活跃度：窗口内最不活跃的交易对（Top5）")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--weeks", type=int, help="统计近 N 周（按 7*N 个自然日，北京时间）")
    g.add_argument("--months", type=int, help="统计近 N 月（按 30*N 个自然日，与群内时间口径一致）")
    p.add_argument("--top", type=int, default=5, help="每个维度取最不活跃前 N 个，默认 5")
    args, _ = p.parse_known_args()
    return args


def window_days(weeks: Optional[int], months: Optional[int]) -> Tuple[int, str]:
    if months is not None:
        m = int(months)
        if m > 0:
            return m * 30, "{}月".format(m)
    if weeks is not None:
        w = int(weeks)
        if w > 0:
            return w * 7, "{}周".format(w)
    raise ValueError("weeks/months 须为正整数")


def bjt_date_range_inclusive(num_days: int) -> Tuple[date, date]:
    """近 num_days 个自然日（含今天），北京时间。"""
    n = max(1, int(num_days))
    end_d = datetime.now(BJT).date()
    start_d = end_d - timedelta(days=n - 1)
    return start_d, end_d


def _iter_days(start_d: date, end_d: date) -> List[date]:
    out: List[date] = []
    cur = start_d
    while cur <= end_d:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _connect():
    import mysql.connector

    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
    )


def _date_expr(dc: str) -> str:
    return "LEFT(TRIM({}), 10)".format(dc)


def _diag_table(cursor, table: str) -> str:
    dc = "`{}`".format(DATE_COL.replace("`", ""))
    try:
        cursor.execute("SELECT COUNT(*), MIN({d}), MAX({d}) FROM {t}".format(d=dc, t=table))
        cnt, dmin, dmax = cursor.fetchone()
        return "诊断: 全表行数={} 日期范围={} ~ {}".format(cnt, dmin, dmax)
    except Exception as e:
        return "诊断失败: {}".format(e)


def _fetch_least_active_mysql(
    cursor,
    table: str,
    sum_col: str,
    start_d: date,
    end_d: date,
    top: int,
) -> List[Tuple[str, float]]:
    dc = "`{}`".format(DATE_COL.replace("`", ""))
    col = "`{}`".format(sum_col.replace("`", ""))
    sql = (
        "SELECT symbol, COALESCE(SUM({col}), 0) AS total_v "
        "FROM {table} "
        "WHERE {de} >= %s AND {de} <= %s "
        "GROUP BY symbol "
        "ORDER BY total_v ASC "
        "LIMIT %s"
    ).format(col=col, table=table, de=_date_expr(dc))
    cursor.execute(sql, (start_d.isoformat(), end_d.isoformat(), int(top)))
    rows = cursor.fetchall()
    out = []
    for r in rows:
        sym = str(r[0]) if r[0] is not None else ""
        out.append((sym, float(r[1] or 0.0)))
    return out


def _ensure_user_id(df):
    """兼容日切 PKL：已有 user_id，或 taker/maker 双行结构。"""
    import pandas as pd

    d = df.copy()
    if "user_id" in d.columns:
        d["user_id"] = d["user_id"].astype(str)
        return d
    if "taker_user" not in d.columns or "maker_user" not in d.columns:
        raise ValueError("pkl 缺少 user_id / taker_user / maker_user")
    grp_cols = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    for c in grp_cols:
        if c not in d.columns:
            raise ValueError("pkl 缺少字段 {}".format(c))
    d = d.reset_index(drop=True)
    d["_idx_in_pair"] = d.groupby(grp_cols).cumcount()
    d["user_id"] = d["taker_user"].astype(str)
    odd = (d["_idx_in_pair"] % 2) == 1
    d.loc[odd, "user_id"] = d.loc[odd, "maker_user"].astype(str)
    return d.drop(columns=["_idx_in_pair"])


def _load_excluded() -> Set[str]:
    """尽量剔除做市/模拟金；失败则不过滤。"""
    try:
        from exclude_user_ids import get_excluded_user_ids_sync

        return set(get_excluded_user_ids_sync() or [])
    except Exception:
        return set()


def _fetch_least_active_users_pkl(
    start_d: date,
    end_d: date,
    top: int,
    pkl_dir: str,
) -> Tuple[List[Tuple[str, float]], str]:
    """
    从日切 PKL 统计各交易对「日交易用户数」之和（口径对齐原 SUM(user_num)），
    取合计最小的 top 个。
    """
    import pandas as pd

    if not os.path.isdir(pkl_dir):
        raise FileNotFoundError("PKL 目录不存在: {}".format(pkl_dir))

    excluded = _load_excluded()
    # symbol -> sum of daily unique user counts
    totals: Dict[str, float] = {}
    exists: List[str] = []
    miss: List[str] = []

    for d in _iter_days(start_d, end_d):
        stem = d.isoformat()
        fp = os.path.join(pkl_dir, "{}.pkl".format(stem))
        if not os.path.isfile(fp):
            miss.append(stem)
            continue
        exists.append(stem)
        raw = pd.read_pickle(fp)
        if raw is None or len(raw) == 0:
            continue
        df = _ensure_user_id(raw)
        if excluded:
            df = df[~df["user_id"].astype(str).isin(excluded)]
        if "symbol" not in df.columns or df.empty:
            continue
        day_cnt = df.groupby("symbol")["user_id"].nunique()
        for sym, n in day_cnt.items():
            s = str(sym)
            totals[s] = totals.get(s, 0.0) + float(n)

    note = "PKL目录={} 命中{}天 缺失{}天".format(pkl_dir, len(exists), len(miss))
    if miss and len(miss) <= 10:
        note += " 缺失:{}".format(",".join(miss))
    elif miss:
        note += " 缺失例:{}".format(",".join(miss[:5]))

    if not totals:
        return [], note

    ranked = sorted(totals.items(), key=lambda x: (x[1], x[0]))[: int(top)]
    return ranked, note


def build_report(start_d: date, end_d: date, window_label: str, top: int) -> str:
    lines = [
        "合约活跃度（最不活跃交易对 Top{}）".format(top),
        "统计窗口：近{} | 日期范围：{} ~ {}（北京时间）".format(
            window_label, start_d.isoformat(), end_d.isoformat()
        ),
        "",
    ]
    conn = _connect()
    try:
        cur = conn.cursor()

        # 1) 成交金额 — MySQL
        lines.append("【按成交金额usdt合计最小】")
        try:
            rows = _fetch_least_active_mysql(cur, "hot_vol_rank", "deal_amt", start_d, end_d, top)
            if not rows:
                lines.append("（无数据）")
                lines.append(_diag_table(cur, "hot_vol_rank"))
            else:
                for i, (sym, val) in enumerate(rows, start=1):
                    lines.append("{}. {}  {}".format(i, sym, val))
        except Exception as e:
            lines.append("查询失败: {}".format(e))
            lines.append(_diag_table(cur, "hot_vol_rank"))
        lines.append("")

        # 2) 交易用户数 — 日切 PKL 成交明细
        lines.append("【按交易用户数合计最小】")
        try:
            rows, note = _fetch_least_active_users_pkl(start_d, end_d, top, PKL_DIR)
            lines.append("({})".format(note))
            if not rows:
                lines.append("（无数据）")
            else:
                for i, (sym, val) in enumerate(rows, start=1):
                    lines.append("{}. {}  {}".format(i, sym, val))
        except Exception as e:
            lines.append("查询失败: {}".format(e))
        lines.append("")

        # 3) 手续费 — MySQL
        lines.append("【按手续费合计最小】")
        try:
            rows = _fetch_least_active_mysql(cur, "symbol_fee_rank", "fee", start_d, end_d, top)
            if not rows:
                lines.append("（无数据）")
                lines.append(_diag_table(cur, "symbol_fee_rank"))
            else:
                for i, (sym, val) in enumerate(rows, start=1):
                    lines.append("{}. {}  {}".format(i, sym, val))
        except Exception as e:
            lines.append("查询失败: {}".format(e))
            lines.append(_diag_table(cur, "symbol_fee_rank"))
        lines.append("")

        cur.close()
    finally:
        conn.close()
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    try:
        args = parse_args()
        top = max(1, min(50, int(args.top)))
        num_days, window_label = window_days(args.weeks, args.months)
        start_d, end_d = bjt_date_range_inclusive(num_days)
        text = build_report(start_d, end_d, window_label, top)
        sys.stdout.write(text)
        return 0
    except ValueError as e:
        sys.stderr.write("{}\n".format(e))
        return 2
    except Exception as e:
        sys.stderr.write("contract_active_rank 失败: {}\n".format(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

