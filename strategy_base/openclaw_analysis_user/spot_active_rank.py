#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
现货 mongoorders：按交易对统计活跃度并排名（最不活跃 TopN）。

用法（Ubuntu 服务器）
--------------------
1) 依赖（建议 Python 3.9+）：
   pip3 install mysql-connector-python
   # 若系统无 zoneinfo（极少见），可：pip3 install tzdata

2) 进入脚本目录后直接运行（二选一：--weeks 或 --months）：
   python3 spot_active_rank.py --weeks 2
   python3 spot_active_rank.py --months 1
   python3 spot_active_rank.py --weeks 4 --top 10

3) 参数说明：
   --weeks N   近 N 周（按 7*N 个自然日，北京时间）
   --months N  近 N 月（按 30*N 个自然日）
   --top N     输出最不活跃前 N 名，默认 5，范围 1~50

4) MySQL 连接（可用环境变量覆盖，密码建议用环境变量）：
   export SPOT_FUND_MYSQL_HOST='...'
   export SPOT_FUND_MYSQL_USER='admin'
   export SPOT_FUND_MYSQL_PASSWORD='...'
   export SPOT_FUND_MYSQL_DATABASE='fund'
   export SPOT_FUND_MYSQL_PORT='3306'
   # ts 字符串格式：iso（默认）| compact | slash
   export SPOT_FUND_TS_STR_FORMAT='iso'

示例输出：最不活跃交易对列表（交易金额从小到大）。
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

BJT = ZoneInfo("Asia/Shanghai")

# 连接参数可通过环境变量覆盖（密码请务必用环境变量，勿写入版本库）
MYSQL_HOST = os.environ.get(
    "SPOT_FUND_MYSQL_HOST",
    "abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
)
MYSQL_USER = os.environ.get("SPOT_FUND_MYSQL_USER", "admin")
MYSQL_PASSWORD = os.environ.get("SPOT_FUND_MYSQL_PASSWORD", "A(?xvw8~v(ke0(O,=Se!W(!UGBujuh(XkBHuQTRu2")
MYSQL_DATABASE = os.environ.get("SPOT_FUND_MYSQL_DATABASE", "fund")
MYSQL_PORT = int(os.environ.get("SPOT_FUND_MYSQL_PORT", "3306"))

# ts 在库中为字符串时与 contract_active_rank 一致：iso / compact / slash
_DATE_STR_STYLE = os.environ.get("SPOT_FUND_TS_STR_FORMAT", "iso").lower().strip()


def _ts_bounds_str(start_dt: datetime, end_dt: datetime) -> Tuple[str, str]:
    """mongoorders.ts 多为 datetime 字符串，区间 [start, end] 闭区间片段。"""

    def _fmt(dt: datetime) -> str:
        if _DATE_STR_STYLE == "compact":
            return dt.strftime("%Y%m%d%H%M%S")
        if _DATE_STR_STYLE == "slash":
            return dt.strftime("%Y/%m/%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    return _fmt(start_dt), _fmt(end_dt)


def parse_args():
    p = argparse.ArgumentParser(description="现货 mongoorders：按交易对统计活跃度并排名")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--weeks", type=int, help="近 N 周（7*N 个自然日，北京时间）")
    g.add_argument("--months", type=int, help="近 N 月（30*N 个自然日）")
    p.add_argument("--top", type=int, default=5, help="最不活跃前 N，默认 5")
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


def bjt_datetime_range_inclusive_days(num_days: int) -> Tuple[datetime, datetime]:
    n = max(1, int(num_days))
    end = datetime.now(BJT).replace(second=59, microsecond=999999)
    start_day = end.date() - timedelta(days=n - 1)
    start = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0, tzinfo=BJT)
    return start, end


def _connect():
    import mysql.connector

    if not MYSQL_PASSWORD:
        raise RuntimeError(
            "请设置环境变量 SPOT_FUND_MYSQL_PASSWORD（勿把密码写进脚本提交仓库）"
        )
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
    )


def fetch_symbol_stats(
    cursor: Any, ts_lo: str, ts_hi: str
) -> List[Tuple[Any, ...]]:
    sql = (
        "SELECT symbol, "
        "COUNT(DISTINCT `order`) AS order_cnt, "
        "COUNT(DISTINCT id) AS user_cnt, "
        "COALESCE(SUM(ABS(amountQuote)), 0) AS sum_quote "
        "FROM mongoorders "
        "WHERE ts >= %s AND ts <= %s "
        "GROUP BY symbol"
    )
    cursor.execute(sql, (ts_lo, ts_hi))
    return list(cursor.fetchall() or [])


def build_report(
    start_dt: datetime, end_dt: datetime, window_label: str, top: int
) -> str:
    ts_lo, ts_hi = _ts_bounds_str(start_dt, end_dt)
    lines = [
        "现货活跃度排名（mongoorders）",
        "统计窗口：近{} | ts范围：{} ~ {}（北京时间）".format(
            window_label, ts_lo, ts_hi
        ),
        "",
    ]
    conn = _connect()
    try:
        cur = conn.cursor()
        rows = fetch_symbol_stats(cur, ts_lo, ts_hi)
        cur.close()
    finally:
        conn.close()

    if not rows:
        lines.append("（窗口内无成交数据）")
        return "\n".join(lines) + "\n"

    # 按交易金额从小到大
    least = sorted(rows, key=lambda r: (float(r[3] or 0), str(r[0] or "")))[:top]
    lines.append("【最不活跃交易对 Top{}】（交易金额从小到大）".format(top))
    for i, r in enumerate(least, start=1):
        sym, oc, uc, sq = r
        lines.append(
            "{}. {}  成交笔数:{}  用户数:{}  交易金额:{}".format(
                i, sym, int(oc or 0), int(uc or 0), float(sq or 0)
            )
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    try:
        args = parse_args()
        top = max(1, min(500, int(args.top)))
        num_days, window_label = window_days(args.weeks, args.months)
        start_dt, end_dt = bjt_datetime_range_inclusive_days(num_days)
        text = build_report(start_dt, end_dt, window_label, top)
        sys.stdout.write(text)
        return 0
    except ValueError as e:
        sys.stderr.write("{}\n".format(e))
        return 2
    except Exception as e:
        sys.stderr.write("spot_active_rank 失败: {}\n".format(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


