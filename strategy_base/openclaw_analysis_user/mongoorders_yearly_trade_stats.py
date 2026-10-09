# -*- coding: utf-8 -*-
"""查询 mongoorders：按年日均指标 + 全量交易量 TopN 交易对。"""
from __future__ import annotations

import pandas as pd
import pymysql

# ---- 参数 ----
MYSQL = {
    "host": "abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
    "port": 3306,
    "user": "admin",
    "password": "A(?xvw8~v(ke0(O,=Se!W(!UGBujuh(XkBHuQTRu2",
    "db": "fund",
}
TOP_N = 15
SQL_DAILY = """
SELECT DATE(ts) AS day,
       COUNT(DISTINCT id) AS users,
       COUNT(*) AS trades,
       SUM(ABS(amountQuote)) AS total_amt
FROM mongoorders
GROUP BY DATE(ts)
"""
SQL_TOP = """
SELECT symbol, SUM(ABS(amountQuote)) AS vol
FROM mongoorders
GROUP BY symbol
ORDER BY vol DESC
LIMIT {n}
"""
COLS = ["users", "trades", "total_amt", "amt_per_user", "amt_per_trade"]


def main():
    conn = pymysql.connect(**MYSQL, charset="utf8")
    try:
        df = pd.read_sql(SQL_DAILY, conn)
        top = pd.read_sql(SQL_TOP.format(n=TOP_N), conn)
    finally:
        conn.close()

    if df.empty:
        print("无数据")
        return

    df["amt_per_user"] = df["total_amt"] / df["users"].clip(lower=1)
    df["amt_per_trade"] = df["total_amt"] / df["trades"].clip(lower=1)
    df["year"] = pd.to_datetime(df["day"]).dt.year

    rows = df.groupby("year")[COLS].mean()
    rows.loc["全部"] = df[COLS].mean()

    print("{:<8} {:>12} {:>12} {:>16} {:>14} {:>14}".format(
        "年份", "日均人数", "日均笔数", "日均金额", "日均金额/人", "日均金额/笔"
    ))
    for idx, r in rows.iterrows():
        print("{:<8} {:>12.2f} {:>12.2f} {:>16.2f} {:>14.2f} {:>14.2f}".format(
            idx, r["users"], r["trades"], r["total_amt"], r["amt_per_user"], r["amt_per_trade"]
        ))

    print("\n总交易量 Top{}".format(TOP_N))
    print("{:<6} {:<20} {:>18}".format("排名", "交易对", "总交易量"))
    for i, r in enumerate(top.itertuples(index=False), 1):
        print("{:<6} {:<20} {:>18.2f}".format(i, r.symbol, float(r.vol)))


if __name__ == "__main__":
    main()

