# -*- coding: utf-8 -*-
"""统计日切 PKL 中全部成交的 taker / maker 名义金额，并拆分做市商 vs 普通用户。"""
import os
import re

import pandas as pd

DATA_DIR = "/home/ubuntu/strategy_base/strategy/openclaw_analysis_user/data/mongo_daily_deals_pkl"
PKL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.pkl$")

# 做市商账户
MM_IDS = {
    "100023",
    "100024",
    "100025",
    "617918",
    "617914",
    "617921",
    "617923",
    "617925",
}


def main():
    files = sorted(
        os.path.join(DATA_DIR, n)
        for n in os.listdir(DATA_DIR)
        if PKL_RE.match(n)
    )
    if not files:
        raise FileNotFoundError("无 pkl: {}".format(DATA_DIR))

    df = pd.concat([pd.read_pickle(p) for p in files], ignore_index=True)

    # 兼容 camelCase / snake_case
    if "taker_user" not in df.columns and "takerUser" in df.columns:
        df = df.rename(columns={"takerUser": "taker_user", "makerUser": "maker_user"})
    if "taker_user" not in df.columns or "maker_user" not in df.columns:
        raise ValueError("缺少 taker_user / maker_user（或 takerUser / makerUser）")

    # 每笔成交两行：偶数为 taker 腿，奇数为 maker 腿
    gcols = ["ts_text", "symbol", "price", "amount", "taker_user", "maker_user"]
    idx = df.groupby(gcols).cumcount()
    notional = (
        pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
        * pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        * pd.to_numeric(df["face_value"], errors="coerce").fillna(1.0)
    )

    is_taker_leg = idx % 2 == 0
    is_maker_leg = idx % 2 == 1
    taker_is_mm = df["taker_user"].astype(str).isin(MM_IDS)
    maker_is_mm = df["maker_user"].astype(str).isin(MM_IDS)

    taker_mm_amt = float(notional[is_taker_leg & taker_is_mm].sum())
    taker_normal_amt = float(notional[is_taker_leg & ~taker_is_mm].sum())
    maker_mm_amt = float(notional[is_maker_leg & maker_is_mm].sum())
    maker_normal_amt = float(notional[is_maker_leg & ~maker_is_mm].sum())

    taker_amt = taker_mm_amt + taker_normal_amt
    maker_amt = maker_mm_amt + maker_normal_amt
    total = taker_amt + maker_amt

    def pct(part, whole):
        return part / whole if whole else 0.0

    print("pkl文件数: {}".format(len(files)))
    print("腿行数: {}".format(len(df)))
    print("做市商id: {}".format(",".join(sorted(MM_IDS))))
    print("---")
    print("taker金额: {:.4f}  占总计: {:.4%}".format(taker_amt, pct(taker_amt, total)))
    print("  做市商: {:.4f}  占taker: {:.4%}".format(taker_mm_amt, pct(taker_mm_amt, taker_amt)))
    print("  普通用户: {:.4f}  占taker: {:.4%}".format(taker_normal_amt, pct(taker_normal_amt, taker_amt)))
    print("maker金额: {:.4f}  占总计: {:.4%}".format(maker_amt, pct(maker_amt, total)))
    print("  做市商: {:.4f}  占maker: {:.4%}".format(maker_mm_amt, pct(maker_mm_amt, maker_amt)))
    print("  普通用户: {:.4f}  占maker: {:.4%}".format(maker_normal_amt, pct(maker_normal_amt, maker_amt)))
    print("合计: {:.4f}".format(total))


if __name__ == "__main__":
    main()

