"""查询现货 Mongo `exchange.real_deal` 成交明细。

口径（对齐 monitor/getmongo、spot_paddington/abc_asset）：
- type = fromUser 方向（buy/sell）；toUser 为反向
- 可按用户、日期、交易对过滤；时间按北京时间

运行:
  py mongdb_spot_deal.py
依赖: pip install motor pandas
"""
from __future__ import annotations

import asyncio
import datetime
from typing import List, Optional

import motor.motor_asyncio
import pandas as pd

# ========== 配置 ==========
MONGO_URI = "mongodb://root:ddRjILDzAvuBbcf0@10.51.102.18:27018/"
USER_IDS = ["1484781"]  # 空列表=不按用户过滤（慎用，量大）
BEGIN_DATE = "2026-09-01"  # 北京时间 YYYY-MM-DD
END_DATE = "2026-09-17"
SYMBOLS: List[str] = []  # 如 ["BTC-USDT","ETH-USDT"]；空=全部
SAVE_CSV = ""  # 如 "spot_deals.csv"；空=不落盘
PRINT_LIMIT = 200  # 控制台最多打印行数；0=全打
# ==========================

_BJT = datetime.timezone(datetime.timedelta(hours=8))


def _bjt_date_range_to_ts(begin_date: str, end_date: str):
    lo = datetime.datetime.strptime(begin_date, "%Y-%m-%d").replace(tzinfo=_BJT)
    hi = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=_BJT
    )
    return int(lo.timestamp()), int(hi.timestamp())


def _ts_bjt(ts: int) -> str:
    return (
        datetime.datetime.utcfromtimestamp(int(ts)) + datetime.timedelta(hours=8)
    ).strftime("%Y-%m-%d %H:%M:%S")


def _uid_terms(uids: List[str]) -> list:
    terms = []
    seen = set()
    for u in uids:
        s = str(u)
        cands = [s]
        if s.isdigit():
            cands.append(int(s))
        for x in cands:
            if x in seen:
                continue
            seen.add(x)
            terms.append(x)
    return terms


def _user_view(deal: dict, uid: str) -> Optional[dict]:
    """从指定用户视角解析一笔现货成交；用户不在成交中则返回 None。"""
    from_u, to_u = str(deal.get("fromUser")), str(deal.get("toUser"))
    deal_type = str(deal.get("type") or "").lower()  # fromUser 方向
    if uid == from_u:
        side = deal_type
        fee = float(deal.get("fromFee") or 0)
        order_id = deal.get("fromOrder")
        role = "from"
        counterparty = to_u
    elif uid == to_u:
        side = "sell" if deal_type == "buy" else "buy" if deal_type == "sell" else deal_type
        fee = float(deal.get("toFee") or 0)
        order_id = deal.get("toOrder")
        role = "to"
        counterparty = from_u
    else:
        return None

    price = float(deal.get("price") or 0)
    amount = float(deal.get("amount") or 0)
    return {
        "userid": uid,
        "tsText": _ts_bjt(deal["ts"]),
        "symbol": deal.get("symbol"),
        "side": side,
        "price": price,
        "amount": amount,
        "quote": price * amount,
        "fee": fee,
        "role": role,
        "order_id": order_id,
        "counterparty": counterparty,
        "deal_type": deal_type,
        "ts": int(deal["ts"]),
        "_id": str(deal.get("_id")),
    }


async def fetch_deals() -> pd.DataFrame:
    ts_min, ts_max = _bjt_date_range_to_ts(BEGIN_DATE, END_DATE)
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_deal

    ts_rng = {"$gte": ts_min, "$lte": ts_max}
    query: dict = {"ts": ts_rng}
    if SYMBOLS:
        query["symbol"] = {"$in": SYMBOLS}

    uids = [str(u) for u in USER_IDS]
    if uids:
        terms = _uid_terms(uids)
        query = {
            "$or": [
                {"fromUser": {"$in": terms}, "ts": ts_rng},
                {"toUser": {"$in": terms}, "ts": ts_rng},
            ]
        }
        if SYMBOLS:
            # 每个分支带上 symbol，便于索引
            query = {
                "$or": [
                    {"fromUser": {"$in": terms}, "ts": ts_rng, "symbol": {"$in": SYMBOLS}},
                    {"toUser": {"$in": terms}, "ts": ts_rng, "symbol": {"$in": SYMBOLS}},
                ]
            }

    rows = []
    cursor = col.find(query).sort("ts", 1)
    async for deal in cursor:
        if uids:
            for uid in uids:
                row = _user_view(deal, uid)
                if row:
                    rows.append(row)
        else:
            # 无用户过滤：各输出 from/to 两条用户视角
            for uid in (str(deal.get("fromUser")), str(deal.get("toUser"))):
                row = _user_view(deal, uid)
                if row:
                    rows.append(row)

    return pd.DataFrame(rows)


async def main():
    print(
        f"查询 real_deal 区间={BEGIN_DATE}~{END_DATE}(BJT) "
        f"users={USER_IDS or 'ALL'} symbols={SYMBOLS or 'ALL'}"
    )
    df = await fetch_deals()
    if df.empty:
        print("无成交数据")
        return

    cols = [
        "tsText", "userid", "symbol", "side", "price", "amount", "quote",
        "fee", "role", "order_id", "counterparty",
    ]
    show = df[cols]
    n = len(show)
    limit = PRINT_LIMIT if PRINT_LIMIT and PRINT_LIMIT > 0 else n
    print(show.head(limit).to_string(index=False))
    if n > limit:
        print(f"... 共 {n} 行，仅打印前 {limit} 行")

    print(
        f"合计笔数={n} 成交额={df['quote'].sum():.4f} "
        f"手续费={df['fee'].sum():.6f} "
        f"买={int((df['side']=='buy').sum())} 卖={int((df['side']=='sell').sum())}"
    )
    if SAVE_CSV:
        df.to_csv(SAVE_CSV, index=False, encoding="utf-8-sig")
        print(f"已保存 {SAVE_CSV}")


if __name__ == "__main__":
    asyncio.run(main())

