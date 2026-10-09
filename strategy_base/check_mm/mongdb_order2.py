"""
指定时间段内，亏损最多的 100 笔平仓记录（平多/平空）。
"""
import asyncio
import datetime
import heapq

import motor.motor_asyncio

_BJT = datetime.timezone(datetime.timedelta(hours=8))
SIDE = {"1": "开多", "2": "开空", "3": "平多", "4": "平空"}
CLOSE = {"3", "4"}
TOP_N = 100

BEGIN_DATE = "2026-07-15 21:00:00"  # 北京时间，精确到秒
END_DATE = "2026-07-15 21:20:00"
MONGO_URI = "mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/"


def _bjt_range(begin: str, end: str):
    lo = datetime.datetime.strptime(begin, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_BJT)
    hi = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_BJT)
    return int(lo.timestamp()), int(hi.timestamp())


def _ts_bjt(ts: int) -> str:
    return (datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")


def _pick(deal, role: str):
    if role == "taker":
        return (
            str(deal["takerUser"]),
            str(deal["takerBuyOrSell"]),
            float(deal["takerProfitLoss"]),
            float(deal["takerFee"]),
            float(deal["takerFaceValue"]),
        )
    return (
        str(deal["makerUser"]),
        str(deal["makerBuyOrSell"]),
        float(deal["makerProfitLoss"]),
        float(deal["makerFee"]),
        float(deal["takerFaceValue"]),
    )


async def main():
    ts_min, ts_max = _bjt_range(BEGIN_DATE, END_DATE)
    client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    col = client.exchange.real_contract_deal

    # 用最大堆保留亏损最深的 TOP_N（profit 越小越亏）
    heap = []  # (-profit, seq, row) 取反后小顶堆等价于最大亏损
    seq = 0

    cursor = col.find({"ts": {"$gte": ts_min, "$lte": ts_max}}).sort("ts", 1)
    async for deal in cursor:
        price = float(deal["price"])
        amount = float(deal["amount"])
        for role in ("taker", "maker"):
            uid, side, profit, fee, face = _pick(deal, role)
            if side not in CLOSE or not uid or uid in ("0", "None"):
                continue
            if profit >= 0:
                continue
            row = (
                profit,
                _ts_bjt(deal["ts"]),
                uid,
                deal["symbol"],
                SIDE[side],
                price,
                amount * face,
                price * amount * face,
                fee,
            )
            seq += 1
            item = (-profit, seq, row)  # -profit 越小 = 亏损越大，堆顶是亏损最小的
            if len(heap) < TOP_N:
                heapq.heappush(heap, item)
            elif item[0] > heap[0][0]:
                heapq.heapreplace(heap, item)

    rows = [x[2] for x in sorted(heap, key=lambda x: x[2][0])]  # profit 升序：亏最多在前
    print(f"时间范围(北京时间): [{BEGIN_DATE}, {END_DATE}]  亏损平仓 TOP{TOP_N}（共命中 {len(rows)} 笔）")
    print(f"{'盈亏':>12} {'时间':<20} {'user':<12} {'币对':<12} {'方向':<4} {'价格':>12} {'数量':>12} {'金额':>14} {'手续费':>10}")
    for profit, t, uid, sym, side, px, qty, notion, fee in rows:
        print(
            f"{profit:>12.2f} {t:<20} {uid:<12} {sym:<12} {side:<4} "
            f"{px:>12.6f} {qty:>12.4f} {notion:>14.2f} {fee:>10.4f}"
        )


if __name__ == "__main__":
    asyncio.run(main())

