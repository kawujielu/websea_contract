import sys
import asyncio
from pathlib import Path

sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from crypto_center.client.rest.okex import contract as okx_rest
from crypto_center.object.types_apis import Entry

# 现货(币币)划转到永续合约的 USDT 数量，0 表示不划转
TRANSFER_AMOUNT = 0


def build_ok_rest():
    return okx_rest.OkexContract(
        '0f075dec-55e8-4677-98a6-438c90ba9417',
        'F9D499D65932321247A23CE52ACF7A42',
        'Gf794972.',
    )


async def transfer_spot_to_contract(ok_rest, amount=None):
    """资金从现货(币币账户)划转到永续合约。amount 默认用全局变量 TRANSFER_AMOUNT。"""
    if amount is None:
        amount = TRANSFER_AMOUNT
    if not amount or amount <= 0:
        print(f"划转金额为 {amount}，跳过划转")
        return
    entry = Entry("POST", "/api/v5/asset/transfer", sign=True)
    data = {
        "ccy": "USDT",
        "amt": str(amount),
        "from": "1",   # 币币/现货
        "to": "9",     # 永续合约
        "type": "0",   # 账户内划转
    }
    status_code, resp, _ = await ok_rest.send_request_entry(ok_rest.url, entry, data=data)
    print(f"现货划转合约 {amount} USDT, status={status_code}, resp={resp}")
    return resp


async def query_balance(ok_rest=None):
    if ok_rest is None:
        ok_rest = build_ok_rest()
        ok_rest.DEBUG = False
    res = await ok_rest.fetch_balance()
    if not isinstance(res, dict):
        print(f"查询余额失败: {res}")
        return

    print("OK账户余额:")
    shown = 0
    for ccy, bal in res.items():
        if not isinstance(bal, dict):
            continue
        total = bal.get('total', 0) or 0
        free = bal.get('free', 0) or 0
        used = bal.get('used', 0) or 0
        if total == 0 and free == 0 and used == 0:
            continue
        print(f"{ccy} 权益:{total} 可用:{free} 冻结:{used}")
        shown += 1
    if shown == 0:
        print("账户无非零余额")


async def query_position(ok_rest=None):
    """查询当前永续持仓。"""
    if ok_rest is None:
        ok_rest = build_ok_rest()
        ok_rest.DEBUG = False
    res = await ok_rest.fetch_position()
    if res is None or (isinstance(res, dict) and res.get("code") not in (None, "0", 0)):
        # fetch_position 失败时可能返回错误对象/字典
        if not isinstance(res, list):
            print(f"查询持仓失败: {res}")
            return

    print("OK当前持仓:")
    if not res:
        print("无持仓")
        return

    shown = 0
    for p in res:
        try:
            contracts = float(getattr(p, "contracts", 0) or 0)
        except (TypeError, ValueError):
            contracts = 0.0
        if contracts == 0:
            continue
        symbol = getattr(p, "symbol", "-")
        side = getattr(p, "side", "-")
        entry = getattr(p, "entryPrice", None)
        mark = getattr(p, "markPrice", None)
        upl = getattr(p, "unrealizedPnl", None)
        lev = getattr(p, "leverage", None)
        mgn = getattr(p, "marginMode", None)
        print(
            f"{symbol} 方向:{side} 张数:{contracts} "
            f"开仓价:{entry} 标记价:{mark} 浮盈:{upl} "
            f"杠杆:{lev} 保证金:{mgn}"
        )
        shown += 1
    if shown == 0:
        print("无持仓")


async def run():
    ok_rest = build_ok_rest()
    ok_rest.DEBUG = False
    await query_balance(ok_rest)
    await query_position(ok_rest)
    await transfer_spot_to_contract(ok_rest)


def main():
    asyncio.run(run())


if __name__ == '__main__':
    main()

