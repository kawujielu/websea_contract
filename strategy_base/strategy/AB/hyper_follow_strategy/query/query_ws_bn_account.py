#!/usr/bin/env python3
"""查询 Websea / Binance 合约权益与持仓。用法: python query/query_ws_bn_account.py"""

import asyncio

# ---------- 密钥 ----------
WEBSEA_API_KEY = "23cb167617bb8398ccfcf74506t64669304"
WEBSEA_API_SECRET = "9mowefaye9nhyxf2w7zq"
BINANCE_API_KEY = "qBY47YZYAP6ZT4BQdOAqf7uyLUSOctSJ3OWeE3qFXBMQSrnNAHyP74tknFePsQFO"
BINANCE_API_SECRET = "9oKFd6iM3YObfPE3xrlDHIMIiMMOum4Z9M2ZPYhLRXxJ2wSqltqbsfmg7jNbI61i"


async def query_websea():
    from crypto_center.client.rest.websea import contract_quan as ws

    rest = ws.WebseaContract(WEBSEA_API_KEY, WEBSEA_API_SECRET)
    rest.DEBUG = False
    bal, pos_resp = await asyncio.gather(
        rest.fetch_balance_normal(is_full=2),
        rest.fetch_position_normal(),
    )
    result = (bal or {}).get("result") if isinstance(bal, dict) else bal
    wallet = result[0] if isinstance(result, list) and result else (result or {})
    equity = (
        float(wallet.get("avail") or 0)
        + float(wallet.get("hold") or 0)
        + float(wallet.get("unrealizedPL") or 0)
    )
    print(f"[Websea] 权益={equity:.4f} avail={wallet.get('avail')} hold={wallet.get('hold')}")
    positions = (pos_resp or {}).get("result") if isinstance(pos_resp, dict) else pos_resp
    for p in positions or []:
        side = "多" if int(p.get("type") or 0) == 1 else "空"
        print(
            f"  {p.get('symbol')} {side} amt={p.get('amount')} entry={p.get('open_price_avg')} "
            f"upnl={p.get('un_profit')} lev={p.get('lever_rate')}"
        )
    if not positions:
        print("  (无持仓)")


async def query_binance():
    from crypto_center.client.rest.binance.u_contract import UBinanceContract

    rest = UBinanceContract(BINANCE_API_KEY, BINANCE_API_SECRET)
    rest.DEBUG = False
    bal, positions = await asyncio.gather(
        rest.fetch_balance("USDT"),
        rest.fetch_position(),
    )
    usdt = (bal or {}).get("USDT") or {}
    equity = float(usdt["total"] if isinstance(usdt, dict) else getattr(usdt, "total", 0) or 0)
    free = float(usdt["free"] if isinstance(usdt, dict) else getattr(usdt, "free", 0) or 0)
    print(f"[Binance] 权益={equity:.4f} available={free:.4f}")
    for p in positions or []:
        print(
            f"  {p['symbol']} side={p['side']} sz={p['contracts']} "
            f"entry={p['entryPrice']} upnl={p['unrealizedPnl']} lev={p['leverage']}"
        )
    if not positions:
        print("  (无持仓)")


async def main():
    await query_websea()
    await query_binance()


if __name__ == "__main__":
    asyncio.run(main())
