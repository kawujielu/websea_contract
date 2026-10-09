"""查询 Websea 账户权益与持仓；可选市价全平（参数均写在本脚本内）。"""
import sys
import asyncio
import traceback

sys.path.insert(0, "/home/ubuntu/strategy_base")
sys.path.append("../..")
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
import objects.contract_request.websea as ocw

# ========== 参数 ==========
CLOSE_ALL = False  # True=查询后市价全平当前持仓；False=仅查询

# uid / 跟单号 / 昵称 / token / secret / 初始资金
ACCOUNTS = [
    # {"uid": "39271012", "follow_id": "555555", "name": "James", "token": "d6fc035b4e4319319743b70cbdk59445990", "secret": "mwo2m8xyxappj07xr2d7", "init": 5000},
    # {"uid": "51303608", "follow_id": "555559", "name": "one more", "token": "41d5ce0235d482aab2e60a779dl60001992", "secret": "pigm0st0p01j6x9esb5o", "init": 5000},
    {"uid": "37286252", "follow_id": "557412", "name": "eric_deal", "token": "c05f95b1c43bde42556b6e34e1y67448667", "secret": "fikpc1jlgqb05b3yq9r9", "init": 3000},
    {"uid": "16053185", "follow_id": "557471", "name": "要想富满仓隔夜是条路", "token": "7287274648396dc3f797c553ecd55748600", "secret": "fwz3tuyik4nvs60v7i8v", "init": 4000},
    {"uid": "69047987", "follow_id": "557472", "name": "404alive", "token": "9294e3098ff151a5aa854926b6h57978648", "secret": "ye31wgeixol2wumb6iuv", "init": 5000},
    {"uid": "43444608", "follow_id": "557476", "name": "五条悟虚式【茈】", "token": "19f991d6d7cdaac1083264c933j59094046", "secret": "c4029mguoouis2ph0yox", "init": 6000},
    {"uid": "87140264", "follow_id": "557478", "name": "labubugogogo", "token": "fcd8cd9ea07b0a7f0c85b12fa3c55191807", "secret": "ocpvq2nh50mzr2krzm5i", "init": 8000},
    {"uid": "97644531", "follow_id": "557479", "name": "zero量化", "token": "23cb167617bb8398ccfcf74506t64669304", "secret": "9mowefaye9nhyxf2w7zq", "init": 10000},
    {"uid": "85600496", "follow_id": "557501", "name": "酉时三刻", "token": "53dab96a47bd1021d9dcef3d9fk59654212", "secret": "p56gp29wyz5dybrbu430", "init": 15000},
]
# ==========================


def _equity(balance) -> float:
    for k in ("equity", "balance", "total", "avail"):
        if hasattr(balance, k) and getattr(balance, k) is not None:
            return float(getattr(balance, k))
    return 0.0


async def close_positions(rest, positions, name: str) -> list:
    """市价全平：多仓卖出、空仓买入，contract_type=close。"""
    lines = []
    for p in positions:
        side = "buy" if p.type == 1 else "sell"
        od_type = ocw.OrderType.sell_market if side == "buy" else ocw.OrderType.buy_market
        try:
            precision = await rest.get_precision(p.symbol, quan=True)
            price = (await rest.get_index(p.symbol)).price
            res = await rest.order_create(
                symbol=p.symbol, od_type=od_type, price=price,
                amount=abs(p.amount), precision=precision, contract_type="close",
            )
            lines.append(f"全平成功 {p.symbol} 原方向={side} 张数={p.amount} 价格={price} 回报={res}")
        except Exception:
            lines.append(f"全平失败 {p.symbol} 原方向={side} 张数={p.amount}\n{traceback.format_exc()}")
        await asyncio.sleep(0.2)
    if not lines:
        lines.append("无需全平")
    return lines


async def query_one(acc: dict):
    rest = ws_contract_rest(acc["token"], acc["secret"])
    rest.DEBUG = False
    size_map = {}
    try:
        for s in await rest.get_symbols(quan=True):
            size_map[s.symbol] = s.contract_size
    except Exception:
        pass

    balance = await rest.get_walletList(is_full=2)
    eq = _equity(balance)
    avail = float(getattr(balance, "avail", 0) or 0)
    init = acc.get("init", 0)
    pnl = eq - init
    pnl_pct = (pnl / init * 100) if init else 0
    lines = [
        f"===== {acc['name']} (uid={acc.get('uid')} follow={acc.get('follow_id')}) =====",
        f"权益={eq:.4f}  可用={avail:.4f}  初始={init}  盈亏={pnl:.4f}({pnl_pct:.2f}%)",
    ]
    positions = await rest.get_position(is_full=1) or []
    if not positions:
        lines.append("持仓: 无")
    else:
        for p in positions:
            side = "buy" if p.type == 1 else "sell"
            size = size_map.get(p.symbol, 1)
            lines.append(
                f"{p.symbol} 方向={side} 张数={p.amount} 币数={p.amount*size} "
                f"开仓价={p.open_price_avg} 盈亏={p.profit} 杠杆={p.lever_rate} 爆仓价={p.liquidation_price}"
            )
        if CLOSE_ALL:
            lines.append("--- 开始全平 ---")
            lines.extend(await close_positions(rest, positions, acc["name"]))
    return "\n".join(lines), eq


async def main():
    if CLOSE_ALL:
        print("!!! CLOSE_ALL=True，将对各账户当前持仓市价全平 !!!\n")
    total_eq = 0.0
    total_init = 0.0
    for acc in ACCOUNTS:
        try:
            msg, eq = await query_one(acc)
            print(msg + "\n")
            total_eq += eq
            total_init += acc.get("init", 0)
        except Exception:
            print(f"===== {acc['name']} 查询失败 =====\n{traceback.format_exc()}\n")
        await asyncio.sleep(0.2)
    pnl = total_eq - total_init
    print(f"======= 合计权益 ≈ {total_eq:.4f}  初始={total_init:.0f}  盈亏={pnl:.4f} =======")


if __name__ == "__main__":
    asyncio.run(main())

