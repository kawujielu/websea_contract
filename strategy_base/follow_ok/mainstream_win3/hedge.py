"""查询 Websea 权益/持仓、OKX 带单员公开持仓；可选内盘下单（读对应目录下 config）。"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import traceback
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append("../../..")
sys.path.insert(0, "/home/ubuntu/strategy_base")
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
import objects.contract_request.websea as ocw
from okx_public_current_subpositions import get_public_current_subpositions

# ========== 参数 ==========
# 账户目录：读取其下 CONFIG（切换账户只改这两项）
ACCOUNT_DIR = os.path.join(os.path.dirname(__file__), "..", "mainstream_win3")
CONFIG = "ok_follow_deal_config.py"  # loss 目录用 re_ok_follow_deal_config.py

ENABLE_ORDER = True       # True 才下单
ORDER_SYMBOL = "BTC-USDT"
ORDER_SIDE = "buy"         # buy / sell
ORDER_AMOUNT = 60           # 张
ORDER_CLOSE = True        # True=平仓
# ==========================


def load_config():
    fp = os.path.abspath(os.path.join(ACCOUNT_DIR, CONFIG))
    if not os.path.isfile(fp):
        raise FileNotFoundError("找不到配置: {}".format(fp))
    spec = importlib.util.spec_from_file_location("ok_follow_cfg", fp)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _equity(bal) -> float:
    for k in ("equity", "balance", "total", "avail"):
        if hasattr(bal, k) and getattr(bal, k) is not None:
            return float(getattr(bal, k))
    return 0.0


async def order_create(rest, symbol, side, amount, close=False, slip=0.002):
    od_type = ocw.OrderType.buy_market if side == "buy" else ocw.OrderType.sell_market
    depth = await rest.get_depth(symbol, quan=True)
    price = depth.asks[0].price * (1 + slip) if side == "buy" else depth.bids[0].price * (1 - slip)
    precision = await rest.get_precision(symbol, quan=True)
    kw = dict(symbol=symbol, od_type=od_type, price=price, amount=abs(amount), precision=precision)
    if close:
        kw["contract_type"] = "close"
    res = await rest.order_create(**kw)
    print("下单 {} {} 张={} 价={} close={} 回报={}".format(symbol, side, amount, price, close, res))
    return res


async def main() -> None:
    cfg = load_config()
    acc = cfg.config
    name = getattr(cfg, "strategy_name", CONFIG)
    slip = getattr(cfg, "slip", 0.002)
    rest = ws_contract_rest(acc["base_token"], acc["base_secret"])
    rest.DEBUG = False
    lead_id = getattr(cfg, "lead_id", "")

    size_map = {}
    try:
        for s in await rest.get_symbols(quan=True) or []:
            size_map[s.symbol] = s.contract_size
    except Exception:
        print("合约单位查询失败:\n{}".format(traceback.format_exc()))

    # 内盘权益 / 持仓
    bal = await rest.get_walletList(is_full=2)
    eq = _equity(bal)
    avail = float(getattr(bal, "avail", 0) or 0)
    print("===== 内盘 {} =====".format(name))
    print("权益={:.4f}  可用={:.4f}".format(eq, avail))
    positions = await rest.get_position(is_full=1) or []
    if not positions:
        print("持仓: 无")
    else:
        print("持仓:")
        for p in positions:
            side = "buy" if p.type == 1 else "sell"
            sz = size_map.get(p.symbol, 1)
            print(
                "  {} {} 张={} 币={} 开仓={} 盈亏={} 杠杆={} 爆仓={}".format(
                    p.symbol, side, p.amount, p.amount * sz,
                    p.open_price_avg, p.profit, p.lever_rate, p.liquidation_price,
                )
            )

    # 外盘 OKX 持仓（带单员公开持仓，同 follow_ok_copy_to_ws）
    print("===== 外盘 OKX lead_id={} =====".format(lead_id))
    try:
        rows = get_public_current_subpositions(lead_id).get("data") or []
        if not rows:
            print("持仓: 无")
        else:
            print("持仓共{}笔:".format(len(rows)))
            for i in rows:
                open_time = datetime.fromtimestamp(int(i["openTime"]) / 1000).strftime("%Y-%m-%d %H:%M:%S")
                print(
                    "  {} 持仓={} 方向={} 开仓价={} 标记价={} 盈亏={} 杠杆={} 开仓时间={}".format(
                        i["instId"], i["subPos"], i["posSide"],
                        round(float(i["openAvgPx"] or 0), 4),
                        round(float(i["markPx"] or 0), 2),
                        round(float(i["upl"] or 0), 2),
                        i["lever"], open_time,
                    )
                )
    except Exception:
        print("外盘持仓查询失败:\n{}".format(traceback.format_exc()))

    # 内盘下单
    if ENABLE_ORDER:
        try:
            await order_create(rest, ORDER_SYMBOL, ORDER_SIDE, ORDER_AMOUNT, close=ORDER_CLOSE, slip=slip)
        except Exception:
            print("下单失败:\n{}".format(traceback.format_exc()))
    else:
        print("下单: 未启用 (ENABLE_ORDER=False)")


if __name__ == "__main__":
    asyncio.run(main())

