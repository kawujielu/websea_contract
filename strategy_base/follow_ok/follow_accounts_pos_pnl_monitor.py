"""跟单账户持仓+盈亏监控：内盘持仓 / 带单员持仓 / 带单员权益 / 权益盈亏，每2小时发 TG。"""
from __future__ import annotations

import asyncio
import sys
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, "/home/ubuntu/strategy_base")
sys.path.append("../..")
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from okx_public_current_subpositions import get_public_current_subpositions
from okx_public_stats import get_public_stats

# ========== 参数 ==========
TEL_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
CHAT_ID = -5149924609
INTERVAL_SEC = 2 * 3600  # 每2小时
RETRY = 5
RETRY_SLEEP = 1
BJT = ZoneInfo("Asia/Shanghai")
# 带单员持仓只展示这些币（OKX instId 如 BTC-USDT-SWAP）
LEAD_POS_BASES = frozenset({"BTC", "ETH", "SOL", "BNB"})

# init 为初始权益，请手动填写；盈亏 = 当前权益 - init
ACCOUNTS = [
    {
        "name": "quant_sky_004(mainstream_win)",
        "token": "de750dbc0df94e10f7263811d9i58536555",
        "secret": "aod4rl3vofolhaauwxtk",
        "lead_id": "9D336194ACB74891",
        "init": 10000,  # TODO 手动填初始权益
    },
    {
        "name": "quant_sky_002(mainstream_win2)",
        "token": "11098fbe8c915f2e4549433ff6d55748600",
        "secret": "ztb56om1q82qxbj22z64",
        "lead_id": "741867347484730662",
        "init": 27851.7,  # TODO 手动填初始权益
    },
    {
        "name": "quant_sky_003(mainstream_win3)",
        "token": "c1e296a532fedadb020cffe96cj59093622",
        "secret": "69ahgpifg15l5euwmetp",
        "lead_id": "F40F5A70BC7DE41E",
        "init": 20367,  # TODO 手动填初始权益
    },
    {
        "name": "quant_sky_005(mainstream_win4)",
        "token": "ab3c66061669a2729efffbf677z68014146",
        "secret": "k7866kfh9mi54yqymimr",
        "lead_id": "460047947CE774E1",
        "init": 10000,  # TODO 手动填初始权益
    },
]
# ==========================


def _equity(balance) -> float:
    for k in ("equity", "balance", "total", "avail"):
        if hasattr(balance, k) and getattr(balance, k) is not None:
            return float(getattr(balance, k))
    return 0.0


def retry_sync(fn, *args, **kwargs):
    """失败 sleep(1) 重试，最多 RETRY 次；全失败返回 None。"""
    last = None
    for i in range(RETRY):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last = e
            print("接口失败({}/{}): {} {}".format(i + 1, RETRY, fn.__name__, e))
            time.sleep(RETRY_SLEEP)
    return None


async def retry_async(coro_fn, *args, **kwargs):
    """异步接口重试。"""
    last = None
    for i in range(RETRY):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            last = e
            name = getattr(coro_fn, "__name__", str(coro_fn))
            print("接口失败({}/{}): {} {}".format(i + 1, RETRY, name, e))
            await asyncio.sleep(RETRY_SLEEP)
    return None


def send_tg(text: str) -> None:
    url = "https://api.telegram.org/bot{}/sendMessage".format(TEL_TOKEN)
    chunk, buf = [], ""
    for line in text.splitlines(True):
        if len(buf) + len(line) > 3500:
            chunk.append(buf)
            buf = line
        else:
            buf += line
    if buf:
        chunk.append(buf)
    for c in chunk:
        def _post():
            r = requests.post(url, data={"chat_id": CHAT_ID, "text": c}, timeout=30)
            r.raise_for_status()
            return r
        if retry_sync(_post) is None:
            print("TG 发送跳过")
        time.sleep(0.3)


def fmt_lead_pos(lead_id: str) -> str:
    data = retry_sync(get_public_current_subpositions, lead_id)
    if data is None:
        return "带单员持仓: 查询失败已跳过"
    rows = data.get("data") or []
    if not rows:
        return "带单员持仓: 无"
    # 按 instId 合并：subPos/upl 求和，开仓价按仓位加权；仅 BTC/ETH/SOL/BNB
    merged = {}
    kept = 0
    for r in rows:
        inst = r.get("instId") or ""
        if not inst:
            continue
        base = inst.split("-", 1)[0].upper()
        if base not in LEAD_POS_BASES:
            continue
        kept += 1
        pos = float(r.get("subPos") or 0)
        upl = float(r.get("upl") or 0)
        open_px = float(r.get("openAvgPx") or 0)
        m = merged.get(inst)
        if not m:
            merged[inst] = {
                "pos": pos, "upl": upl, "open_w": open_px * abs(pos),
                "abs_pos": abs(pos), "side": r.get("posSide"), "mark": r.get("markPx"),
            }
        else:
            m["pos"] += pos
            m["upl"] += upl
            m["open_w"] += open_px * abs(pos)
            m["abs_pos"] += abs(pos)
            m["mark"] = r.get("markPx") or m["mark"]
    if not merged:
        return "带单员持仓(lead={}): 无(仅展示BTC/ETH/SOL/BNB)".format(lead_id)
    lines = [
        "带单员持仓(lead={}) 共{}笔→合并{}个币对(仅BTC/ETH/SOL/BNB):".format(
            lead_id, kept, len(merged)
        )
    ]
    for inst, m in merged.items():
        open_avg = (m["open_w"] / m["abs_pos"]) if m["abs_pos"] else 0
        side = "long" if m["pos"] > 0 else ("short" if m["pos"] < 0 else (m["side"] or "flat"))
        lines.append(
            "  {} side={} subPos={:.4f} open={:.4f} mark={} upl={:.2f}".format(
                inst, side, m["pos"], open_avg, m["mark"], m["upl"],
            )
        )
    return "\n".join(lines)


def fmt_lead_equity(lead_id: str) -> str:
    """带单员公开权益（public-stats.investAmt）。"""
    data = retry_sync(get_public_stats, lead_id)
    if data is None:
        return "带单员权益: 查询失败已跳过"
    row = (data.get("data") or [{}])[0] or {}
    try:
        invest = float(row.get("investAmt") or 0)
    except (TypeError, ValueError):
        return "带单员权益: 解析失败"
    return "带单员权益(investAmt)={:.2f}".format(invest)


async def fmt_ws_account(acc: dict) -> str:
    rest = ws_contract_rest(acc["token"], acc["secret"])
    rest.DEBUG = False
    size_map = {}
    symbols = await retry_async(rest.get_symbols, quan=True)
    if symbols:
        for s in symbols:
            size_map[s.symbol] = s.contract_size

    bal = await retry_async(rest.get_walletList, is_full=2)
    if bal is None:
        return "===== {} =====\n权益/钱包查询失败已跳过".format(acc["name"])
    eq = _equity(bal)
    init = float(acc.get("init") or 0)
    pnl = eq - init
    pct = (pnl / init * 100) if init else 0.0
    lines = [
        "===== {} =====".format(acc["name"]),
        "权益={:.2f}  初始={:.2f}  盈亏={:.2f}({:.2f}%)".format(eq, init, pnl, pct),
    ]
    positions = await retry_async(rest.get_position, is_full=1)
    if positions is None:
        lines.append("内盘持仓: 查询失败已跳过")
    elif not positions:
        lines.append("内盘持仓: 无")
    else:
        lines.append("内盘持仓:")
        for p in positions:
            side = "buy" if p.type == 1 else "sell"
            sz = size_map.get(p.symbol, 1)
            lines.append(
                "  {} {} 张={} 币={} 开仓={} 盈亏={} 杠杆={}".format(
                    p.symbol, side, p.amount, p.amount * sz,
                    p.open_price_avg, p.profit, p.lever_rate,
                )
            )
    lines.append(fmt_lead_pos(acc["lead_id"]))
    lines.append(fmt_lead_equity(acc["lead_id"]))
    return "\n".join(lines)


async def once() -> None:
    now = datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")
    parts = ["【跟单持仓监控】{}".format(now)]
    for acc in ACCOUNTS:
        try:
            parts.append(await fmt_ws_account(acc))
        except Exception:
            parts.append("===== {} 查询失败 =====\n{}".format(acc["name"], traceback.format_exc()))
        await asyncio.sleep(0.3)
    text = "\n\n".join(parts)
    print(text)
    send_tg(text)


async def main():
    # 初次运行立即发送，之后每 INTERVAL_SEC 再发
    while True:
        try:
            await once()
        except Exception:
            print("一轮失败:\n{}".format(traceback.format_exc()))
            try:
                send_tg("跟单持仓监控失败:\n{}".format(traceback.format_exc()[-500:]))
            except Exception:
                pass
        await asyncio.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(main())

