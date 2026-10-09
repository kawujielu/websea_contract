# -*- coding: utf-8 -*-
"""
定时监控内盘指定用户持仓，OKX 对冲账户仅跟随减仓/全平（不加仓）。

规则（每 POLL_SEC 秒）:
  1. 拉内盘 user_id 持仓 + OKX 账户持仓，按交易对各自对比
  2. 仅当监控账户该交易对减仓（|仓|变小）时，OKX 按相同比例减仓
  3. 监控账户加仓：不跟
  4. 监控账户该交易对（或全部）仓位归零：OKX 对应（或全部）市价全平

OKX API Key 写在本文件，不 import get_balance.py。
每 TG_INTERVAL_SEC 秒向 Telegram 推送内外盘持仓（双侧皆空则不发）。

运行:
  python check_user_pos.py
"""
from __future__ import annotations

import asyncio
import math
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # noqa: E402
from crypto_center.client.rest.okex import contract as okx_rest  # noqa: E402

BJT = ZoneInfo("Asia/Shanghai")

# ========== 配置 ==========
USER_ID = "606019"  # 监控的内盘用户
POLL_SEC = 10
PAGE_SIZE = 20
RISK_TOKEN = "c1cf4185b2bed317aeb6e6674491fbef"
RISK_SECRET = ""

# OKX 对冲账户（与 get_balance.py 同账户，参数写死在本脚本）
OKX_API_KEY = "0f075dec-55e8-4677-98a6-438c90ba9417"
OKX_SECRET = "F9D499D65932321247A23CE52ACF7A42"
OKX_PASSPHRASE = "Gf794972."

EXECUTE = False  # True 才真实下单；默认 False 只打印计划
TD_MODE = "cross"
MIN_CLOSE_CONTRACTS = 0.01  # 小于此张数不减仓，避免粉尘
ABS_EPS = 1e-8  # 视为空仓的阈值
FACE_VALUE_U = 0.01  # 展示用

# Telegram（与 check_hedge_pos 同群）
TG_BOT_TOKEN = "6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q"
TG_CHAT_ID = -1002413824899
TG_INTERVAL_SEC = 10 * 60  # 每 10 分钟推送一次
# ==========================


def _now() -> str:
    return datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")


def _pnl(row: dict) -> float:
    v = row.get("profit_loss")
    if v is None:
        v = row.get("profitLoss")
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _signed_inner(row: dict) -> float:
    try:
        amt = float(row.get("amount") or 0)
        od = int(row.get("openDirection") or 0)
    except (TypeError, ValueError):
        return 0.0
    if od == 1:
        return amt
    if od == 2:
        return -amt
    return 0.0


def _norm_symbol(sym: str) -> str:
    """统一成 BTC-USDT 形态，便于内外盘对比。"""
    s = (sym or "").strip().upper()
    if s.endswith("-SWAP"):
        s = s[: -len("-SWAP")]
    return s


async def fetch_inner_holds(rest: Contract, user_id: str) -> List[dict]:
    rows: List[dict] = []
    while True:
        try:
            data = await rest.fetch_hold_list(page_size=PAGE_SIZE, user_id=str(user_id))
            total_page = int(data["pager"]["total_page"])
            rows.extend(data.get("data") or [])
            for n in range(2, total_page + 1):
                data = await rest.fetch_hold_list(
                    page=n, page_size=PAGE_SIZE, user_id=str(user_id)
                )
                rows.extend(data.get("data") or [])
                await asyncio.sleep(0.5)
            break
        except Exception as e:
            print(f"[{_now()}] 内盘拉取失败 user_id={user_id}: {e}")
            await asyncio.sleep(5)
    out: List[dict] = []
    for r in rows:
        if r not in out:
            out.append(r)
    return out


def aggregate_inner(rows: List[dict]) -> Dict[str, float]:
    """symbol -> 净持仓张数（多正空负）。"""
    pos: Dict[str, float] = {}
    for r in rows:
        sym = _norm_symbol(str(r.get("symbol") or ""))
        if not sym:
            continue
        pos[sym] = pos.get(sym, 0.0) + _signed_inner(r)
    return {k: v for k, v in pos.items() if abs(v) > ABS_EPS}


def _okx_signed(p) -> Tuple[str, float, str, str]:
    """
    返回 (symbol, signed_contracts, pos_side_for_api, margin_mode)
    pos_side_for_api: long/short/net
    """
    symbol = _norm_symbol(str(getattr(p, "symbol", "") or ""))
    side = str(getattr(p, "side", "") or "").lower()
    try:
        contracts = float(getattr(p, "contracts", 0) or 0)
    except (TypeError, ValueError):
        contracts = 0.0
    info = getattr(p, "info", None) or {}
    if isinstance(info, dict):
        raw_pos = info.get("pos")
        if raw_pos is not None and side in ("", "net"):
            try:
                contracts = float(raw_pos)
            except (TypeError, ValueError):
                pass
        mgn = info.get("mgnMode") or getattr(p, "marginMode", "") or "cross"
    else:
        mgn = getattr(p, "marginMode", "") or "cross"
    mgn = "isolated" if str(mgn).lower().startswith("iso") else "cross"

    if side == "short":
        signed = -abs(contracts)
        pos_side = "short"
    elif side == "long":
        signed = abs(contracts)
        pos_side = "long"
    else:
        signed = contracts
        pos_side = "net"
    return symbol, signed, pos_side, mgn


async def fetch_okx_positions(okx) -> Dict[str, dict]:
    """
    symbol -> {
      signed, pos_side, margin_mode, abs
    }
    同 symbol 多条（long/short）合并为净仓；全平用 pos_side=net 时再按原侧处理。
    """
    raw = await okx.fetch_position()
    if not isinstance(raw, list):
        print(f"[{_now()}] OKX 持仓查询失败: {raw}")
        return {}

    # 先按 (symbol, pos_side) 存，再合成净仓
    by_side: Dict[Tuple[str, str], dict] = {}
    for p in raw:
        sym, signed, pos_side, mgn = _okx_signed(p)
        if not sym or abs(signed) <= ABS_EPS:
            continue
        key = (sym, pos_side)
        if key in by_side:
            by_side[key]["signed"] += signed
        else:
            by_side[key] = {
                "signed": signed,
                "pos_side": pos_side,
                "margin_mode": mgn,
            }

    # 净仓视图（减仓比例用）+ 保留分侧明细（全平/减仓下单用）
    net: Dict[str, dict] = {}
    for (sym, pos_side), v in by_side.items():
        if sym not in net:
            net[sym] = {
                "signed": 0.0,
                "legs": [],
                "margin_mode": v["margin_mode"],
            }
        net[sym]["signed"] += v["signed"]
        net[sym]["legs"].append(
            {
                "pos_side": pos_side,
                "signed": v["signed"],
                "margin_mode": v["margin_mode"],
            }
        )
    return {k: v for k, v in net.items() if abs(v["signed"]) > ABS_EPS or v["legs"]}


def format_inner(rows: List[dict]) -> str:
    lines = [f"--- 内盘 {USER_ID} [{_now()}] {len(rows)} 笔 ---"]
    if not rows:
        lines.append("（无持仓）")
        return "\n".join(lines)
    for r in rows:
        vol = _signed_inner(r)
        side = "多" if vol > 0 else "空"
        try:
            avg = float(r.get("avgPrice") or 0)
        except (TypeError, ValueError):
            avg = 0.0
        lines.append(
            f"  {_norm_symbol(str(r.get('symbol')))} {side} {vol}张 "
            f"约{abs(vol)*FACE_VALUE_U*avg:.2f}U 成本:{r.get('avgPrice')} 浮:{_pnl(r):.2f}"
        )
    return "\n".join(lines)


def format_okx(okx_map: Dict[str, dict]) -> str:
    lines = [f"--- OKX 对冲持仓 [{_now()}] ---"]
    if not okx_map:
        lines.append("（无持仓）")
        return "\n".join(lines)
    for sym, v in okx_map.items():
        s = v["signed"]
        side = "多" if s > 0 else ("空" if s < 0 else "平")
        lines.append(f"  {sym} {side} 净仓:{s} legs={v.get('legs')}")
    return "\n".join(lines)


def build_tg_message(rows: List[dict], okx_map: Dict[str, dict]) -> str:
    return (
        f"C组大户对冲持仓快照\n"
        f"时间:{_now()} USER_ID={USER_ID}\n"
        f"{format_inner(rows)}\n"
        f"{format_okx(okx_map)}"
    )


def send_tg(text: str) -> None:
    if not text or not TG_BOT_TOKEN or TG_CHAT_ID is None:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    # Telegram 单条约 4096，超长则切片
    chunk = 3500
    parts = [text[i : i + chunk] for i in range(0, len(text), chunk)] or [text]
    for part in parts:
        try:
            r = requests.post(
                url,
                json={"chat_id": TG_CHAT_ID, "text": part},
                timeout=30,
            )
            if r.status_code != 200:
                print(f"[{_now()}] Telegram 发送失败 status={r.status_code} body={r.text[:200]}")
        except Exception:
            print(f"[{_now()}] Telegram 发送异常:\n{traceback.format_exc()}")


def maybe_send_tg(
    rows: List[dict],
    cur_inner: Dict[str, float],
    okx_map: Dict[str, dict],
    last_tg_ts: float,
) -> float:
    """内外盘均无持仓则不发；距上次 >= TG_INTERVAL_SEC 才发。返回更新后的 last_tg_ts。"""
    if not cur_inner and not okx_map:
        return last_tg_ts
    now = time.time()
    # 首次有仓立即发一回，之后每 10 分钟
    if last_tg_ts > 0 and now - last_tg_ts < TG_INTERVAL_SEC:
        return last_tg_ts
    msg = build_tg_message(rows, okx_map)
    print(f"[{_now()}] 推送 Telegram 持仓快照 chat_id={TG_CHAT_ID}")
    send_tg(msg)
    return now


async def okx_reduce(
    okx,
    symbol: str,
    close_signed: float,
    pos_side: str,
    margin_mode: str,
    reason: str,
) -> None:
    """
    市价只减仓。
    close_signed: 与持仓同号，表示要平掉的仓（多仓>0 则卖出；空仓<0 则买入）。
    """
    amt = abs(close_signed)
    if amt < MIN_CLOSE_CONTRACTS:
        print(f"[{_now()}] 跳过粉尘减仓 {symbol} amt={amt}")
        return

    if close_signed > 0:
        side = "sell"  # 平多
    else:
        side = "buy"  # 平空

    kw = {
        "tdMode": TD_MODE if margin_mode == "cross" else "isolated",
        "reduceOnly": True,
    }
    if pos_side in ("long", "short"):
        kw["posSide"] = pos_side

    msg = (
        f"[{_now()}] OKX减仓计划 {reason} {symbol} side={side} sz={amt} "
        f"posSide={kw.get('posSide')} tdMode={kw['tdMode']} EXECUTE={EXECUTE}"
    )
    print(msg)
    if not EXECUTE:
        return
    try:
        res = await okx.create_order(
            symbol=symbol,
            order_type="market",
            side=side,
            amount=amt,
            price=None,
            **kw,
        )
        print(f"[{_now()}] OKX减仓回报 {symbol}: {res}")
    except Exception:
        print(f"[{_now()}] OKX减仓失败 {symbol}:\n{traceback.format_exc()}")


async def okx_close_all_symbol(okx, symbol: str, legs: List[dict], reason: str) -> None:
    """某交易对全平。优先 close_position，失败则按腿市价减仓。"""
    print(f"[{_now()}] OKX全平计划 {reason} {symbol} legs={legs} EXECUTE={EXECUTE}")
    if not EXECUTE:
        return
    # 逐腿全平（兼容 long/short/net）
    for leg in legs:
        pos_side = leg["pos_side"]
        mgn = leg.get("margin_mode") or "cross"
        margin_mode = "isolated" if mgn == "isolated" else "cross"
        try:
            res = await okx.close_position(
                symbol=symbol,
                position_side=pos_side if pos_side in ("long", "short", "net") else "net",
                marginMode=margin_mode,
                autoCxl=True,
            )
            print(f"[{_now()}] OKX close_position {symbol} {pos_side}: {res}")
        except Exception:
            print(f"[{_now()}] close_position 失败，改市价减仓 {symbol}:\n{traceback.format_exc()}")
            signed = float(leg.get("signed") or 0)
            if abs(signed) > ABS_EPS:
                await okx_reduce(okx, symbol, signed, pos_side, margin_mode, reason=reason + "/fallback")


async def handle_reduce_follow(
    okx,
    last_inner: Dict[str, float],
    cur_inner: Dict[str, float],
    okx_map: Dict[str, dict],
) -> None:
    """
    对比上一拍与当前内盘持仓，仅处理减仓/全平，驱动 OKX。
    """
    # 内盘整体空仓 → OKX 全部全平
    if not cur_inner:
        if okx_map:
            print(f"[{_now()}] 内盘已全平，OKX 跟随全平 symbols={list(okx_map)}")
            for sym, v in list(okx_map.items()):
                await okx_close_all_symbol(okx, sym, v.get("legs") or [], reason="内盘全平")
        return

    symbols = set(last_inner) | set(cur_inner) | set(okx_map)
    for sym in sorted(symbols):
        old = float(last_inner.get(sym, 0.0))
        new = float(cur_inner.get(sym, 0.0))
        okx_info = okx_map.get(sym)

        # 无历史或不存在旧仓：无法算减仓比例，跳过（首轮已在外层跳过）
        if abs(old) <= ABS_EPS:
            if abs(new) > ABS_EPS:
                print(f"[{_now()}] {sym} 内盘新开/加仓 old=0 new={new}，不加仓跟随")
            continue

        abs_old, abs_new = abs(old), abs(new)

        # 加仓或同向放大：不跟
        if abs_new > abs_old + ABS_EPS:
            print(f"[{_now()}] {sym} 内盘加仓 {old} -> {new}，不跟随")
            continue

        # 无变化
        if abs(abs_new - abs_old) <= ABS_EPS and math.copysign(1, new or old) == math.copysign(1, old):
            continue

        # 方向反转视为先平旧仓（按全平该对冲腿处理）
        flipped = abs_new > ABS_EPS and ((old > 0 and new < 0) or (old < 0 and new > 0))
        if flipped:
            print(f"[{_now()}] {sym} 内盘方向反转 {old}->{new}，按减仓到0处理 OKX（不加反向）")
            abs_new = 0.0

        if not okx_info or abs(okx_info.get("signed", 0)) <= ABS_EPS:
            print(f"[{_now()}] {sym} 内盘减仓但 OKX 无仓，跳过")
            continue

        # 内盘该币全平
        if abs_new <= ABS_EPS:
            await okx_close_all_symbol(
                okx, sym, okx_info.get("legs") or [], reason=f"内盘{sym}清仓"
            )
            continue

        # 同比例减仓：remain = new/old，OKX 目标 = OKX * remain，平掉差额
        remain = abs_new / abs_old
        if remain >= 1 - 1e-9:
            continue
        okx_signed = float(okx_info["signed"])
        target = okx_signed * remain
        close_signed = okx_signed - target  # 需要平掉的仓（与持仓同号）
        close_amt = abs(close_signed)
        print(
            f"[{_now()}] {sym} 内盘减仓 {old}->{new} remain={remain:.6f} "
            f"OKX {okx_signed}->{target} 平掉{close_amt}"
        )

        # 按腿分摊（net 单腿直接平；hedge 模式按净仓方向选对应腿）
        legs = okx_info.get("legs") or []
        if len(legs) == 1:
            leg = legs[0]
            await okx_reduce(
                okx,
                sym,
                math.copysign(close_amt, leg["signed"]),
                leg["pos_side"],
                leg.get("margin_mode") or "cross",
                reason=f"比例减仓 remain={remain:.4f}",
            )
        else:
            # 多腿：优先减与净仓同向的腿
            same = [lg for lg in legs if lg["signed"] * okx_signed > 0]
            use = same[0] if same else legs[0]
            await okx_reduce(
                okx,
                sym,
                math.copysign(close_amt, use["signed"]),
                use["pos_side"],
                use.get("margin_mode") or "cross",
                reason=f"比例减仓 remain={remain:.4f}",
            )


async def poll_loop() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    inner_rest = Contract(RISK_TOKEN, RISK_SECRET, dev=False)
    inner_rest.DEBUG = False
    inner_rest.rest_timeout = 120

    okx = okx_rest.OkexContract(OKX_API_KEY, OKX_SECRET, OKX_PASSPHRASE)
    okx.DEBUG = False

    last_inner: Optional[Dict[str, float]] = None
    last_tg_ts = 0.0
    print(
        f"[{_now()}] 启动 check_user_pos USER_ID={USER_ID} POLL={POLL_SEC}s "
        f"EXECUTE={EXECUTE} TG每{TG_INTERVAL_SEC}s（仅跟随内盘减仓/全平）"
    )

    while True:
        try:
            rows = await fetch_inner_holds(inner_rest, USER_ID)
            cur_inner = aggregate_inner(rows)
            okx_map = await fetch_okx_positions(okx)

            print(format_inner(rows))
            print(format_okx(okx_map))

            last_tg_ts = maybe_send_tg(rows, cur_inner, okx_map, last_tg_ts)

            if last_inner is None:
                print(f"[{_now()}] 首轮仅记录内盘基准，不交易: {cur_inner}")
            else:
                await handle_reduce_follow(okx, last_inner, cur_inner, okx_map)

            last_inner = dict(cur_inner)

            # 内盘已无持仓：处理完本轮（含 OKX 全平）后停止轮询
            if not cur_inner:
                print(f"[{_now()}] 内盘用户 {USER_ID} 已无持仓，停止定时查询内外盘")
                break
        except Exception:
            print(f"[{_now()}] 主循环异常:\n{traceback.format_exc()}")
        await asyncio.sleep(POLL_SEC)


def main() -> None:
    asyncio.run(poll_loop())


if __name__ == "__main__":
    main()

