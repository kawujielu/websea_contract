#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AB仓策略 Websea 合约实盘（BTC-USDT，BBO+0.2%滑点 + 成交滑点统计）。

与 run_live_websea_btc.py 区别：下单价=BBO±0.2%滑点；统计每笔成交价相对 BBO 的滑点，
每 10 笔输出累计平均/最大滑点。

运行（strategy_base 环境）:
    python run_live_websea_btc_slip.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

# ---------- 策略参数（写死） ----------
SYMBOL = "BTC-USDT"
REDIS_DB = 1
ORDER_AMOUNT = 0.001
LEVERAGE = 5
MARGIN_MODE = "crossed"  # crossed | isolated
# env_pro is_full: 1=逐仓 2=全仓
MARGIN_MODE_API = 2 if MARGIN_MODE == "crossed" else 1
ORDER_SLIP = 0.002  # 下单相对 BBO 一档滑点 0.2%
SLIP_STAT_BATCH = 10
ORDER_QUERY_DELAY_SEC = 0.3
API_RETRY_TIMES = 5
API_RETRY_INTERVAL_SEC = 1.0
POSITION_SYNC_INTERVAL_SEC = 60
OPEN_COOLDOWN_SEC = 60  # 距上次开仓至少间隔
STARTUP_CLOSE_SLIP = 0.005  # 启动平仓滑点 0.5%

CHANNEL_TRADE = f"contract.trade.{SYMBOL}.binance"
CHANNEL_BBO = f"contract.bids_asks.{SYMBOL}.websea"

API_KEY = "4905669b7f070117807133b5e3j59096908"
API_SECRET = "jyulvaphe2hnrsbvatj1"

# ---------- 路径 ----------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_STRATEGY_BASE_ROOT: Optional[str] = None


def _add_sys_path(path: str) -> None:
    if path and os.path.isdir(path) and path not in sys.path:
        sys.path.append(path)


def _find_root_upward(start: str, marker: str) -> Optional[str]:
    """从 start 向上查找包含 marker 文件的目录。"""
    cur = os.path.abspath(start)
    while True:
        if os.path.isfile(os.path.join(cur, marker)):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return None
        cur = parent


def _resolve_strategy_base_root() -> Optional[str]:
    """strategy_base 根目录（含 utils/aio_redis.py）。"""
    candidates = [
        # 服务器: /home/ubuntu/strategy_base/strategy/AB -> ../../
        os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..")),
        _find_root_upward(_SCRIPT_DIR, os.path.join("utils", "aio_redis.py")),
        # 本机: 策略/AB仓策略 -> ../../合约项目/strategy_base
        os.path.abspath(os.path.join(_SCRIPT_DIR, "..", "..", "合约项目", "strategy_base")),
    ]
    for root in candidates:
        if root and os.path.isfile(os.path.join(root, "utils", "aio_redis.py")):
            return root
    return None


_add_sys_path(_SCRIPT_DIR)

_STRATEGY_BASE_ROOT = _resolve_strategy_base_root()
_add_sys_path(_STRATEGY_BASE_ROOT or "")

# utils/config 以 strategy_base 为准，强制置于 sys.path 最前
if _STRATEGY_BASE_ROOT:
    if _STRATEGY_BASE_ROOT in sys.path:
        sys.path.remove(_STRATEGY_BASE_ROOT)
    sys.path.insert(0, _STRATEGY_BASE_ROOT)
else:
    raise SystemExit(
        f"未找到 strategy_base 根目录（需要 utils/aio_redis.py），当前脚本目录: {_SCRIPT_DIR}"
    )

from utils.aio_redis import MyAioredis  # noqa: E402
from client.env_pro.rest.websea.contract import WebseaContract as Contract  # noqa: E402
import objects.contract_request.websea as ocw  # noqa: E402
from signals import Task, _ccxt_our_symbol  # noqa: E402
from libs.log import logger  # noqa: E402


def _resolve_api_key() -> str:
    return API_KEY or os.environ.get("WEBSEA_API_KEY", "")


def _resolve_api_secret() -> str:
    return API_SECRET or os.environ.get("WEBSEA_API_SECRET", "")


async def _retry_rest(label: str, fn):
    """REST 失败（None 或异常）时间隔 1s 重试。"""
    for i in range(1, API_RETRY_TIMES + 1):
        try:
            ret = await fn()
            if ret is not None:
                return ret
        except Exception as e:
            logger.warning("%s attempt %d/%d error: %s", label, i, API_RETRY_TIMES, e)
        if i < API_RETRY_TIMES:
            logger.warning("%s attempt %d/%d failed, retry in %ss", label, i, API_RETRY_TIMES, API_RETRY_INTERVAL_SEC)
            await asyncio.sleep(API_RETRY_INTERVAL_SEC)
    return None


class SlipStats:
    """每笔成交价相对 BBO 一档的滑点，每 batch 笔输出累计平均/最大滑点。"""

    def __init__(self, batch: int = SLIP_STAT_BATCH) -> None:
        self.batch = batch
        self.n = 0
        self.slips: list[float] = []

    def record(self, side: str, bbo: float, fill_price: float) -> None:
        if bbo <= 0 or fill_price <= 0:
            return
        slip = (fill_price - bbo) / bbo * 100 if side == "buy" else (bbo - fill_price) / bbo * 100
        self.slips.append(slip)
        self.n += 1
        logger.info("slip side=%s bbo=%s fill=%s slip=%.4f%%", side, bbo, fill_price, slip)
        if self.n % self.batch == 0:
            logger.info(
                "slip_stat total(%d): avg=%.4f%% max=%.4f%%",
                self.n, sum(self.slips) / len(self.slips), max(self.slips),
            )


class PnlStats:
    """开平一轮盈亏占比，每 batch 笔输出累计平均（按开仓方向分组）。"""

    def __init__(self, batch: int = SLIP_STAT_BATCH) -> None:
        self.batch = batch
        self.n = 0
        self.all_pnls: list[float] = []
        self.buy_open_pnls: list[float] = []
        self.sell_open_pnls: list[float] = []

    def record(self, open_side: str, open_price: float, close_price: float) -> None:
        if open_price <= 0 or close_price <= 0:
            return
        pnl = (close_price - open_price) / open_price * 100 if open_side == "buy" else (
            (open_price - close_price) / open_price * 100
        )
        self.all_pnls.append(pnl)
        (self.buy_open_pnls if open_side == "buy" else self.sell_open_pnls).append(pnl)
        self.n += 1
        logger.info("pnl open=%s open_px=%s close_px=%s pnl=%.4f%%", open_side, open_price, close_price, pnl)
        if self.n % self.batch == 0:
            buy_avg = sum(self.buy_open_pnls) / len(self.buy_open_pnls) if self.buy_open_pnls else 0.0
            sell_avg = sum(self.sell_open_pnls) / len(self.sell_open_pnls) if self.sell_open_pnls else 0.0
            logger.info(
                "pnl_stat total(%d): avg=%.4f%% buy_open_avg=%.4f%% sell_open_avg=%.4f%%",
                self.n, sum(self.all_pnls) / len(self.all_pnls), buy_avg, sell_avg,
            )


class LiveRunner:
    """Redis 行情 + signals.Task 信号 + Websea 开平仓。"""

    def __init__(self) -> None:
        self.rest: Optional[Contract] = None
        self.task: Optional[Task] = None
        self.redis_conn = None
        self.position: Optional[str] = None
        self.bid: float = 0.0
        self.ask: float = 0.0
        self.trade_seq: int = 0
        self._lock = asyncio.Lock()
        self._inst = _ccxt_our_symbol(SYMBOL)
        self._precision: Optional[Any] = None
        self._pos_sheets: float = 0.0  # 开仓实际成交张数，平仓沿用
        self._last_open_ts: float = 0.0
        self._open_side: Optional[str] = None
        self._open_price: float = 0.0
        self.slip_stats = SlipStats()
        self.pnl_stats = PnlStats()

    async def setup(self) -> None:
        key, secret = _resolve_api_key(), _resolve_api_secret()
        if not key or not secret:
            raise SystemExit("请在本文件顶部填写 API_KEY/API_SECRET，或设置环境变量 WEBSEA_API_KEY / WEBSEA_API_SECRET")

        self.rest = Contract(token=key, secret_key=secret)
        self.rest.DEBUG = False
        self._precision = await _retry_rest(
            f"get_precision {SYMBOL}",
            lambda: self.rest.get_precision(SYMBOL, quan=True),
        )
        if self._precision is None:
            self._precision = await _retry_rest(
                f"get_precision {SYMBOL} quan=False",
                lambda: self.rest.get_precision(SYMBOL, quan=False),
            )
        if self._precision is None:
            raise SystemExit(f"获取 {SYMBOL} 精度失败（已重试 {API_RETRY_TIMES} 次）")
        logger.info("WebseaContract(env_pro) initialized symbol=%s precision=%s", SYMBOL, self._precision)

        await self._flatten_on_start()

        self.task = Task(
            backtest=True,
            on_trade_signal=self._on_trade_signal,
            strict_trade_id=False,
            write_signal_csv=False,
        )
        self.task.SYMBOLS = [SYMBOL]
        asyncio.create_task(self._sync_position_loop())

    async def _sync_position_loop(self) -> None:
        """每分钟查持仓，与 self.position 不一致则修正。"""
        while True:
            await asyncio.sleep(POSITION_SYNC_INTERVAL_SEC)
            assert self.rest is not None
            positions = await _retry_rest(
                f"get_position sync {SYMBOL}",
                lambda: self.rest.get_position(symbol=SYMBOL, is_full=MARGIN_MODE_API, quan=False),
            )
            if positions is None:
                continue
            ex_side, ex_sheets = None, 0.0
            for pos in positions:
                sheets = float(pos.avail_amount or 0)
                if sheets > 0:
                    ex_side = "long" if pos.type == 1 else "short"  # type 1=多 2=空
                    ex_sheets = sheets
                    break
            async with self._lock:
                if ex_side != self.position or abs(ex_sheets - self._pos_sheets) > 1e-9:
                    logger.warning(
                        "position sync fix: local=%s sheets=%s -> exchange=%s sheets=%s",
                        self.position, self._pos_sheets, ex_side, ex_sheets,
                    )
                    self.position = ex_side
                    self._pos_sheets = ex_sheets

    def _on_trade_signal(self, rec: Dict[str, Any]) -> None:
        asyncio.get_running_loop().create_task(self._handle_signal(rec))

    def _slip_price(self, side: str, bid: float, ask: float, slip: float) -> float:
        """买加价、卖减价，便于一次性吃单成交。"""
        return ask * (1 + slip) if side == "buy" else bid * (1 - slip)

    async def _flatten_on_start(self) -> None:
        """启动时平掉 SYMBOL 全部持仓（BBO ±滑点限价）。"""
        assert self.rest is not None
        positions = await _retry_rest(
            f"get_position {SYMBOL}",
            lambda: self.rest.get_position(symbol=SYMBOL, is_full=MARGIN_MODE_API, quan=False),
        )
        if positions is None:
            raise SystemExit(f"启动获取 {SYMBOL} 持仓失败")
        if not positions:
            logger.info("startup flatten: no position symbol=%s", SYMBOL)
            self.position = None
            self._pos_sheets = 0.0
            return

        bid, ask = await self._get_bbo()
        if bid <= 0 or ask <= 0:
            raise SystemExit(f"启动平仓失败: 无法获取 {SYMBOL} BBO bid={bid} ask={ask}")

        for pos in positions:
            sheets = float(pos.avail_amount or 0)
            if sheets <= 0:
                continue
            side = "sell" if pos.type == 1 else "buy"
            od_type = ocw.OrderType.sell_limit if side == "sell" else ocw.OrderType.buy_limit
            price = self._slip_price(side, bid, ask, STARTUP_CLOSE_SLIP)
            logger.info(
                "startup flatten side=%s sheets=%s price=%s (bbo bid=%s ask=%s slip=%.2f%%)",
                side, sheets, price, bid, ask, STARTUP_CLOSE_SLIP * 100,
            )
            resp = await _retry_rest(
                f"startup close {side}",
                lambda s=side, o=od_type, p=price, a=sheets: self.rest.order_create(
                    symbol=SYMBOL,
                    od_type=o,
                    price=p,
                    amount=a,
                    contract_type="close",
                    is_full=MARGIN_MODE_API,
                    precision=self._precision,
                    quan=False,
                ),
            )
            if not resp or not resp.order_id:
                raise SystemExit(f"启动平仓下单失败 type={pos.type} sheets={sheets}")
            await asyncio.sleep(ORDER_QUERY_DELAY_SEC)
            detail = await _retry_rest(
                f"startup order_detail {resp.order_id}",
                lambda oid=str(resp.order_id): self.rest.order_detail(order_id=oid),
            )
            filled = float(detail.deal_amount or 0) if detail else 0.0
            if filled < sheets:
                await _retry_rest(
                    f"startup order_cancel {resp.order_id}",
                    lambda oid=str(resp.order_id): self.rest.order_cancel(order_ids=[oid], quan=False),
                )
                raise SystemExit(
                    f"启动平仓未完全成交 filled={filled} need={sheets} order_id={resp.order_id}"
                )
            logger.info("startup flatten ok order_id=%s filled=%s", resp.order_id, filled)

        self.position = None
        self._pos_sheets = 0.0
        logger.info("startup flatten done symbol=%s", SYMBOL)

    async def on_redis_message(self, channel: str, item: Dict[str, Any]) -> None:
        if "bids_asks" in channel and item.get("messageType") == "message":
            try:
                b = float(item.get("bid") or 0)
                a = float(item.get("ask") or 0)
            except (TypeError, ValueError):
                return
            if b > 0:
                self.bid = b
            if a > 0:
                self.ask = a
            return

        if not channel.startswith("contract.trade") or "binance" not in channel:
            return
        if self.task is None:
            return
        try:
            self.trade_seq += 1
            norm = {
                "trade_id": self.trade_seq,
                "price": str(item["price"]),
                "size": str(item["amount"]),
                "side": item["side"],
                "timestamp": int(item["timestamp"]),
                "instrument_id": self._inst,
            }
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("trade parse skip: %s item=%s", e, item)
            return
        self.task.save_data(norm)

    def _sheets_to_coin(self, sheets: float) -> float:
        if self._precision is None:
            return sheets
        return sheets * float(self._precision.faceValue)

    def _coin_to_sheets(self, coin: float) -> float:
        if self._precision is None:
            return coin
        return coin / float(self._precision.faceValue)

    async def _get_bbo(self) -> Tuple[float, float]:
        if self.bid > 0 and self.ask > 0:
            return self.bid, self.ask
        if self.rest is None:
            return 0.0, 0.0
        try:
            depth = await _retry_rest(
                f"get_depth {SYMBOL}",
                lambda: self.rest.get_depth(SYMBOL, limit=5, quan=True),
            )
            if depth and depth.bids and depth.asks:
                self.bid = float(depth.bids[0].price)
                self.ask = float(depth.asks[0].price)
                return self.bid, self.ask
        except Exception as e:
            logger.warning("get_depth fallback failed: %s", e)
        return self.bid, self.ask

    async def _record_fill(self, order_id: str, side: str, bid: float, ask: float) -> Tuple[float, float]:
        """撤余单并查成交，返回 (成交张数, 成交均价)；有成交则统计相对 BBO 滑点。"""
        assert self.rest is not None
        await asyncio.sleep(ORDER_QUERY_DELAY_SEC)
        await _retry_rest(
            f"order_cancel {order_id}",
            lambda: self.rest.order_cancel(order_ids=[str(order_id)], quan=False),
        )
        detail = await _retry_rest(
            f"order_detail {order_id}",
            lambda: self.rest.order_detail(order_id=str(order_id)),
        )
        if detail is None:
            logger.warning("order_detail failed id=%s after retries", order_id)
            return 0.0, 0.0
        logger.info("order_detail: %s", detail)
        filled_sheets = float(detail.deal_amount or 0)
        fill_avg = float(detail.price_avg or 0)
        if filled_sheets > 0 and fill_avg > 0:
            bbo = ask if side == "buy" else bid
            self.slip_stats.record(side, bbo, fill_avg)
        return filled_sheets, fill_avg

    async def _place_order(self, side: str, reduce_only: bool) -> bool:
        assert self.rest is not None
        bid, ask = await self._get_bbo()
        price = self._slip_price(side, bid, ask, ORDER_SLIP)
        if price <= 0:
            logger.warning("skip order: no bbo side=%s bid=%s ask=%s", side, bid, ask)
            return False

        od_type = ocw.OrderType.buy_limit if side == "buy" else ocw.OrderType.sell_limit
        contract_type = "close" if reduce_only else "open"
        if reduce_only:
            amount_sheets = self._pos_sheets
            if amount_sheets <= 0:
                logger.warning("skip close: pos_sheets=0")
                return False
        else:
            if self._last_open_ts > 0:
                elapsed = time.time() - self._last_open_ts
                if elapsed < OPEN_COOLDOWN_SEC:
                    logger.info(
                        "skip open: cooldown %.0fs since last open (need %ss) side=%s",
                        elapsed, OPEN_COOLDOWN_SEC, side,
                    )
                    return False
            amount_sheets = self._coin_to_sheets(ORDER_AMOUNT)
        try:
            resp = await _retry_rest(
                f"order_create {side} {contract_type}",
                lambda: self.rest.order_create(
                    symbol=SYMBOL,
                    od_type=od_type,
                    price=price,
                    amount=amount_sheets,
                    contract_type=contract_type,
                    is_full=MARGIN_MODE_API,
                    precision=self._precision,
                    quan=False,
                ),
            )
            logger.info("order_create resp: %s", resp)
        except Exception as e:
            logger.exception("order_create error side=%s reduce=%s: %s", side, reduce_only, e)
            return False

        if resp is None:
            logger.error("order rejected side=%s reduce=%s", side, reduce_only)
            return False
        order_id = resp.order_id
        logger.info(
            "order ok side=%s reduce_only=%s price=%s sheets=%s id=%s",
            side,
            reduce_only,
            price,
            amount_sheets,
            order_id,
        )
        if not order_id:
            return False
        filled_sheets, fill_avg = await self._record_fill(str(order_id), side, bid, ask)
        if filled_sheets <= 0:
            logger.warning("order no fill side=%s reduce=%s id=%s", side, reduce_only, order_id)
            return False
        if reduce_only:
            self._pos_sheets = max(0.0, self._pos_sheets - filled_sheets)
            if fill_avg > 0 and self._open_side and self._open_price > 0:
                self.pnl_stats.record(self._open_side, self._open_price, fill_avg)
                self._open_side = None
                self._open_price = 0.0
        else:
            self._pos_sheets = filled_sheets
            self._last_open_ts = time.time()
            if fill_avg > 0:
                self._open_side = side
                self._open_price = fill_avg
        return True

    async def _handle_signal(self, rec: Dict[str, Any]) -> None:
        want = "long" if int(rec["signal_side"]) == 1 else "short"
        logger.info(
            "signal want=%s m=%s ts=%s",
            want,
            rec.get("m"),
            rec.get("timestamp"),
        )

        async with self._lock:
            bid, ask = await self._get_bbo()
            if bid <= 0 or ask <= 0:
                logger.warning("skip signal: no valid websea bbo bid=%s ask=%s", bid, ask)
                return

            if self.position == want:
                if self._pos_sheets > 0:
                    logger.info("skip signal: already %s sheets=%s", want, self._pos_sheets)
                    return
                logger.warning("phantom position=%s pos_sheets=0, reset", want)
                self.position = None

            if self.position == "long" and want == "short":
                if not await self._place_order("sell", True):
                    return
                if not await self._place_order("sell", False):
                    return
                self.position = "short"
            elif self.position == "short" and want == "long":
                if not await self._place_order("buy", True):
                    return
                if not await self._place_order("buy", False):
                    return
                self.position = "long"
            elif self.position is None:
                if want == "long":
                    if await self._place_order("buy", False):
                        self.position = "long"
                elif await self._place_order("sell", False):
                    self.position = "short"

            logger.info("position now=%s", self.position)


async def main() -> None:
    runner = LiveRunner()
    await runner.setup()

    redis_pool = MyAioredis(db=REDIS_DB)
    runner.redis_conn = await redis_pool.open()
    logger.info(
        "live start symbol=%s trade=%s bbo=%s amount=%s leverage=%s order_slip=%.2f%%",
        SYMBOL,
        CHANNEL_TRADE,
        CHANNEL_BBO,
        ORDER_AMOUNT,
        LEVERAGE,
        ORDER_SLIP * 100,
    )
    await runner.redis_conn.subscribe_async(
        channel=[CHANNEL_TRADE, CHANNEL_BBO],
        callback=runner.on_redis_message,
    )


if __name__ == "__main__":
    asyncio.run(main())

