#!/usr/bin/env python3
"""
功能：各 coin 最低分地址反向跟单 — HL trades + Websea/Binance 下单 + Redis BBO。

用法：
    cd ~/strategy_base/strategy/AB/hyper_follow_strategy
    pip install -r requirements.txt   # 必须含 pyarrow
    python3 reverse_deal_startegy.py
"""

import asyncio
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from hyper_config import SCORE_DIR, ROOT, latest_parquet
from sub_hyper_deals import run as run_trades
from query_user_positions import fetch_clearinghouse

from template.template_timer import TemplateTimer, CronTrigger

# ---------- 策略参数 ----------
TRACKED_COINS = ["BTC", "ETH", "SOL", "HYPE"]       # 跟单交易对，与 HL coin 名称一致

# Websea 合约账户（反向跟单主腿）
WEBSEA_API_KEY = "23cb167617bb8398ccfcf74506t64669304"
WEBSEA_API_SECRET = "9mowefaye9nhyxf2w7zq"

# Binance U 本位合约账户（对冲腿）
BINANCE_API_KEY = "qBY47YZYAP6ZT4BQdOAqf7uyLUSOctSJ3OWeE3qFXBMQSrnNAHyP74tknFePsQFO"
BINANCE_API_SECRET = "9oKFd6iM3YObfPE3xrlDHIMIiMMOum4Z9M2ZPYhLRXxJ2wSqltqbsfmg7jNbI61i"

ORDER_SLIP = 0.002  # 限价单滑点，买：ask*(1+slip)，卖：bid*(1-slip)
PNL_BATCH = 10      # 每 N 笔下单记录统计一次盈亏
STAT_BATCH = 20     # 每满 N 笔按 coin/交易所打印胜负统计
EQUITY_ALERT = 10   # 两账户总权益相对启动时下跌超过该值(U)则报警
EQUITY_INTERVAL = 60  # 权益监控间隔(秒)
NO_TRADE_ALERT = 8 * 3600  # 某 coin 连续无成交超过该秒数则预警
ORDER_LOG = Path(ROOT) / "order_records.log"  # 订单记录日志（每次覆盖）


class Strategy(TemplateTimer):
    """Hyperliquid 反向跟单策略（结构参考龙虾1号）。"""

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config()

    def _load_config(self):
        """初始化账户、精度缓存、BBO 缓存。"""
        self.tracked_coins = TRACKED_COINS
        self.order_slip = ORDER_SLIP
        self.addr_coins = {}
        self.bbo = {}              # symbol -> exchange -> {bid, ask}
        self.bbo_lock = threading.Lock()
        self.rest = None           # Websea REST (strategy_base)
        self.binance_rest = None   # Binance REST
        self.prec = {}             # websea symbol -> precision对象
        self.unit_size = {}        # websea symbol -> contract_size
        self.binance_prec = {}     # binance symbol -> (price精度, amount精度)
        self.binance_tick = {}     # binance symbol -> tickSize
        self.ws_min_sz = {}        # websea 最小下单量(币)
        self.bn_min_sz = {}        # binance 最小下单量(币)
        self.bn_min_notional = {}  # binance 最小下单名义价值(USDT)
        self.order_records = []    # [{exchange, coin, price, sz, side}, ...]
        self.init_ws_equity = None  # 启动时 websea 权益
        self.init_bn_equity = None  # 启动时 bn 权益
        self.last_trade_ts = {c: time.time() for c in TRACKED_COINS}  # coin -> 上次跟单成交时间
        self.stale_alerted = set()  # 已发过无成交预警的 coin

    @staticmethod
    def _coin_symbol(coin):
        """HL coin -> 合约 symbol。"""
        return f"{coin}-USDT"

    async def on_first(self):
        """启动：加载地址、初始化交易所、订阅 BBO、订阅 HL trades。"""
        print("=== 各交易对最低分地址 ===")
        self.addr_coins = self.load_addr_coins()
        print(f"\n订阅 HL trades: {self.tracked_coins}")
        print(f"addr_coins: {self.addr_coins}")

        await self.init_exchanges()
        print("=== 启动前账户状态 ===")
        await self._snapshot_equity()
        await self.sub_bbo()
        await self.close_all_positions()
        self.init_ws_equity, self.init_bn_equity = await self._snapshot_equity()
        print(f"[启动权益] websea={self.init_ws_equity:.4f} bn={self.init_bn_equity:.4f}")
        threading.Thread(target=self._run_equity_monitor, daemon=True).start()
        # HL trades 为同步阻塞 WSS，放独立线程；成交回调投递到本 loop
        threading.Thread(target=self._run_hl_trades, daemon=True).start()

    async def on_timer(self):
        """定时任务占位（可按需扩展心跳/风控）。"""
        pass

    def load_addr_coins(self):
        """读 parquet_file/score/ 中日期最新的 {YYYY-MM-DD}_address_scores.parquet。"""
        scores_file = latest_parquet(SCORE_DIR, "*_address_scores.parquet")
        print(f"读取最新打分({scores_file.name[:10]}): {scores_file}")

        df = pd.read_parquet(scores_file)
        sub = df[df["coin"].isin(self.tracked_coins)]
        if sub.empty:
            raise ValueError("无 BTC/ETH/SOL/HYPE 打分记录")

        addr_coins = {}
        for coin in self.tracked_coins:
            coin_df = sub[sub["coin"] == coin]
            if coin_df.empty:
                continue
            row = coin_df.loc[coin_df["score"].idxmin()]
            addr = str(row["address"]).lower()
            addr_coins[coin] = addr
            print(f"  {coin}  address={addr}  score={row['score']:.4f}")

        if not addr_coins:
            raise ValueError("未选出任何地址")
        return addr_coins

    async def init_exchanges(self):
        """初始化 Websea、Binance REST 及精度。"""
        await self.init_websea()
        await self.init_binance()

    async def init_websea(self):
        """Websea 合约（同 hedge.py：strategy_base WebseaContract）。"""
        from client.env_pro.rest.websea.contract import WebseaContract

        self.rest = WebseaContract(WEBSEA_API_KEY, WEBSEA_API_SECRET)
        self.rest.DEBUG = False
        symbols = await self.rest.get_symbols(quan=True)
        for s in symbols:
            self.unit_size[s.symbol] = s.contract_size
            self.ws_min_sz[s.symbol] = float(s.contract_size)
        self.prec = await self.rest.get_precision(quan=True) or {}
        print(f"Websea 精度已加载: {list(self.prec.keys())}")

    async def _ws_equity(self):
        bal = await self.rest.get_walletList(is_full=2)
        return float(getattr(bal, "avail", 0) or 0)

    async def _ws_positions(self, symbol=None):
        pos = await self.rest.get_position(symbol=symbol, is_full=1) or []
        out = []
        for p in pos:
            size = self.unit_size.get(p.symbol, 1)
            amt = p.amount * size
            out.append({
                "symbol": p.symbol, "side": "buy" if p.type == 1 else "sell",
                "amount": p.amount, "sz": amt, "entry": p.open_price_avg,
                "upnl": p.un_profit, "lev": p.lever_rate, "raw": p,
            })
        return out

    def _print_ws_positions(self, positions):
        for p in positions:
            side = "多" if p["raw"].type == 1 else "空"
            print(
                f"  [ws] {p['symbol']} {side} amt={p['amount']} sz={p['sz']:.6f} "
                f"entry={p['entry']} upnl={p['upnl']}"
            )
        if not positions:
            print("  [ws] (无持仓)")

    async def init_binance(self):
        """Binance U 本位合约精度。"""
        from crypto_center.client.rest.binance.u_contract import UBinanceContract

        self.binance_rest = UBinanceContract(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.binance_rest.DEBUG = False
        for s, v in (await self.binance_rest.fetch_precision()).items():
            self.binance_prec[s] = (int(v["price"]), int(v["amount"]))
            info = v.get("info") or {}
            filters = info.get("filters") or []
            tick = next(
                (float(f["tickSize"]) for f in filters if f.get("filterType") == "PRICE_FILTER"),
                10 ** (-int(v["price"])),
            )
            self.binance_tick[s] = tick
            self.bn_min_sz[s] = float((v.get("limit") or {}).get("amount", {}).get("min") or 0)
            self.bn_min_notional[s] = next(
                (float(f.get("notional") or f.get("minNotional") or 0)
                 for f in filters if f.get("filterType") in ("MIN_NOTIONAL", "NOTIONAL")),
                0.0,
            )
        print(f"Binance 精度已加载: {list(self.binance_prec.keys())}")

    async def listen_bbo(self, channel, callback):
        """单 channel Redis BBO 订阅（同 wss推送直连aws测试 listen）。"""
        from config.redis_cfg import RedisConfigClusterAwsPro
        from utils.redis_cluster_shard import RedisClusterShard

        redis = RedisClusterShard(
            RedisConfigClusterAwsPro.HOST,
            RedisConfigClusterAwsPro.PORT,
            RedisConfigClusterAwsPro.PASSWORD,
        )
        await redis.subscribe(channel=channel, callback=callback)

    async def on_bbo_message(self, channel, item):
        """解析 BBO 推送，缓存 bid/ask。"""
        if not channel.startswith("contract.bids_asks"):
            return
        parts = channel.split(".")
        if len(parts) < 4:
            return
        symbol, exchange = parts[2], parts[3]
        bid, ask = item.get("bid"), item.get("ask")
        if bid is None or ask is None:
            return
        with self.bbo_lock:
            self.bbo.setdefault(symbol, {})[exchange] = {"bid": float(bid), "ask": float(ask)}

    async def sub_bbo(self):
        """订阅各 coin 的 websea、binance BBO channel。"""
        for coin in self.tracked_coins:
            symbol = self._coin_symbol(coin)
            for exchange in ("websea", "binance"):
                ch = f"contract.bids_asks.{symbol}.{exchange}"
                print(f"订阅 BBO: {ch}")
                self.loop.create_task(self.listen_bbo(ch, self.on_bbo_message))

    def get_bbo_prices(self, coin):
        """读取指定 coin 最新 binance、websea BBO。"""
        symbol = self._coin_symbol(coin)
        with self.bbo_lock:
            ex = self.bbo.get(symbol, {})
        bn, ws = ex.get("binance", {}), ex.get("websea", {})
        return {
            "binance_bid": bn.get("bid"),
            "binance_ask": bn.get("ask"),
            "websea_bid": ws.get("bid"),
            "websea_ask": ws.get("ask"),
        }

    async def _snapshot_equity(self):
        """查询 websea / bn 权益与持仓，打印持仓，返回 (ws_equity, bn_equity)。"""
        ws_eq, ws_pos, bn_bal, bn_pos = await asyncio.gather(
            self._ws_equity(),
            self._ws_positions(),
            self.binance_rest.fetch_balance("USDT"),
            self.binance_rest.fetch_position(),
        )
        bn_usdt = (bn_bal or {}).get("USDT") or {}
        bn_eq = float(bn_usdt["total"] if isinstance(bn_usdt, dict) else getattr(bn_usdt, "total", 0) or 0)

        print(f"[账户监控] websea权益={ws_eq:.4f} bn权益={bn_eq:.4f} 合计={ws_eq + bn_eq:.4f}")
        self._print_ws_positions(ws_pos)
        for p in bn_pos or []:
            print(
                f"  [bn] {p['symbol']} side={p['side']} sz={p['contracts']} "
                f"entry={p['entryPrice']} upnl={p['unrealizedPnl']}"
            )
        if not bn_pos:
            print("  [bn] (无持仓)")
        return ws_eq, bn_eq

    async def _check_equity(self):
        """定时检查权益；总权益相对启动下跌超阈值则报警。"""
        try:
            ws_eq, bn_eq = await self._snapshot_equity()
            drop = (self.init_ws_equity + self.init_bn_equity) - (ws_eq + bn_eq)
            if drop >= EQUITY_ALERT:
                print(
                    f"[权益报警] 总权益下跌 {drop:.4f}U (>= {EQUITY_ALERT}U) | "
                    f"websea 初始={self.init_ws_equity:.4f} 当前={ws_eq:.4f} | "
                    f"bn 初始={self.init_bn_equity:.4f} 当前={bn_eq:.4f}"
                )
        except Exception:
            print(f"[账户监控] 失败\n{traceback.format_exc()}")

    def _run_equity_monitor(self):
        """独立线程：每 EQUITY_INTERVAL 秒查询权益/持仓，并检查无成交预警。"""
        while True:
            time.sleep(EQUITY_INTERVAL)
            fut = asyncio.run_coroutine_threadsafe(self._monitor_tick(), self.loop)
            try:
                fut.result(timeout=EQUITY_INTERVAL)
            except Exception as e:
                print(f"[账户监控] 调度失败: {e}")

    async def _monitor_tick(self):
        await self._check_equity()
        await self._check_stale_trades()

    async def _check_stale_trades(self):
        """某 coin 连续 NO_TRADE_ALERT 无跟单成交则预警（每轮只报一次，有新成交后重置）。"""
        now = time.time()
        for coin in self.tracked_coins:
            last = self.last_trade_ts.get(coin, now)
            if now - last < NO_TRADE_ALERT:
                continue
            if coin in self.stale_alerted:
                continue
            self.stale_alerted.add(coin)
            try:
                symbol = self._coin_symbol(coin)
                leader = self.addr_coins.get(coin)

                async def _empty():
                    return {}

                leader_ch, ws_pos, bn_pos = await asyncio.gather(
                    asyncio.to_thread(fetch_clearinghouse, leader) if leader else _empty(),
                    self._ws_positions(symbol),
                    self.binance_rest.fetch_position(symbol),
                )
                leader_pos = next(
                    (p.get("position") for p in (leader_ch.get("assetPositions") or [])
                     if (p.get("position") or {}).get("coin") == coin),
                    None,
                )
                last_str = datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"[无成交预警] coin={coin} 上次交易={last_str} "
                    f"idle={(now - last) / 3600:.1f}h | "
                    f"websea持仓={ws_pos or '无'} | bn持仓={bn_pos or '无'} | "
                    f"leader={leader} 持仓={leader_pos or '无'}"
                )
            except Exception:
                print(f"[无成交预警] {coin} 查询失败\n{traceback.format_exc()}")

    async def close_all_positions(self):
        """启动时用 BBO+滑点限价平掉 websea / bn 全部持仓。"""
        print("=== 启动清仓 ===")
        for _ in range(50):
            if all(
                all(self.get_bbo_prices(c).get(k) for k in (
                    "websea_bid", "websea_ask", "binance_bid", "binance_ask"
                ))
                for c in self.tracked_coins
            ):
                break
            await asyncio.sleep(0.1)

        ws1, ws2, bn_pos = await asyncio.gather(
            self.rest.get_position(is_full=1),
            self.rest.get_position(is_full=2),
            self.binance_rest.fetch_position(),
        )
        tasks = []
        for is_full, resp in ((1, ws1), (2, ws2)):
            for p in resp or []:
                symbol, amt = p.symbol, abs(int(p.amount or 0))
                if not amt or symbol not in self.prec:
                    continue
                coin = symbol.split("-")[0]
                px = self.get_bbo_prices(coin)
                bid, ask = px["websea_bid"], px["websea_ask"]
                if bid is None or ask is None:
                    print(f"跳过 websea 平仓 {symbol}: 无BBO")
                    continue
                side = "sell" if p.type == 1 else "buy"
                price = self._websea_order_price(side, bid, ask, symbol)
                print(f"[清仓 websea] {symbol} {side} amount={amt} price={price} is_full={is_full}")
                tasks.append(self._ws_order(symbol, side, price, amt, reduce_only=True, is_full=is_full))
        for p in bn_pos or []:
            symbol, contracts = p["symbol"], float(p["contracts"])
            if not contracts or symbol not in self.binance_prec:
                continue
            coin = symbol.split("-")[0]
            px = self.get_bbo_prices(coin)
            bid, ask = px["binance_bid"], px["binance_ask"]
            if bid is None or ask is None:
                print(f"跳过 binance 平仓 {symbol}: 无BBO")
                continue
            side = "sell" if contracts > 0 else "buy"
            vol = round(abs(contracts), self.binance_prec[symbol][1])
            price = self._binance_order_price(side, bid, ask, symbol)
            print(f"[清仓 binance] {symbol} {side} vol={vol} price={price}")
            tasks.append(self.binance_rest.create_order(
                symbol=symbol, order_type="limit", side=side,
                amount=vol, price=price, timeInForce="GTC", reduceOnly=True,
            ))
        if not tasks:
            print("无持仓，跳过清仓")
            return
        for r in await asyncio.gather(*tasks, return_exceptions=True):
            print(f"[清仓回报] {r}")

    def _websea_order_price(self, side, bid, ask, symbol):
        """Websea 限价：BBO + 滑点。"""
        px_prec = int(self.prec[symbol].price)
        if side == "buy":
            return round(ask * (1 + self.order_slip), px_prec)
        return round(bid * (1 - self.order_slip), px_prec)

    async def _ws_order(self, symbol, side, price, amount, reduce_only=False, is_full=1):
        """Websea 下单（同 hedge.py：precision quan=True，order_create 走普通接口）。"""
        import objects.contract_request.websea as ocw

        od_type = ocw.OrderType.buy_limit if side == "buy" else ocw.OrderType.sell_limit
        precision = await self.rest.get_precision(symbol, quan=True)
        return await self.rest.order_create(
            symbol=symbol, od_type=od_type, amount=abs(amount), price=price,
            precision=precision, contract_type="close" if reduce_only else "open", is_full=is_full,
        )

    def _binance_order_price(self, side, bid, ask, symbol):
        """Binance 限价：BBO + 滑点，按 tickSize 对齐（避免 -4014）。"""
        tick = self.binance_tick.get(symbol) or 10 ** (-self.binance_prec[symbol][0])
        raw = ask * (1 + self.order_slip) if side == "buy" else bid * (1 - self.order_slip)
        return round(round(raw / tick) * tick, 8)

    @staticmethod
    def _calc_pnl(records):
        """同方向未平仓盈亏=0；有买有卖则按 min(买量,卖量) 匹配计算已平仓盈亏。"""
        buy_sz = sum(r["sz"] for r in records if r["side"] == "buy")
        sell_sz = sum(r["sz"] for r in records if r["side"] == "sell")
        if buy_sz == 0 or sell_sz == 0:
            return 0.0
        matched = min(buy_sz, sell_sz)
        avg_buy = sum(r["price"] * r["sz"] for r in records if r["side"] == "buy") / buy_sz
        avg_sell = sum(r["price"] * r["sz"] for r in records if r["side"] == "sell") / sell_sz
        return (avg_sell - avg_buy) * matched

    def _print_order_stats(self):
        """同 统计盈亏.py：按 exchange+coin 配对反向单，统计胜负/胜率/盈亏比例/持仓时长。"""
        stats, prev = defaultdict(lambda: [0, 0, [], []]), {}
        for od in self.order_records:
            px = od.get("raw_price") or od["price"]
            key, cur = (od["exchange"], od["coin"]), {**od, "price": px}
            p = prev.get(key)
            if p and p["side"] != cur["side"]:
                win = cur["price"] > p["price"] if p["side"] == "buy" else cur["price"] < p["price"]
                ratio = ((cur["price"] - p["price"]) / p["price"] if p["side"] == "buy"
                         else (p["price"] - cur["price"]) / p["price"])
                stats[key][0 if win else 1] += 1
                stats[key][2].append(ratio)
                stats[key][3].append(abs(cur["ts"] - p["ts"]) / 1000)
                prev[key] = None
            else:
                prev[key] = cur
        print(f"[订单统计 共{len(self.order_records)}笔]")
        for (ex, coin), (w, l, ratios, holds) in stats.items():
            n = w + l
            print(
                f"{ex}_{coin} 胜:{w} 负:{l} 胜率:{(w / n if n else 0):.2%} "
                f"单笔盈利比例:{(sum(ratios) / len(ratios) if ratios else 0):.4%} "
                f"平均持仓时长:{(sum(holds) / len(holds) if holds else 0):.1f}s"
            )

    def _record_order(self, exchange, coin, raw_price, price, sz, side):
        ts = int(time.time() * 1000)
        self.order_records.append({
            "exchange": exchange, "coin": coin, "raw_price": raw_price, "price": price,
            "sz": sz, "side": side, "ts": ts,
            "time": datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        })
        with open(ORDER_LOG, "w", encoding="utf-8") as f:
            f.write(f"记录订单: {self.order_records}\n")
        if len(self.order_records) % STAT_BATCH == 0:
            self._print_order_stats()
        if len(self.order_records) % PNL_BATCH != 0:
            return
        batch = self.order_records[-PNL_BATCH:]
        ws = [r for r in batch if r["exchange"] == "websea"]
        bn = [r for r in batch if r["exchange"] == "binance"]
        print(
            f"[盈亏统计 近{PNL_BATCH}笔] websea={self._calc_pnl(ws):.4f} "
            f"binance={self._calc_pnl(bn):.4f}"
        )

    async def make_websea_order(self, symbol, side, price, vol, reduce_only=False):
        """Websea 限价下单。"""
        try:
            t0 = time.time() * 1000
            amount = int(vol / self.unit_size[symbol])
            result = await self._ws_order(symbol, side, price, amount, reduce_only)
            print(
                f"websea下单 {symbol} 价格:{price} 数量:{vol} 张数:{amount} 方向:{side} "
                f"延时:{int(time.time() * 1000 - t0)}ms 回报:{result}"
            )
        except Exception:
            print(f"websea下单失败 {symbol} {side} price={price} vol={vol}\n{traceback.format_exc()}")

    async def make_binance_order(self, symbol, side, price, vol):
        """Binance U 本位限价下单。"""
        try:
            t0 = time.time() * 1000
            result = await self.binance_rest.create_order(
                symbol=symbol, order_type="limit", side=side,
                amount=abs(vol), price=price, timeInForce="GTC",
            )
            print(
                f"binance下单 {symbol} 价格:{price} 数量:{vol} 方向:{side} "
                f"延时:{int(time.time() * 1000 - t0)}ms 回报:{result}"
            )
        except Exception:
            print(f"binance下单失败 {symbol} {side} price={price} vol={vol}\n{traceback.format_exc()}")

    async def place_websea_order(self, coin, side, sz, bid, ask, trade, reduce_only=False):
        """Websea 下单入口。"""
        symbol = self._coin_symbol(coin)
        if not self.rest or bid is None or ask is None or symbol not in self.prec:
            print(f"跳过 websea 下单 {symbol} {side} sz={sz} bbo={bid}/{ask}")
            return
        price = self._websea_order_price(side, bid, ask, symbol)
        min_sz = self.ws_min_sz.get(symbol, self.unit_size.get(symbol, 0))
        sz = max(float(sz or 0), min_sz)
        vol = round(sz, int(self.prec[symbol].amount))
        amount = max(1, int(vol / self.unit_size[symbol]))
        print(f"[websea] 对冲leader成交: {trade}")
        print(
            f"[websea] 下单参数: symbol={symbol} side={side} price={price} vol={vol} "
            f"amount={amount} min_sz={min_sz} contract_type={'close' if reduce_only else 'open'} is_full=1 "
            f"bbo_bid={bid} bbo_ask={ask} slip={self.order_slip}"
        )
        self._record_order("websea", coin, ask if side == "buy" else bid, price, vol, side)
        await self.make_websea_order(symbol, side, price, vol, reduce_only)

    async def place_binance_order(self, coin, side, sz, bid, ask, trade):
        """Binance 对冲下单。"""
        symbol = self._coin_symbol(coin)
        if not self.binance_rest or bid is None or ask is None or symbol not in self.binance_prec:
            print(f"跳过 binance 下单 {symbol} {side} sz={sz} bbo={bid}/{ask}")
            return
        price = self._binance_order_price(side, bid, ask, symbol)
        # BN 要求名义价值 >= 5U（-4164）；exchangeInfo 可能未返回或 round 后跌破
        min_n = max(float(self.bn_min_notional.get(symbol) or 0), 5.0)
        prec = self.binance_prec[symbol][1]
        step = 10 ** (-prec) if prec else 1.0
        min_sz = max(self.bn_min_sz.get(symbol, 0), (min_n / price) if price else 0)
        vol = max(float(sz or 0), min_sz)
        vol = round(int(vol / step + 0.999999) * step, prec)  # 向上取整到 step
        if price and vol * price < min_n:
            vol = round(int(min_n / price / step + 0.999999) * step, prec)
        print(f"[binance] 对冲leader成交: {trade}")
        print(
            f"[binance] 下单参数: symbol={symbol} side={side} price={price} vol={vol} "
            f"min_sz={min_sz} order_type=limit timeInForce=GTC "
            f"bbo_bid={bid} bbo_ask={ask} slip={self.order_slip}"
        )
        self._record_order("binance", coin, ask if side == "buy" else bid, price, vol, side)
        await self.make_binance_order(symbol, side, price, vol)

    async def handle_trade(self, trade):
        """leader 成交时：Websea + Binance 反向跟单。"""
        coin = trade.get("coin")
        leader_add = self.addr_coins.get(coin)
        if not leader_add or leader_add not in (trade.get("users") or []):
            return

        self.last_trade_ts[coin] = time.time()
        self.stale_alerted.discard(coin)

        buy_address, sell_address = trade["users"][0], trade["users"][1]
        px = self.get_bbo_prices(coin)
        bn_bid, bn_ask = px["binance_bid"], px["binance_ask"]
        ws_bid, ws_ask = px["websea_bid"], px["websea_ask"]

        # leader 持仓（clearinghouse 含权益）
        symbol = self._coin_symbol(coin)
        leader_ch = await asyncio.to_thread(fetch_clearinghouse, leader_add)
        leader_pos = next(
            (p for p in (leader_ch.get("assetPositions") or [])
             if (p.get("position") or {}).get("coin") == coin),
            None,
        )
        leader_equity = float((leader_ch.get("marginSummary") or {}).get("accountValue") or 0)

        # 查询 websea / bn 当前权益与持仓
        ws_equity, ws_pos, bn_bal, bn_pos = await asyncio.gather(
            self._ws_equity(),
            self._ws_positions(symbol),
            self.binance_rest.fetch_balance("USDT"),
            self.binance_rest.fetch_position(symbol),
        )
        bn_usdt = (bn_bal or {}).get("USDT") or {}
        bn_equity = float(bn_usdt["total"] if isinstance(bn_usdt, dict) else getattr(bn_usdt, "total", 0) or 0)
        bn_contracts = abs(float(bn_pos[0]["contracts"])) if bn_pos else 0.0
        bn_lev = float(bn_pos[0]["leverage"]) if bn_pos else 1.0
        print(
            f"权益/持仓 {coin}: leader={leader_equity:.4f} pos={bool(leader_pos)} | "
            f"websea={ws_equity:.4f} pos={ws_pos} | bn={bn_equity:.4f} sz={bn_contracts} lev={bn_lev}"
        )

        # leader 已平仓 → 跟单全平
        if not leader_pos:
            wp = ws_pos[0] if ws_pos else None
            websea_sz = (wp["sz"] if wp["side"] == "buy" else -wp["sz"]) if wp else 0
            bn_sz = float(bn_pos[0]["contracts"]) if bn_pos else 0.0
            if websea_sz > 0:
                await self.place_websea_order(coin, "sell", websea_sz, ws_bid, ws_ask, trade)
            elif websea_sz < 0:
                await self.place_websea_order(coin, "buy", websea_sz, ws_bid, ws_ask, trade)
            if bn_sz > 0:
                await self.place_binance_order(coin, "sell", bn_sz, bn_bid, bn_ask, trade)
            elif bn_sz < 0:
                await self.place_binance_order(coin, "buy", bn_sz, bn_bid, bn_ask, trade)
            print(f"leader已平仓,websea和bn仓位已全平。websea仓位:{websea_sz} bn仓位:{bn_sz}")
            return
        else:
            # 按 leader 持仓比例，用 bn 权益算目标仓，再减当前仓得跟单量
            p = leader_pos["position"]
            pos_amt = abs(float(p.get("positionValue") or 0))
            lev = float((p.get("leverage") or {}).get("value") or 1)
            equity_ratio = pos_amt / (leader_equity * lev) if leader_equity and lev else 0.0
            mid = ((bn_bid or 0) + (bn_ask or 0)) / 2 or 1.0
            sz = abs(bn_equity * bn_lev * equity_ratio / mid - bn_contracts)
            websea_sz = sz
            bn_sz = sz
            print(f"跟单量 {coin}: leader_ratio={equity_ratio:.6f} {sz=:.6f}")

        if leader_add == buy_address:
            websea_side, bn_side = "buy", "sell"
            print(
                f"leader买 {coin=} side={websea_side} | "
                f"websea买 {coin=} side={websea_side} {websea_sz=} | "
                f"binance卖 {coin=} side={bn_side} {bn_sz=} | "
                f"bn bid={bn_bid} ask={bn_ask} | ws bid={ws_bid} ask={ws_ask}"
            )
            await self.place_websea_order(coin, websea_side, websea_sz, ws_bid, ws_ask, trade)
            await self.place_binance_order(coin, bn_side, bn_sz, bn_bid, bn_ask, trade)
        elif leader_add == sell_address:
            websea_side, bn_side = "sell", "buy"
            print(
                f"leader卖 {coin=} side={websea_side} | "
                f"websea卖 {coin=} side={websea_side} {websea_sz=} | "
                f"binance买 {coin=} side={bn_side} {bn_sz=} | "
                f"bn bid={bn_bid} ask={bn_ask} | ws bid={ws_bid} ask={ws_ask}"
            )
            await self.place_websea_order(coin, websea_side, websea_sz, ws_bid, ws_ask, trade)
            await self.place_binance_order(coin, bn_side, bn_sz, bn_bid, bn_ask, trade)

    def _run_hl_trades(self):
        """HL trades 阻塞订阅；回调投递到 asyncio loop 执行 handle_trade。"""
        def on_trade(trade):
            asyncio.run_coroutine_threadsafe(self.handle_trade(trade), self.loop)

        run_trades(self.tracked_coins, on_trade=on_trade)


def main():
    Strategy().run()


if __name__ == "__main__":
    main()

