#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
并且由于主程序使用了守护进程,所以任务对象不可使用多进程.
必须实现__del__方法用于释放资源与锁.
必须将任务对象添加到task文件夹下,必须修改__init__.py导入任务对象.

可以选择使用libs.config中的配置信息.如果需要在不停止项目的情况下修改配置.
请在任务对象内部重写配置信息.

行情逻辑：基于逐笔成交的「连续同向成交 + 价格推进」识别买/卖冲击（breaked_buy_num / breaked_sell_num），
在回撤（set back）时写诊断 CSV，并在满足强度条件时通过 nav_trade 发单。

实盘仅记录（不下单）：同目录下 run_live.py，Binance 公共 WS + on_trade_signal，对手盘一档价写入 logs/paper_live/。
盘口过滤版：run_live_orderbook_filtered.py，子类覆盖 _allow_buy_break_count / _allow_sell_break_count。
"""
#  #########################内置包##########################
import argparse
import os
import time
import datetime
import csv
import warnings

import requests

warnings.filterwarnings("ignore")
#  #########################自定义（回测可延迟加载 ws / nav_trade / redis）##########################
try:
    from libs.log import logger
except ImportError:  # pragma: no cover
    import logging
    import sys
    logger = logging.getLogger("signals")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                            format="%(asctime)s %(levelname)s %(message)s")

# 现货公开行情 REST（与官方文档一致）：https://binance-docs.github.io/apidocs/spot/en/
BINANCE_SPOT_REST = "https://api.binance.com"
SIGNAL_CSV_HEADER = [
    "trade_id", "timestamp", "price", "size", "side", "breaking price",
    "1st_break_data_timestamp", "extreme order timestamp", "1st_set_back_timestamp", "1st_step_back_timestamp",
    "1st_break_price", "extreme order price 1", "extreme order price 2", "1st_set_back_price",
    "1st_step_back_price", "signal_price",
    "signal_side", "last_signal_side", "m", "set_back_spread_to_extreme", "set_back_spread_to_break",
    "shock_spread_to_breaking,", "last_bid_ask_spread", "estimate_bid_ask_spread", "signal_strength",
]


def _default_signal_log_dir():
    base = os.environ.get("SIGNAL_LOG_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "step_back")
    return os.path.expanduser(base)


def _symbol_to_binance_rest(symbol_ccxt):
    """BTC-USDT -> BTCUSDT"""
    return symbol_ccxt.replace("/", "").replace("-", "")


def _ccxt_our_symbol(symbol_ccxt):
    """BTC-USDT、BTCUSDT -> BTC/USDT"""
    s = symbol_ccxt.replace("/", "-")
    if "-" in s:
        a, b = s.split("-", 1)
        return f"{a}/{b}"
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}/USDT"
    return symbol_ccxt


def _trade_timestamp_ms(msg):
    """统一为毫秒时间戳：优先 Binance 原生字段 T。"""
    info = msg.get("info") or {}
    if isinstance(info, dict) and info.get("T") is not None:
        return int(info["T"])
    ts = msg.get("timestamp")
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, str):
        if ts[:1].isdigit():
            return int(float(ts))
        try:
            return int(datetime.datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    return int(time.time() * 1000)


class Task():
    SYMBOLS = ["BTC-USDT"]

    def __init__(
        self,
        signal_log_dir=None,
        backtest=False,
        on_trade_signal=None,
        strict_trade_id=True,
        write_signal_csv=True,
    ):
        self.signal_log_dir = signal_log_dir or _default_signal_log_dir()
        self._backtest = backtest
        self.on_trade_signal = on_trade_signal
        self.strict_trade_id = strict_trade_id
        self.write_signal_csv = write_signal_csv
        self.exchange = 'binance'
        self.last_side = None
        self.continued_buy = 0
        self.continued_sell = 0
        self.breaked_buy = 0
        self.breaked_sell = 0
        self.last_breaked_buy = 0
        self.last_breaked_sell = 0
        self.last_price = 0
        self.breaking_price = 0
        self.breaked_price = 0
        self.breaked_buy_num = 0
        self.breaked_sell_num = 0
        self.last_trade_id = 0
        self.status_list = []
        self.status = 0
        self.last_status = 0
        self.continued_hit = ''
        self.last_buy_hit = 0
        self.last_sell_hit = 0
        self.is_buy_hit = 0
        self.is_sell_hit = 0
        self.signal_side = ''
        self.last_signal_side = ''
        self.last_buy_price = 0
        self.last_sell_price = 0
        self.is_huicai = False
        self.is_setback = False
        self.first_break_data_timestamp = ''
        self.first_break_price = 0
        self.first_step_back_timestamp = ''
        self.first_step_back_price = 0
        self.first_set_back_timestamp = ''
        self.first_set_back_price = 0
        self.extreme_order_timestamp = ''
        self.extreme_order_price1 = 0
        self.extreme_order_price2 = 0
        self.last_timestamp = ''
        self.last_hitting_num = 0
        self.m_lag1 = 0
        self.m_lag2 = 0
        self.m_lag3 = 0
        self.estimate_bid_ask_spread = 1
        self.shock_spread_to_breaking = 0
        self.signal_strength = 0
        if not backtest:
            from libs.redis_manager import redis_client
            from nav_trade import Task as Trade_task
            self.redis = redis_client
            self.trade_task = Trade_task(init=False)
            self.trade_obj = Trade_task(init=False)
        else:
            self.redis = None
            self.trade_task = None
            self.trade_obj = None
        self.last_breaked_timestamp = ''
        os.makedirs(self.signal_log_dir, exist_ok=True)

    def _allow_buy_break_count(self, msg, price, last_price) -> bool:
        """子类可覆盖：是否允许本笔计入 breaked_buy_num（默认不过滤）。"""
        return True

    def _allow_sell_break_count(self, msg, price, last_price) -> bool:
        """子类可覆盖：是否允许本笔计入 breaked_sell_num（默认不过滤）。"""
        return True

    def _signal_csv_path(self, day_yyyymmdd):
        return os.path.join(self.signal_log_dir, f"signals_{day_yyyymmdd}.csv")

    def _append_signal_row(self, row, timestamp_ms):
        day = datetime.datetime.utcfromtimestamp(timestamp_ms / 1000.0).strftime('%Y%m%d')
        path = self._signal_csv_path(day)
        new_file = not os.path.exists(path)
        with open(path, 'a+', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(SIGNAL_CSV_HEADER)
            w.writerow(row)

    def on_ws_message(self, msg):
        """WebSocket 包装结构：message_type + data 列表。"""
        if not msg or msg.get("message_type", "").endswith(".close"):
            return
        rows = msg.get("data") or []
        for row in rows:
            if isinstance(row, dict) and "trade_id" in row:
                self.save_data(row)

    def run(self) -> None:
        from libs.exchange.ws_exchanges import BinanceWsExchange as ExChange
        from libs.exchange.ws_exchanges.base.WsExchange import MarketType
        if self.trade_obj is None:
            from libs.redis_manager import redis_client
            from nav_trade import Task as Trade_task
            self.redis = redis_client
            self.trade_task = Trade_task(init=False)
            self.trade_obj = Trade_task(init=False)
        try:
            ws = ExChange(
                message_callback=self.on_ws_message,
                market=MarketType.SPOT.value,
                instrument_id=self.SYMBOLS[0],
            )
            open_callback = self.get_public_open_callback(ws, self.SYMBOLS)
            ws.add_open_callback(open_callback)
            ws.connect()
        except Exception as e:
            logger.error(e)

        while True:
            time.sleep(1000)

    def save_data(self, msg):
        """逐笔成交回调：msg 为扁平字段（与 parse_trade 的 data 元素一致）。"""
        try:
            tradeid = int(msg['trade_id'])
            timestamp = _trade_timestamp_ms(msg)

            if self.last_trade_id and tradeid != self.last_trade_id + 1:
                logger.warning('miss tradeid expect=%s got=%s', self.last_trade_id + 1, tradeid)
                self.last_trade_id = tradeid
                if self.strict_trade_id:
                    return

            price = float(msg['price'])
            inst_key = msg.get('instrument_id') or msg.get('symbol') or self.SYMBOLS[0]
            if self.redis is not None:
                try:
                    self.redis.hset(f'{self.exchange}:spot:trade', inst_key, price)
                except Exception as e:
                    logger.error('redis hset: %s', e)

            side = msg['side']
            if side == 'buy':
                self.last_buy_price = price
            elif side == 'sell':
                self.last_sell_price = price
            if side == 'buy' and self.last_side == 'buy':
                self.continued_buy = 1
                self.continued_sell = 0
                if price > self.last_price:
                    self.continued_hit = 'buy'
                    self.breaked_sell_num = 0
                    self.breaked_buy = 1
                    self.is_buy_hit = 1
                    if self.last_buy_hit == 0:
                        if self._allow_buy_break_count(msg, price, self.last_price):
                            self.breaked_buy_num = self.breaked_buy_num + 1
                            self.breaked_price = price
                            self.breaking_price = self.last_price
                            if len(self.status_list) < 20:
                                self.status_list.append(1)
                            else:
                                self.status_list.pop(0)
                                self.status_list.append(1)
                elif price < self.last_price:
                    self.is_buy_hit = 0
                    self.breaked_buy = 0

            elif side == 'sell' and self.last_side == 'buy':
                if self.is_buy_hit == 1:
                    self.is_buy_hit = 0
                self.breaked_buy = 0
            elif side == 'sell' and self.last_side == 'sell':
                self.continued_sell = 1
                self.continued_buy = 0
                if price < self.last_price:
                    self.continued_hit = 'sell'
                    self.breaked_buy_num = 0
                    self.breaked_sell = 1
                    self.is_sell_hit = 1
                    if self.last_sell_hit == 0:
                        if self._allow_sell_break_count(msg, price, self.last_price):
                            self.breaked_sell_num = self.breaked_sell_num + 1
                            self.breaked_price = price
                            self.breaking_price = self.last_price
                            if len(self.status_list) < 20:
                                self.status_list.append(-1)
                            else:
                                self.status_list.pop(0)
                                self.status_list.append(-1)
                elif price > self.last_price:
                    self.is_sell_hit = 0
                    self.breaked_sell = 0
            elif side == 'buy' and self.last_side == 'sell':
                if self.is_sell_hit == 1:
                    self.is_sell_hit = 0
                self.breaked_sell = 0
            else:
                self.continued_buy = 0
                self.continued_sell = 0
                self.breaked_buy = 0
                self.breaked_sell = 0

            if self.trade_obj is not None:
                self.trade_obj.need_cancel = True
            if self.breaked_buy_num > 0:
                if self.is_huicai is False and side == 'sell':
                    self.is_huicai = True
                    self.first_step_back_timestamp = timestamp
                    self.first_step_back_price = price
                if self.is_setback is False:
                    if price < self.last_price or side == 'sell':
                        self.is_setback = True
                        self.first_set_back_timestamp = timestamp
                        self.first_set_back_price = price
                        self.extreme_order_timestamp = self.last_timestamp
                        self.extreme_order_price1 = self.last_price
                        self.extreme_order_price2 = max(self.extreme_order_price1, self.first_set_back_price)
                        signalside = 1
                        set_back_spread_to_extreme = signalside * (self.extreme_order_price1 - self.first_set_back_price)
                        set_back_spread_to_break = signalside * (self.breaked_price - self.first_set_back_price)
                        self.shock_spread_to_breaking = (self.extreme_order_price2 - self.breaking_price) * signalside
                        last_bid_ask_spread = self.estimate_bid_ask_spread
                        if side == 'sell' and self.first_step_back_timestamp != '' and set_back_spread_to_extreme > 0:
                            self.estimate_bid_ask_spread = self.estimate_bid_ask_spread * 0.95 + set_back_spread_to_extreme * 0.05

                        if self.write_signal_csv:
                            self._append_signal_row([
                                tradeid, timestamp, price, msg['size'], side, self.breaking_price,
                                self.first_break_data_timestamp, self.extreme_order_timestamp, self.first_set_back_timestamp, self.first_step_back_timestamp,
                                self.first_break_price, self.extreme_order_price1, self.extreme_order_price2, self.first_set_back_price, self.first_step_back_price, self.signal_price,
                                signalside, self.last_signal_side, self.breaked_buy_num,
                                set_back_spread_to_extreme, set_back_spread_to_break, self.shock_spread_to_breaking, last_bid_ask_spread, self.estimate_bid_ask_spread, self.signal_strength,
                            ], timestamp)
                        self.last_signal_side = signalside
            elif self.breaked_sell_num > 0:
                if self.is_huicai is False and side == 'buy':
                    self.is_huicai = True
                    self.first_step_back_timestamp = timestamp
                    self.first_step_back_price = price
                if self.is_setback is False:
                    if price > self.last_price or side == 'buy':
                        self.is_setback = True
                        self.first_set_back_timestamp = timestamp
                        self.first_set_back_price = price
                        self.extreme_order_timestamp = self.last_timestamp
                        self.extreme_order_price1 = self.last_price
                        self.extreme_order_price2 = min(self.extreme_order_price1, self.first_set_back_price)
                        signalside = -1
                        set_back_spread_to_extreme = signalside * (self.extreme_order_price1 - self.first_set_back_price)
                        set_back_spread_to_break = signalside * (self.breaked_price - self.first_set_back_price)
                        self.shock_spread_to_breaking = (self.extreme_order_price2 - self.breaking_price) * signalside
                        last_bid_ask_spread = self.estimate_bid_ask_spread
                        if side == 'buy' and self.first_step_back_timestamp != '' and set_back_spread_to_extreme > 0:
                            self.estimate_bid_ask_spread = self.estimate_bid_ask_spread * 0.95 + set_back_spread_to_extreme * 0.05

                        if self.write_signal_csv:
                            self._append_signal_row([
                                tradeid, timestamp, price, msg['size'], side, self.breaking_price,
                                self.first_break_data_timestamp, self.extreme_order_timestamp, self.first_set_back_timestamp, self.first_step_back_timestamp,
                                self.first_break_price, self.extreme_order_price1, self.extreme_order_price2, self.first_set_back_price, self.first_step_back_price, self.signal_price,
                                signalside, self.last_signal_side, self.breaked_sell_num,
                                set_back_spread_to_extreme, set_back_spread_to_break, self.shock_spread_to_breaking, last_bid_ask_spread, self.estimate_bid_ask_spread, self.signal_strength,
                            ], timestamp)
                        self.last_signal_side = signalside
            if self.last_buy_hit == 0 and self.is_buy_hit:
                self.first_break_data_timestamp = timestamp
                self.first_break_price = price
                self.is_huicai = False
                self.is_setback = False
                self.first_step_back_timestamp = ''
                self.first_step_back_price = ''
                m = self.breaked_buy_num

                self.signal_price = self.breaking_price - 1.3 * max(self.estimate_bid_ask_spread, 0.7)
                if m > 1:
                    if self.trade_obj:
                        self.trade_obj.need_cancel = False
                    if not self._backtest and self.trade_obj:
                        self.trade_obj.signal_to_cancel("buy", self.last_breaked_timestamp)
                    self.last_breaked_timestamp = timestamp

                if m > 1 and m <= 5 and self.shock_spread_to_breaking > 0.1 * self.estimate_bid_ask_spread:
                    self.signal_strength = 1
                    record_dict = {
                        'trade_id': tradeid,
                        'timestamp': timestamp,
                        'signal_price': self.signal_price,
                        'signal_side': 1,
                        'm': m,
                        'shock_spread_to_breaking': self.shock_spread_to_breaking,
                        'estimate_bid_ask_spread': self.estimate_bid_ask_spread,
                        'signal_strength': self.signal_strength
                    }
                    if self.on_trade_signal is not None:
                        self.on_trade_signal(record_dict)
                    elif not self._backtest and self.trade_obj is not None:
                        self.trade_obj.signal_to_trade(record_dict)

            elif self.last_sell_hit == 0 and self.is_sell_hit:
                self.first_break_data_timestamp = timestamp
                self.first_break_price = price
                self.is_huicai = False
                self.is_setback = False
                self.first_step_back_timestamp = ''
                self.first_step_back_price = ''
                m = self.breaked_sell_num
                self.signal_strength = min(self.shock_spread_to_breaking, 1)
                self.signal_price = self.breaking_price + 1.3 * max(self.estimate_bid_ask_spread, 0.7)
                if m > 1:
                    if self.trade_obj:
                        self.trade_obj.need_cancel = False
                    if not self._backtest and self.trade_obj:
                        self.trade_obj.signal_to_cancel("sell", self.last_breaked_timestamp)
                    self.last_breaked_timestamp = timestamp
                if m > 1 and m <= 5 and self.shock_spread_to_breaking > 0.1 * self.estimate_bid_ask_spread:
                    self.signal_strength = 1
                    record_dict = {
                        'trade_id': tradeid,
                        'timestamp': timestamp,
                        'signal_price': self.signal_price,
                        'signal_side': -1,
                        'm': m,
                        'shock_spread_to_breaking': self.shock_spread_to_breaking,
                        'estimate_bid_ask_spread': self.estimate_bid_ask_spread,
                        'signal_strength': self.signal_strength
                    }
                    if self.on_trade_signal is not None:
                        self.on_trade_signal(record_dict)
                    elif not self._backtest and self.trade_obj is not None:
                        self.trade_obj.signal_to_trade(record_dict)

            else:
                self.signal_side = ''
                self.signal_price = ''

            self.last_side = side
            self.last_price = price
            self.last_breaked_buy = self.breaked_buy
            self.last_breaked_sell = self.breaked_sell
            self.last_trade_id = tradeid
            self.last_buy_hit = self.is_buy_hit
            self.last_sell_hit = self.is_sell_hit
            self.last_timestamp = timestamp

        except Exception as e:
            logger.error(e)

    def run_backtest(
        self,
        symbol=None,
        start_time_ms=None,
        end_time_ms=None,
        max_rows=None,
        sleep_sec=0.05,
    ):
        """
        使用现货 REST GET /api/v3/aggTrades 拉取聚合成交并按时间顺序回放（公开接口，无需 API Key）。
        与 WS 单笔成交相比为聚合口径，trade_id 使用聚合成交 ID `a`，在同一 symbol 内递增。
        文档：https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
        """
        self._backtest = True
        sym = symbol or self.SYMBOLS[0]
        rest_sym = _symbol_to_binance_rest(sym)
        url = f"{BINANCE_SPOT_REST}/api/v3/aggTrades"
        last_agg_id = None
        count = 0

        while True:
            p = {"symbol": rest_sym, "limit": 1000}
            if last_agg_id is not None:
                p["fromId"] = last_agg_id + 1
            else:
                if start_time_ms is not None:
                    p["startTime"] = int(start_time_ms)
                if end_time_ms is not None:
                    p["endTime"] = int(end_time_ms)
            r = requests.get(url, params=p, timeout=30)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            for row in batch:
                norm = {
                    "trade_id": row["a"],
                    "price": row["p"],
                    "size": row["q"],
                    "side": "sell" if row["m"] else "buy",
                    "timestamp": int(row["T"]),
                    "instrument_id": _ccxt_our_symbol(sym),
                    "info": row,
                }
                self.save_data(norm)
                count += 1
                if max_rows and count >= max_rows:
                    logger.info("backtest stop: max_rows=%s", max_rows)
                    return
                last_agg_id = row["a"]
            if len(batch) < 1000:
                break
            time.sleep(sleep_sec)

        logger.info("backtest done: rows=%s symbol=%s", count, sym)

    @staticmethod
    def get_public_open_callback(ws, symbols):
        """获取市场公共话题的订阅回调"""

        def public_open_callback():
            ws.fetch_trade_list(symbols)

        return public_open_callback

    def __del__(self):
        """释放资源"""
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="signals: 实盘 WS 或 REST 回放回测")
    parser.add_argument("--mode", choices=("live", "backtest"), default="backtest")
    parser.add_argument("--symbol", default="BTC-USDT")
    parser.add_argument("--start-ms", type=int, default=None, help="回测起始时间戳（毫秒）")
    parser.add_argument("--end-ms", type=int, default=None, help="回测结束时间戳（毫秒）")
    parser.add_argument("--max-rows", type=int, default=5000, help="最多回放条数（调试）")
    parser.add_argument("--signal-log-dir", default=None)
    args = parser.parse_args()
    ok = Task(signal_log_dir=args.signal_log_dir, backtest=(args.mode == "backtest"))
    ok.SYMBOLS = [args.symbol]
    if args.mode == "live":
        ok._backtest = False
        ok.run()
    else:
        ok.run_backtest(
            symbol=args.symbol,
            start_time_ms=args.start_ms,
            end_time_ms=args.end_ms,
            max_rows=args.max_rows,
        )
