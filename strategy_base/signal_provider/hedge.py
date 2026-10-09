'''
    策略逻辑:
    用 RSI + 布林带做均值回归，并用 ATR 做止损止盈
    使用账户:
    quant_sky_006@126.com
    id:97644531   557479    zero量化
    key:23cb167617bb8398ccfcf74506t64669304
    secret:9mowefaye9nhyxf2w7zq
'''

import sys
import time
import datetime
import traceback
import asyncio
import pandas as pd
import numpy as np
sys.path.append('../..')
from utils.ToolBoxNew import ToolBox as tb
import objects.contract_request.websea as ocw
from utils.redis_cluster_shard import RedisClusterShard
from config.redis_cfg import RedisConfigClusterAwsPro
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as Contract  # 普通用户接口
# from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from crypto_center.client.rest.okex import contract as okx_rest

from utils.aio_redis import MyAioredis, MyAioredisFunctools

strategy_name = "zero量化(557479)带单策略"
TIME_INTERVAL = 5  # 策略判断周期


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config()  # 读取配置
        self.symbols = ['BTC-USDT','ETH-USDT','SOL-USDT','BNB-USDT']
        self.bid_ask_dict = {}
        self.split = 0.002               # 下单滑点
        self.deal_amt = 1000             # 每次下单金额
        self.stop_ratio = 0.02
        self.stop_amt = self.deal_amt*self.stop_ratio
        self.symbol_precision_dict = {}  # 币种精度
        self.kline_data = {}             # 保存K线数据
        self.symbol_unit_size = {}       # 合约单位
        self.rsi_len=14
        self.bb_len=20
        self.bb_std=2
        self.atr_len=14
        self.max_len = max(self.rsi_len, self.bb_len, self.bb_std, self.atr_len) + 2
        
    def _load_config(self):
        self.rest = Contract('23cb167617bb8398ccfcf74506t64669304', '9mowefaye9nhyxf2w7zq')
        # self.rest = Contract('5bae7319086ca6cdc135803a5426da33','allhrtvujrbrjdvbgf73')  # 测试环境
        # self.token_rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        self.okx_rest = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        self.rest.DEBUG = False
        self.okx_rest.DEBUG = False

    async def on_first(self):
        task = tb()
        self.redis_pool = MyAioredis(db=1)
        self.redis_conn = await self.redis_pool.open()
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(1)
        # await self.redis_conn.sub_channel(f"contract.bids_asks.{self.symbol}.websea")   # TODO 没有数据推送
        # await self.redis_conn.sub_channel(f"contract.bids_asks.{self.symbol}.binance")
        await self.symbol_precision()   # 获取币种精度
        await self.get_kline()    # 获取初始K线数据
        await self.sub_symbols()  # 订阅k线数据
        await self.risk()         # 风控获取当前持仓
        
    async def on_timer(self):
        print("=======timer======")
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))   # 风控
    
    async def listen(self, channel: str, callback):
        redis = RedisClusterShard(RedisConfigClusterAwsPro.HOST, RedisConfigClusterAwsPro.PORT, RedisConfigClusterAwsPro.PASSWORD)
        await redis.subscribe(channel=channel, callback=callback)
    
    async def sub_symbols(self):
        for s in self.symbols:
            await self.redis_conn.sub_channel(f"contract.kline.1m.{s}.binance")
            # await self.redis_conn.sub_channel(f"contract.bids_asks.{s}.websea")
        
    async def on_message(self, channel: str, item: dict):
        # print(f"redis推送:{channel}, {item}")
        # contract.kline.1m.ETH-USDT.binance, {'exchange': 'binance', 'symbol': 'ETH-USDT', 
        # 'open': 3017.59, 'high': 3017.6, 'low': 3013.53, 'close': 3013.57, 'volume': 1480.456, 
        # 'timestamp': 1769635440000, 'messageType': 'message', 'info': {'data': {'E': 1769635500314, 'e': 'kline', 'k': {'B': '0', 'L': 7297378504, 'Q': '597235.63351', 'T': 1769635499999, 'V': '198.129', 'c': '3013.57', 'f': 7297375325, 'h': '3017.60', 'i': '1m', 'l': '3013.53', 'n': 3178, 'o': '3017.59', 'q': '4463307.60435', 's': 'ETHUSDT', 't': 1769635440000, 'v': '1480.456', 'x': True}, 's': 'ETHUSDT'}, 'stream': 'ethusdt@kline_1m'}}
        if channel.startswith("contract.kline") and 'binance' in channel:
            if item['info']['data']['k']['x']:
                kline_dict = {'open': item['open'], 'high': item['high'], 'low': item['low'], 'close': item['close'], 'volume': item['volume'], 'ts': item['timestamp']}
                self.kline_data[item['symbol']].append(kline_dict)
                if len(self.kline_data[item['symbol']]) > 50:
                    self.kline_data[item['symbol']].pop(0)
                await self.main(item['symbol'])
        elif channel.startswith("contract.bids_asks") and 'websea' in channel:
            # {'exchange': 'websea', 'symbol': 'ETH-USDT', 'ask': 3012.67, 'askVolume': 1690, 'bid': 3012.65, 'bidVolume': 1102, 'timestamp': 1769640848759, 'messageType': 'message'}
            self.bid_ask_dict[item['symbol']] = [item['bid'], item['ask']]
    
    # 风控
    async def risk(self):
        # res = await self.rest.fetch_balance()
        # print(f"账户权益:{res}")
        # return
        
        # 当前持仓
        pos_data = await self.rest.get_position()
        # print(f"当前持仓:{pos_data}")
        # [Position(symbol='TRX-USDT', userId=557479, type=2, is_full=1, lever_rate=5, amount=343, 
        # profit=-0.0343, open_price_avg=0.29196, bood=200.28456, avail_amount=343, 
        # contract_frozen=0, settle_rate=None, equity=9390.5337, avail=0.0, bond_rate=2.5, 
        # risk_rate=None, liquidation_price='0.34860', un_profit=-0.0343, open_time=1769753100, 
        # mark_price=None, origData=None)]
        pos_dict = {}
        for i in pos_data:
            amount = i.amount if i.type == 1 else -i.amount
            try:
                pos_dict[i.symbol][0] += amount*self.symbol_unit_size[i.symbol]
                pos_dict[i.symbol][1] += i.un_profit
            except:
                pos_dict[i.symbol] = [amount*self.symbol_unit_size[i.symbol], i.un_profit]
        self.pos_dict = pos_dict
        print(f"当前持仓:{pos_dict}")

        for symbol, pos in pos_dict.items():
            if symbol not in self.symbols:
                continue
            bid_ask = await self.rest.get_depth(symbol, limit=1)
            bid = bid_ask.bids[0].price
            ask = bid_ask.asks[0].price
            if pos[1] < -self.stop_amt:
                if pos[0] > 0:
                    price = round(bid*(1-self.split), int(self.symbol_precision_dict[symbol][0]))
                    await self.make_order(symbol, 'sell', price, pos[0], True)
                elif pos[0] < 0:
                    price = round(ask*(1+self.split), int(self.symbol_precision_dict[symbol][0]))
                    await self.make_order(symbol, 'buy', price, pos[0], True)
            elif pos[1] > self.stop_amt:
                if pos[0] < 0:
                    price = round(ask*(1+self.split), int(self.symbol_precision_dict[symbol][0]))
                    await self.make_order(symbol, 'buy', price, pos[0], True)
                elif pos[0] > 0:
                    price = round(bid*(1-self.split), int(self.symbol_precision_dict[symbol][0]))
                    await self.make_order(symbol, 'sell', price, pos[0], True)
        
    # 获取币种精度
    async def symbol_precision(self):
        res = await self.rest.get_symbols()
        # print(f"币种信息:{res}")
        for i in res:
            self.symbol_unit_size[i.symbol] = i.contract_size
        res = await self.rest.get_precision()
        # print(f"币种精度:{res}")
        for symbol, data in res.items():
            self.symbol_precision_dict[symbol] = [data.price, data.amount]
        print(f"币种精度:{self.symbol_precision_dict}")
    
    # 获取K线数据
    async def get_kline(self):
        for s in self.symbols:
            max_len = min(300, self.max_len*TIME_INTERVAL)
            res = await self.okx_rest.fetch_kline(s, "1m", limit=max_len)
            kline_dict = {}
            for i in res:
                kline_dict = {'open': i['open'], 'high': i['high'], 'low': i['low'], 'close': i['close'], 'volume': i['volume'], 'ts': i['timestamp']}
                if s in self.kline_data:
                    self.kline_data[s].append(kline_dict)
                else:
                    self.kline_data[s] = [kline_dict]
        # print(f"OKX的K线数据:{self.kline_data}")
        
    def calc_indicators(
        self, symbol,
        klines: list,
        rsi_len=14,
        bb_len=20,
        bb_std=2,
        atr_len=14
    ):
        if len(klines) < max(rsi_len, bb_len, atr_len) + 2:
            raise ValueError("K线数量不足")

        df = pd.DataFrame(klines)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")

        close = df["close"]
        high = df["high"]
        low = df["low"]

        # ===== RSI =====
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1/rsi_len, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/rsi_len, adjust=False).mean()

        rs = avg_gain / avg_loss
        df["rsi"] = 100 - (100 / (1 + rs))

        # ===== Bollinger Bands =====
        ma = close.rolling(bb_len).mean()
        std = close.rolling(bb_len).std()

        df["bb_mid"] = ma
        df["bb_upper"] = ma + bb_std * std
        df["bb_lower"] = ma - bb_std * std

        # ===== ATR =====
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)

        df["atr"] = tr.ewm(alpha=1/atr_len, adjust=False).mean()

        last = df.iloc[-1]

        buy_signal = last["close"] < last["bb_lower"] and last["rsi"] < 35
        sell_signal = last["close"] > last["bb_mid"] or last["rsi"] > 55

        return {
            "symbol": symbol,
            "ts": last["ts"],
            "close": float(last["close"]),

            "rsi": round(float(last["rsi"]), 2),
            "bb_upper": round(float(last["bb_upper"]), 2),
            "bb_mid": round(float(last["bb_mid"]), 2),
            "bb_lower": round(float(last["bb_lower"]), 2),
            "atr": round(float(last["atr"]), 2),

            "buy_signal": bool(buy_signal),
            "sell_signal": bool(sell_signal),

            "stop_loss": round(float(last["close"] - 1.2 * last["atr"]), 4),
            "take_profit": round(float(last["close"] + 1.8 * last["atr"]), 4),
        }
        
    # 对冲
    async def main(self, symbol):
        # 把self.kline_data数据聚合成10分钟K线
        print(f"主程序,时间间隔:{TIME_INTERVAL}")
        # print(len(self.kline_data[symbol]))
        kline_data = sorted(self.kline_data[symbol], key=lambda x: x['ts'])
        usable_len = (len(kline_data) // TIME_INTERVAL) * TIME_INTERVAL
        kline_data = kline_data[-usable_len:]
        if TIME_INTERVAL != 1:
            # 判断当前时间
            now = datetime.datetime.now()
            if now.minute % TIME_INTERVAL != 0:
                return
            kline_data2 = []
            for i in range(0, len(kline_data), TIME_INTERVAL):
                kline_data2.append({
                    "open": kline_data[i]["open"],
                    "high": max([x["high"] for x in kline_data[i:i+TIME_INTERVAL]]),
                    "low": min([x["low"] for x in kline_data[i:i+TIME_INTERVAL]]),
                    "close": kline_data[i+TIME_INTERVAL-1]["close"],
                    "volume": sum([x["volume"] for x in kline_data[i:i+TIME_INTERVAL]]),
                    "ts": kline_data[i]["ts"]
                })
        # else:
        #     kline_data = sorted(self.kline_data[symbol], key=lambda x: x['ts'])
        # kline_data = sorted(self.kline_data[symbol], key=lambda x: x['ts'])
        kline_data = kline_data2
        print(f"{symbol} k线更新,判断交易机会,k线数据量:{len(kline_data)}")
        res = self.calc_indicators(symbol, kline_data)
        print(f"指标信号:{res}")
        # return
        bid_ask = await self.rest.get_depth(symbol, limit=1)
        bid = bid_ask.bids[0].price
        ask = bid_ask.asks[0].price
        self.bid_ask_dict[symbol] = [bid, ask]
        # 交易逻辑
        if res['buy_signal']:
            if self.pos_dict.get(symbol, [0, 0])[0] == 0:
                price = round(self.bid_ask_dict[symbol][1]*(1+self.split), 
                              int(self.symbol_precision_dict[symbol][0]))
                vol = round(self.deal_amt/price, int(self.symbol_precision_dict[symbol][1]))
                print(f"买入信号:{symbol}, {price}, {vol}, buy, 开仓")
                await self.make_order(symbol, 'buy', price, vol)
            elif self.pos_dict.get(symbol, [0, 0])[0] < 0:  # 平仓
                price = round(self.bid_ask_dict[symbol][1]*(1+self.split), 
                              int(self.symbol_precision_dict[symbol][0]))
                vol = round(abs(self.pos_dict[symbol][0]), int(self.symbol_precision_dict[symbol][1]))
                # vol = round(self.deal_amt/price, int(self.symbol_precision_dict[symbol][1]))
                print(f"买入信号:{symbol}, {price}, {vol}, buy, 平仓")
                await self.make_order(symbol, 'buy', price, vol, True)
        elif res['sell_signal'] and self.pos_dict.get(symbol, [0, 0])[0] >= 0:
            if self.pos_dict.get(symbol, [0, 0])[0] == 0:
                price = round(self.bid_ask_dict[symbol][0]*(1-self.split), 
                              int(self.symbol_precision_dict[symbol][0]))
                vol = round(self.deal_amt/price, int(self.symbol_precision_dict[symbol][1]))
                print(f"卖出信号:{symbol}, {price}, {vol}, sell, 开仓")
                await self.make_order(symbol, 'sell', price, vol)
            elif self.pos_dict.get(symbol, [0, 0])[0] > 0:  # 平仓
                price = round(self.bid_ask_dict[symbol][0]*(1-self.split), 
                              int(self.symbol_precision_dict[symbol][0]))
                vol = round(abs(self.pos_dict[symbol][0]), int(self.symbol_precision_dict[symbol][1]))
                # vol = round(self.deal_amt/price, int(self.symbol_precision_dict[symbol][1]))
                print(f"卖出信号:{symbol}, {price}, {vol}, sell, 平仓")
                await self.make_order(symbol,'sell', price, vol, True)
                
    # 下单
    async def make_order(self, symbol, side, price, vol, reduce_only=False):
        # print(f"下单:{symbol}, {side}, {price}, {vol}, {reduce_only}")
        # return
        try:
            side = ocw.OrderType.buy_limit if side == 'buy' else ocw.OrderType.sell_limit
            contract_type = 'close' if reduce_only else 'open'
            vol = int(vol/self.symbol_unit_size[symbol])
            t1 = time.time()*1000
            result = await self.rest.order_create(symbol, side, 
                                                  price=price, amount=abs(vol), is_full=1,
                                                  contract_type=contract_type)
            deal_mess = f"{symbol} 价格:{price} 数量:{vol} 方向:{side} 下单延时:{int(time.time()*1000-t1)}ms 回报:{result}"
            print(deal_mess)
        except:
            title = f"{strategy_name} 下单失败"
            warning_mess = f"{title} {symbol} 价格:{price} 数量:{vol} 方向:{side} 失败:{traceback.format_exc()}"
            # await self.send_warning(title, warning_mess)
            print(f"下单报错:{warning_mess}")
        

def main():
    Strategy().run()

if __name__ == '__main__':
    main()


