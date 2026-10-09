'''
    策略逻辑:
    用 RSI + 布林带做均值回归，并用 ATR 做止损止盈
    使用账户:
    quant_sky_007@126.com
    id:85600496   557501    酉时三刻
    key:53dab96a47bd1021d9dcef3d9fk59654212
    secret:p56gp29wyz5dybrbu430
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
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as Contract  # 普通用户接口
# from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from crypto_center.client.rest.okex import contract as okx_rest

from utils.aio_redis import MyAioredis, MyAioredisFunctools

strategy_name = "酉时三刻(557501)带单策略"
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
        self.ema_fast=8
        self.ema_slow=21
        self.don_len=20
        self.vol_len=20
        self.atr_len=14
        self.max_len = max(self.ema_fast, self.ema_slow, self.don_len, self.vol_len, self.atr_len) + 2
                
    def _load_config(self):
        self.rest = Contract('53dab96a47bd1021d9dcef3d9fk59654212', 'p56gp29wyz5dybrbu430')
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
    
    async def sub_symbols(self):
        for s in self.symbols:
            await self.redis_conn.sub_channel(f"contract.kline.1m.{s}.binance")
            await self.redis_conn.sub_channel(f"contract.bids_asks.{s}.websea")
        
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
        
    def calc_indicators(
        self,
        symbol, 
        klines: list,
        ema_fast=8,
        ema_slow=21,
        don_len=20,
        vol_len=20,
        atr_len=14
    ):
        # ===== 参数安全 =====
        ema_fast = int(ema_fast)
        ema_slow = int(ema_slow)
        don_len = int(don_len)
        vol_len = int(vol_len)
        atr_len = int(atr_len)

        if len(klines) < max(ema_slow, don_len, vol_len, atr_len) + 2:
            raise ValueError("K线数量不足")

        df = pd.DataFrame(klines)
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")

        close = df["close"]
        high = df["high"]
        low = df["low"]
        volume = df["volume"]

        # ===== EMA =====
        df["ema_fast"] = close.ewm(span=ema_fast, adjust=False).mean()
        df["ema_slow"] = close.ewm(span=ema_slow, adjust=False).mean()

        # ===== Donchian Channel =====
        df["don_high"] = high.rolling(don_len).max()
        df["don_low"] = low.rolling(don_len).min()
        df["don_mid"] = (df["don_high"] + df["don_low"]) / 2

        # ===== Volume Filter =====
        df["vol_ma"] = volume.rolling(vol_len).mean()
        df["vol_ratio"] = volume / df["vol_ma"]

        # ===== ATR =====
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        df["atr"] = tr.ewm(alpha=1 / atr_len, adjust=False).mean()

        last = df.iloc[-1]

        # ===== 信号 =====
        buy_signal = (
            last["ema_fast"] > last["ema_slow"]
            and last["close"] > last["don_high"]
            and last["vol_ratio"] > 1.3
        )

        sell_signal = (
            last["ema_fast"] < last["ema_slow"]
            or last["close"] < last["don_mid"]
        )

        return {
            "symbol": symbol,
            "ts": last["ts"],
            "close": float(last["close"]),

            "ema_fast": float(last["ema_fast"]),
            "ema_slow": float(last["ema_slow"]),
            "don_high": float(last["don_high"]),
            "don_mid": float(last["don_mid"]),
            "don_low": float(last["don_low"]),
            "vol_ratio": float(last["vol_ratio"]),
            "atr": float(last["atr"]),

            "buy_signal": bool(buy_signal),
            "sell_signal": bool(sell_signal),

            "stop_loss": float(last["close"] - 1.5 * last["atr"]),
            "take_profit": float(last["close"] + 3.0 * last["atr"]),
        }
        
    # 对冲
    async def main(self, symbol):
        # 判断当前时间
        # now = datetime.datetime.now()
        # if now.minute % TIME_INTERVAL != 0:
        #     return
        if TIME_INTERVAL != 1:
            kline_data = []
            for i in range(0, len(self.kline_data[symbol]), TIME_INTERVAL):
                kline_data.append({
                    "open": self.kline_data[symbol][i]["open"],
                    "high": max([x["high"] for x in self.kline_data[symbol][i:i+TIME_INTERVAL]]),
                    "low": min([x["low"] for x in self.kline_data[symbol][i:i+TIME_INTERVAL]]),
                    "close": self.kline_data[symbol][i+TIME_INTERVAL-1]["close"],
                    "volume": sum([x["volume"] for x in self.kline_data[symbol][i:i+TIME_INTERVAL]]),
                    "ts": self.kline_data[symbol][i]["ts"]
                })
        else:
            kline_data = sorted(self.kline_data[symbol], key=lambda x: x['ts'])
        print(f"{symbol} k线更新,判断交易机会,k线数据量:{len(kline_data)}")
        res = self.calc_indicators(symbol, kline_data)
        print(f"指标信号:{res}")
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
            elif self.pos_dict.get(symbol, [0, 0])[0]> 0:  # 平仓
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




