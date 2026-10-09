'''
    压盘口策略
'''
import sys
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from typing import Optional, Dict, List
import time
from datetime import datetime
import math
import traceback
import importlib
import asyncio
import pytz
from collections import OrderedDict
from dataclasses import dataclass
import reduce_mm_config
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from utils.some_array import FixedSizeOrderedList
import objects.contract_request.websea as ocw
import objects.contract_request.binance as ocb
from objects.constant import OrderStatus as ost
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
# from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from client.env_pro.rest.binance.u_contract import UBinanceContract as bn_rest
from client.env_pro.wss.binance.u_contract import UBinanceContract as bn_wss
from client.env_dev.rest.websea_contract import WebseaContract as ws_contract_rest    # 测试环境
from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss      # 测试环境
# from client.env_pro.rest.binance import u_contract as bn_rest
# from client.env_pro.wss.binance.u_contract import UBinanceContract as bn_wss
from utils.functool import format_float
sys.path.append("/usr/local/server/wbfAPI/exchange")
from binanceUsdtSwap import DataWss, AccountRest

timezone = pytz.timezone("Asia/Shanghai")


class LimitedSizeDict(OrderedDict):
    def __init__(self, *args, max_size=100, **kwargs):
        self._max_size = max_size
        super().__init__(*args, **kwargs)
        
    def __setitem__(self, key, value):
         # 如果字典已满，则先删除最早的项
        while len(self) >= self._max_size:
            self.popitem(last=False)
        super().__setitem__(key, value)

# noinspection DuplicatedCode
@dataclass
class BpRow:
    side: str                        # 方向
    start_ts: int = -1               # 冲击开始时间
    end_ts: int = -1                 # 冲击结束时间
    breaking_ts: int = -1            # 突破时间
    setback_type: int = 0            # 冲击结束方式
    open_price: float = -1           # 冲击开始价格
    breaking_price: float = -1       # 突破价格
    setback_price: float = -1        # 冲击转向价格
    close_price: float = -1          # 冲击结束价格
    breaking_qty: float = 0          # 突破总量
    cum_shocking_qty: float = 0      # 冲击总量
    cum_chopping_ask_qty: float = 0  # ask震荡总量
    cum_chopping_bid_qty: float = 0  # bid震荡总量

    def to_dict(self):
        return {
            "side": self.side,
            "start_ts": str(self.start_ts),
            "start_time": datetime.fromtimestamp(self.start_ts / 1000, tz=timezone).strftime("%Y-%m-%d %H:%M:%S"),
            "end_ts": str(self.end_ts),
            "end_time": datetime.fromtimestamp(self.end_ts / 1000, tz=timezone).strftime("%Y-%m-%d %H:%M:%S"),
            "breaking_ts": str(self.breaking_ts),
            "breaking_time": datetime.fromtimestamp(self.breaking_ts / 1000, tz=timezone).strftime("%Y-%m-%d %H:%M:%S"),
            "setback_type": str(self.setback_type),
            "open_price": str(self.open_price),
            "breaking_price": str(self.breaking_price),
            "setback_price": str(self.setback_price),
            "close_price": str(self.close_price),
            "breaking_qty": str(self.breaking_qty),
            "cum_shocking_qty": str(self.cum_shocking_qty),
            "cum_chopping_ask_qty": str(self.cum_chopping_ask_qty),
            "cum_chopping_bid_qty": str(self.cum_chopping_bid_qty),
        }


class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        # self.vol_log = tb.Log('vol.log')
        # self.price_log = tb.Log('price.log')
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
    
    async def on_first(self):
        self.log.info("=======first======")
        self.refer_price = 0                # 上次成交价格
        self.hedge_symbol = self.symbol.replace('-','/').lower()
        await self.get_precision()
        await self.hedge_contract_info()
        await self.get_symbol_unit()
        depth = await self.bn_rest.get_depth(self.symbol, limit=5)
        self.bid1_wb = float(depth['bids'][0][0])
        self.ask1_wb = float(depth['asks'][0][0])
        tick = AccountRest('','').getTick(self.hedge_symbol)
        tick = [tick['data'][-1]]
        await self.update_benchmark_price(tick)
        await self.update_kline()
        await self.update_range()
        await self.cancel_orders(tag='开盘撤单')

        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_depth = self.on_depth
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_depth(symbol=self.symbol))

        self.bn_wss = bn_wss()
        self.bn_wss.on_ticker_order = self.on_bid_ask
        self.bn_wss.on_kline = self.on_kline
        self.loop.create_task(self.bn_wss.only_subscribe())
        self.loop.create_task(self.bn_wss.sub_ticker_order(symbol=self.symbol))
        self.loop.create_task(self.bn_wss.sub_kline(self.symbol, '1m'))
        DataWss(self.hedge_symbol, topics=['tick'], rspFunc=self.on_order)
        await self.risk()

    async def on_timer(self):
        # self.log.info("=======timer======")
        self.schedule.add_job(self.update_range, CronTrigger(second="*/1"))  # 每1s执行一次
        self.schedule.add_job(self.check_info, CronTrigger(second="*/2"))    # 每2s执行一次
        self.schedule.add_job(self.check_wss, CronTrigger(second="*/5"))     # 每5s执行一次
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))         # 每10s执行一次
        self.schedule.add_job(self.update_kline, CronTrigger(second="00"))  # 每分钟的0s执行
        self.schedule.add_job(self.reload_config, CronTrigger(second="00"))  # 每分钟的0s执行
            
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rest = ws_contract_rest(config['token'], config['secret'])
        self.bn_rest = bn_rest(config['hedge_token'], config['hedge_secret'])

    def _initParams(self):
        """初始化参数
        """
        self.shock_dodge_last = 0           # 上次盘口后移价格
        self.signal1_last = 0               # 上次信号方向
        self.signal_last = 0
        self.last_current_position = 0      # 上次持仓
        self.hedge_symbol_precision = {}    # 保存binance合约信息
        self.symbols_1000 = []              # 保存binanc名称包含1000的交易对
        self.last_on_order_ts = 0           # 最后一次成交推送时间戳
        self.last_on_kline_ts = 0           # 最后一次kline推送时间戳
        self.last_on_bid_ask_ts = 0         # 最后一次bid_ask推送时间戳
        self.last_on_depth_ts = 0           # 最后一次depth推送时间戳
        self.local_open_buy_list = []       # 本地保存buy委托数据
        self.local_open_sell_list = []      # 本地保存sell委托数据
        self.is_open = True                 # 是否开仓,False表示下平仓单
        self.last_to_buy = {}               # 保存上次下单的买盘数据,防止重复下单
        self.last_to_sell = {}              # 保存上次下单的卖盘数据,防止重复下单
        self.main_clock = False             # 下单循环加锁
        self.buy_shocking = 0               # buy挂单后移
        self.sell_shocking = 0              # sell挂单后移
        
        self.buy_shape = 1
        self.sell_shape = 1
        self.bid_spread = 0                 # 盘口价格往后挪多少
        self.ask_spread = 0                 # 盘口价格往后挪多少
        self.kline_dict = LimitedSizeDict(max_size=70)    # 保存kline
        self.pos_BN = 0                     # binance持仓量
        self.pos_WB = 0                     # websea持仓量
        self.hedge_hurdle = 0.1             # 10%对冲 测试时可以为0
        self.ignore_order_status = {3, 4, 6, ost.AlreadyDeal, ost.Canceling, ost.Canceling} # TODO 346含义？

        # 计算冲击成本
        # depth
        self.ask1: float = -1
        self.bid1: float = -1
        # trade
        self.trades: List[dict] = FixedSizeOrderedList(1000)
        self.bp_rows: List[BpRow] = []
        self.shocking: bool = False             # 是否开始冲击


    async def get_precision(self):
        """更新 币对信息"""
        self.precision = await self.rest.get_precision(self.symbol)   # , quan=True)
        self.min_price_step = 10 ** (-self.precision.price)
        if self.symbol == "BTC-USDT":
            self.min_price_step = 0.1
    
    async def hedge_contract_info(self):
        data = await self.bn_rest.get_precision(self.symbol)
        for k, v in data.items():
            if '1000' in k:
                self.symbols_1000.append(k.lstrip('1000'))
            self.hedge_symbol_precision[k] = [v.price, v.amount]

    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                data = await self.rest.get_symbols(self.symbol)
                # 若走这个逻辑,表示交易对下架
                if not isinstance(data, list):
                    self.symbol_unit = data.contract_size
                    break
                else:
                    raise f'{self.symbol}交易对已下架,获取不到合约单位'
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    async def cancel_orders(self, orders=None, tag=''):
        while True:
            try:
                if orders is None:
                    res = await self.rest.order_cancel(symbol=self.symbol) #, quan=True)
                else:
                    res = await self.rest.order_cancel(order_ids=orders)  #, quan=True) 正式上线取消注释
                #self.log.info(f"{tag} cancel_orders回报:{res}")
                break
            except Exception as e:
                self.log.warning(f"{tag}撤单错误:{traceback.format_exc()}")
            await asyncio.sleep(2)
    
    def adjust_precision(self, number, unit):
        factor = 10 ** unit
        return int(number * factor) / factor

    async def reload_config(self):
        try:
            importlib.reload(reduce_mm_config)
            [setattr(self, k, v) for k, v in vars(reduce_mm_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"main循环报错{traceback.format_exc()}")
            except:
                pass
            
    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    # websea深度
    async def on_depth(self, content):
        if content.bids is not None:
            self.bid1_wb = content.bids[1].price
        if content.asks is not None:
            self.ask1_wb = content.asks[1].price
        self.last_on_depth_ts = content.ts

    # binance盘口一档
    async def on_bid_ask(self, content):
        self.loop.create_task(self.signal(content))
        self.bn_bid_price = content['bid_price']
        self.bn_ask_price = content['ask_price']
        self.last_on_bid_ask_ts = content['ts']
    
    # binance成交数据
    def on_order(self, content):
        self.loop.create_task(self.update_benchmark_price(content))
        self.loop.create_task(self.handle_trade(content))
        self.last_on_order_ts = content[0][0]
    
    async def on_kline(self, content):
        self.kline_dict[content['start_ts']] = [content['high'], content['low']]
        self.kline_price = list(self.kline_dict.values())
        self.last_on_kline_ts = content['ts']
    
    # 检查wss推送是否正常
    async def check_wss(self):
        # 外盘数据
        if time.time() - self.last_on_order_ts/1000 > 5:    # 上一笔成交在5s前,重连wss
            mess = f"压盘口策略{self.symbol},on_order数据wss推送异常,重新订阅"
            # self.log.info(mess)
            DataWss(self.hedge_symbol, topics=['tick'], rspFunc=self.on_order)
            tb.warning(mess, 'risk')
            tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)
        # 外盘数据
        if time.time() - self.last_on_kline_ts/1000 > 30:
            mess = f"压盘口策略{self.symbol},on_kline数据wss推送异常,重新订阅"
            # self.log.info(mess)
            await self.bn_wss.sub_kline(self.symbol, '1m')
            tb.warning(mess, 'risk')
            tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)
        # 外盘数据
        if time.time() - self.last_on_bid_ask_ts/1000 > 10:
            mess = f"压盘口策略{self.symbol},on_bid_ask数据wss推送异常,重新订阅"
            # self.log.info(mess)
            await self.bn_wss.sub_ticker_order(symbol=self.symbol)
            tb.warning(mess, 'risk')
            tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)
        # 内盘数据
        if time.time() - self.last_on_depth_ts/1000 > 30:
            mess = f"压盘口策略{self.symbol},on_depth数据wss推送异常,重新订阅"
            # self.log.info(mess)
            await self.ws_wss.sub_depth(symbol=self.symbol)
            tb.warning(mess, 'risk')
            tb.sendmail(f'websea合约{self.symbol}压盘口策略wss异常', mess)


    ''' ==========================================================================='''
    ''' =================================== bp_mm ================================='''
    ''' ==========================================================================='''

    async def handle_trade(self, item: dict):
        """
        {'symbol': 'ETH-USDT', 'price': 3696.22, 'amount': 0.24, 'cost': 887.0928, 'side': 'sell', 'timestamp': 1734615121759}
        """
        if -1 in (self.ask1, self.bid1):
            return
        side = 'buy' if item[0][-1] == 1 else 'sell'
        temp_item = {'symbol':self.symbol, 'price':item[0][1], 'amount':item[0][2], 'side':side, 'timestamp':item[0][0]}
        item = temp_item
        if not self.trades:
            self.trades.insert(0, item)
            return
        try:
            # 本条数据未结束
            if self.bp_rows[-1].end_ts == -1:
                bp = self.bp_rows.pop()
            # 初始化新数据
            else:
                bp = BpRow(side=item["side"])
                last_bp = self.bp_rows[-1]
                if 0 not in (last_bp.cum_chopping_ask_qty, last_bp.cum_chopping_bid_qty):
                    self.log.info(000, last_bp)
                # self.log.info(000, self.bp_rows[-1])
        except IndexError:
            bp = BpRow(side=item["side"])
        self.bp_rows.append(bp)
        if (side := bp.side) == "sell":
            if abs((price := item["price"]) / self.bid1 - 1) > 0.002:
                return
            await self.on_asks(side=side, price=price, amount=item["amount"], crt=item, last=self.trades[0], bp=bp)
        else:
            if abs((price := item["price"]) / self.ask1 - 1) > 0.002:
                return
            await self.on_bids(side=side, price=price, amount=item["amount"], crt=item, last=self.trades[0], bp=bp)
        # 历史交易数据(倒序)
        self.trades.insert(0, item)

    async def on_asks(self, side: str, price: float, amount: float, crt: dict, last: dict, bp: BpRow):
        if (price > (last_price := last["price"])) and (side == last["side"]) and (self.shocking is False):
            self.shocking = True
            bp.open_price = last_price
            bp.breaking_price = price
            bp.start_ts = last["timestamp"]
            bp.cum_shocking_qty = amount
            bp.breaking_qty = 0
            # 记录突破过程量
            for trade in self.trades:
                if (price == last_price) and (side == last["side"]):
                    bp.breaking_qty += trade["amount"]
                else:
                    bp.breaking_ts = trade["timestamp"]
                    break
        if self.shocking is False:
            bp.cum_chopping_ask_qty += amount
        # 冲击开始
        if self.shocking is True:
            # 方向转向
            if side != last["side"]:
                self.shocking = False
                bp.setback_type = 1
                bp.setback_price = price
                bp.close_price = max(last_price, price)
                bp.end_ts = last["timestamp"]
                await self.update_shock_dodge(bp)
            # 价格转向
            elif side == last["side"] and price < last_price:
                self.shocking = False
                bp.setback_type = 2
                bp.setback_price = price
                bp.close_price = last_price
                bp.end_ts = last["timestamp"]
                await self.update_shock_dodge(bp)
            # 持续冲击
            else:
                bp.cum_shocking_qty += amount
        # 暂存本条数据
        self.bp_rows[-1] = bp

    async def on_bids(self, side: str, price: float, amount: float, crt: dict, last: dict, bp: BpRow):
        if (price < (last_price := last["price"])) and (side == last["side"]) and (self.shocking is False):
            self.shocking = True
            bp.open_price = last_price
            bp.breaking_price = price
            bp.start_ts = last["timestamp"]
            bp.cum_shocking_qty = amount
            bp.breaking_qty = 0
            for trade in self.trades:
                if (price == last_price) and (side == last["side"]):
                    bp.breaking_qty += trade["amount"]
                else:
                    bp.breaking_ts = trade["timestamp"]
                    break
        if self.shocking is False:
            bp.cum_chopping_bid_qty += amount
        # 冲击开始
        if self.shocking is True:
            # 方向转向
            if side != last["side"]:
                self.shocking = False
                bp.setback_type = 1
                bp.setback_price = price
                bp.close_price = min(last_price, price)
                bp.end_ts = last["timestamp"]
                await self.update_shock_dodge(bp)
            # 价格转向
            elif side == last["side"] and price > last_price:
                self.shocking = False
                bp.setback_type = 2
                bp.setback_price = price
                bp.close_price = last_price
                bp.end_ts = last["timestamp"]
                await self.update_shock_dodge(bp)
            # 持续冲击
            else:
                bp.cum_shocking_qty += amount
        self.bp_rows[-1] = bp
    
        
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''
    # 测试验证
    async def check_info(self):
        open_orders = await self.rest.get_currentList(self.symbol, limit=10000, direct='prev') #, quan=True)
        buy_pirce, sell_price = [], []
        buy_num, sell_num = 0, 0
        for order in open_orders:
            if order.type == 'buy-limit':
                buy_num += 1
                buy_pirce.append(order.price)
            else:
                sell_num += 1
                sell_price.append(order.price)
        max_buy = max(buy_pirce) if buy_pirce else 0
        min_sell = min(sell_price) if sell_price else 0
        diff_ratio = round((min_sell-max_buy)/((min_sell+max_buy)/2)*100, 2) if max_buy and min_sell else '缺少挂单数据'
        try:
            arb1_ratio = round((max_buy/self.bn_ask_price-1)*100, 2)
            arb2_ratio = round((self.bn_bid_price/min_sell-1)*100, 2)
        except:
            pass
        local_max_buy = round(max([i[0] for i in self.local_open_buy_list]),2)
        local_min_sell = round(min([i[0] for i in self.local_open_sell_list]),2)
        self.log.info(f"一档价差:{round(min_sell-max_buy, self.precision.price)} {diff_ratio}% "
                      f"buy:{local_max_buy}({max_buy})({self.bn_bid_price}) sell:{local_min_sell}({min_sell})({self.bn_ask_price}) "
                      f"正套:{arb1_ratio}% 反套:{arb2_ratio}% "
                      f"buy档位数:{buy_num} sell档位数:{sell_num}")
        
    # 风控模块
    async def risk(self):
        try:
            # =====测试数据======
            self.base_balance = 10000
            # =====测试数据======

            # 当前委托
            open_buy_vol = 0
            open_sell_vol = 0
            temp_open_buy = []
            temp_open_sell = []
            # print(f"buy当前所有委托{len(self.local_open_buy_list)}笔 sell当前所有委托{len(self.local_open_sell_list)}笔")
            open_orders = await self.rest.get_currentList(self.symbol, limit=10000, direct='prev') #, quan=True)
            # print(f"内盘当前委托:{len(open_orders)}笔")

            # 测试数据
            if len(self.local_open_buy_list)+len(self.local_open_sell_list) != len(open_orders):
                rest_order = []
                local_buy = []
                local_sell = []
                for i in open_orders:
                    rest_order.append(i.order_id)
                for i in self.local_open_buy_list:
                    local_buy.append(i[1])
                for i in self.local_open_sell_list:
                    local_sell.append(i[1])

                # A 中有但 B 中没有的元素
                diff_A = list(set(rest_order) - set(local_buy))
                diff = list(set(diff_A) - set(local_sell))
                # print("rest中有但local没有的元素:", diff)

                # B 中有但 A 中没有的元素
                add = local_buy+local_sell
                diff = list(set(add) - set(rest_order))
                # print("local中有但rest没有的元素:", diff)
            # 测试数据

            buy_pirce, sell_price = [], []
            for order in open_orders:
                deal_amount = 0 if order.deal_amount is None else order.deal_amount
                if order.type == 'buy-limit':
                    buy_pirce.append(order.price)
                    open_buy_vol += order.amount-deal_amount
                    temp_open_buy.append([order.price, order.order_id, order.amount])
                else:
                    sell_price.append(order.price)
                    open_sell_vol += order.amount-deal_amount
                    temp_open_sell.append([order.price, order.order_id, order.amount])
            open_buy_vol *= self.symbol_unit
            open_sell_vol *= self.symbol_unit
            # local_max_buy = max([i[0] for i in self.local_open_buy_list])
            # local_min_sell = min([i[0] for i in self.local_open_sell_list])
            # max_buy = max(buy_pirce) if buy_pirce else 0
            # min_sell = min(sell_price) if sell_price else 0
            # self.log.info(f"本地buy:{local_max_buy} ({max_buy}) 本地sell:{local_min_sell} ({min_sell})")
            self.local_open_buy_list = temp_open_buy
            self.local_open_sell_list = temp_open_sell
            # self.log.info(f"一档价差:{round(min_sell-max_buy, self.precision.price)} {round((min_sell-max_buy)/((min_sell+max_buy)/2)*100, 2)}% buy_one:{max_buy} sell_one:{min_sell}")

            # 当前持仓
            pos_data = await self.rest.get_position(self.symbol) #, quan=True)
            current_position = 0
            buy_pos, sell_pos = 0, 0
            for i in pos_data:
                if i.symbol == self.symbol:
                    if i.type == 1:
                        buy_pos += i.amount
                        current_position += i.amount
                        self.pos_WB += i.amount*self.symbol_unit
                    else:
                        sell_pos += i.amount
                        current_position -= i.amount    # TODO 合约单位验证???
                        self.pos_WB -= i.amount*self.symbol_unit
            deviation = getattr(self, 'deviation', 0)
            if buy_pos > 100000 and sell_pos > 100000:
                self.is_open = False
            elif buy_pos < 20000 or sell_pos < 20000:
                self.is_open = True

            # 测试数据
            current_position = 1
            
            if current_position != self.last_current_position or deviation == 0:
                await self.update_deviation(current_position, open_buy_vol, open_sell_vol)
                self.last_current_position = current_position
            
            # 获取持仓,判断是否对冲
            pos_BN = await self.bn_rest.get_position(self.symbol)
            for k, v in pos_BN.items():
                if k == self.symbol.replace('-', ''):
                    self.pos_BN = float(v['positionAmt'])
            # print(f"外盘持仓:{self.pos_BN}  {pos_BN}")

            # TODO 回报数据的格式,需要确认.目前只有持仓金额,而非权益
            # balance = await self.rest.get_walletList(quan=True)
            # self.log.info(f"账户权益:{balance}")
            # self.base_balance = balance.avail+balance.frozen
            
            
        except Exception as e:
            self.log.warning(f"风控异常:{traceback.format_exc()}")
    
    async def update_kline(self):
        try:
            kline = await self.bn_rest.get_kline(self.symbol, interval='1m', limit=100)
            for k in kline:
                self.kline_dict[k['start_ts']] = [k['high'], k['low']]
            self.kline_price = list(self.kline_dict.values())
        except:
            pass
    
    # 档持仓有变化后更新偏移量。范围在-1到1之间。-1表示开满空仓,1表示开满多仓
    async def update_deviation(self, current_position, open_buy_vol, open_sell_vol):
        self.base_position = self.base_balance/self.trade_price*self.leverage
        self.deviation = max(-1, min(1, current_position/self.base_position))
        self.log.info(f"偏差:{self.deviation}")
        await self.update_shape(self.deviation)
    
    async def update_shape(self, deviation):
        self.buy_shape = self.default_shape - max(0, 0.5*deviation)
        self.sell_shape = self.default_shape - max(0, -0.5*deviation)
    
    async def update_range(self, tag='timer'):
        high_price = [p[0] for p in self.kline_price[-3:]]
        low_price = [p[1] for p in self.kline_price[-3:]]
        if tag == 'timer':
            RH_Time = max(high_price)
            RL_Time = min(low_price)
            # 每秒驱动计算
            RH_Time = max(self.refer_price, 0.01 * self.refer_price + 0.99 * RH_Time)
            RL_Time = min(self.refer_price, 0.01 * self.refer_price + 0.99 * RL_Time)
            self.time_band_f = RH_Time - RL_Time
        elif tag == 'shock':
            RH_shock = max(high_price)
            RL_shock = min(low_price)
            RH_shock = max(self.refer_price, 0.01 * self.refer_price+0.99 * RH_shock)
            RL_shock = min(self.refer_price, 0.01 * self.refer_price+0.99 * RL_shock)
            self.shock_band_f = RH_shock - RL_shock
        time_band_f = self.time_band_f if getattr(self, 'time_band_f', 0) else self.shock_band_f
        shock_band_f = self.shock_band_f if getattr(self, 'shock_band_f', 0) else self.time_band_f
        self.range = max(0.0015 * self.refer_price, time_band_f, shock_band_f)
        
    # 通过推送的一档盘口计算信号
    async def signal(self, content):
        signal1 = 0
        bid_price = content['bid_price']
        ask_price = content['ask_price']
        self.bid1, self.ask1 = bid_price, ask_price
        bid_vol = content['bid_qty']
        ask_vol = content['ask_qty']

        signal0 = 1 if bid_vol > ask_vol else -1
        signal1 = 0.9*self.signal1_last + 0.1*signal0
        self.signal1_last = signal1
        signal = 1 if signal1 > 0 else -1
        if signal != self.signal_last:
            await self.bid_ask_price(signal, 'signal')
            self.signal_last = signal
    
    # 计算挂单量
    async def bid_ask_budget(self):
        # 初始化
        high_price = [p[0] for p in self.kline_price[-60:]]
        low_price = [p[1] for p in self.kline_price[-60:]]
        RH_S = max(high_price)
        RL_S = min(low_price)

        # 用最新成交价格计算
        RH_S = max(self.trade_price, 0.01 * self.trade_price + 0.99 * RH_S)
        RL_S = min(self.trade_price, 0.01 * self.trade_price + 0.99 * RL_S)

        time_band_s = RH_S - RL_S
        # 半衰期1.5 hr 的波动区间
        deviation = getattr(self, 'deviation', 0)
        buy_budget_adj = min(self.max_budget_utilization, math.sqrt(self.range/time_band_s)) * (1-deviation**3)
        # self.vol_log.write(f"buy_budget_adj来源:max_budget_utilization:{self.max_budget_utilization} 分子:{math.sqrt(self.range/time_band_s)} 分母:{1-deviation**3} deviation:{deviation}")
        sell_budget_adj = min(self.max_budget_utilization, math.sqrt(self.range/time_band_s)) * (1+deviation**3)

        # 当前账户usdt权益/标记价格*最大杠杆*buy_budget_adj调整值
        self.buy_budget = max(0, self.leverage*buy_budget_adj*self.base_balance/self.trade_price)
        # self.vol_log.write(f"self.buy_budget来源:self.leverage:{self.leverage}*buy_budget_adj:{buy_budget_adj}*self.base_balance:{self.base_balance}/self.trade_price:{self.trade_price}")
        self.sell_budget = max(0, self.leverage*sell_budget_adj*self.base_balance/self.trade_price)

    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''

    # step 1 计算价格往后躲多远。发生价格冲击后调用这个函数
    async def update_shock_dodge(self, bp: BpRow):
        open_price = bp.open_price
        close_price = bp.close_price
        if open_price == -1 or close_price == -1:   # 这一行导致价差越来越大
            return

        # 只有产生冲击,才会驱动来计算shock_dodge,驱动spread 计算
        shock_impact = close_price - open_price
        # self.price_log.write(f"shock_impact来源:close_price:{close_price} open_price:{open_price} 冲击价格的开始和技术")
        shock_dodge = max(close_price * 0.00004, \
                         0.9*self.shock_dodge_last + 0.1* abs(shock_impact), \
                         0.99*self.shock_dodge_last + 0.01* abs(shock_impact))
        temp = self.shock_dodge_last
        self.shock_dodge_last = shock_dodge
        # self.price_log.write(f"self.shock_dodge_last来源:close_price:{close_price} self.shock_dodge_last:{temp} shock_impact:{shock_impact}")
        await asyncio.gather(self.bid_ask_spread(shock_dodge), \
                             self.update_range('shock'))

    # step 2 算出来我们的盘口价格。每次binance成交调用这个函数
    async def update_benchmark_price(self, content):
        self.trade_ts = content[0][0]
        self.trade_price = content[0][1]
        self.trade_side = content[0][-1]
        # 用trade驱动
        self.refer_price = self.trade_price if self.refer_price == 0 else self.refer_price
        # 跟上一次价格偏差超过1%则忽略
        if abs(self.trade_price/self.refer_price -1) > 0.01:
            return
        else:
            trade_price = self.trade_price
        
        # 驱动决策
        self.benchmark_ask_price = getattr(self, 'benchmark_ask_price', trade_price)    # 以最新成交作为初始值
        self.benchmark_bid_price = getattr(self, 'benchmark_bid_price', trade_price)
        # TODO 价格有更新，但是没有走下面4个逻辑，导致没有重新挂撤单
        if self.trade_side == 1:    # buy
            self.sell_shocking = 0
            if (trade_price - self.benchmark_ask_price) > self.min_price_step:
                self.benchmark_ask_price = trade_price + self.shock_dodge_last
                self.buy_shocking = 1
                await self.bid_ask_price(self.signal_last, 'benchmark_price11111')
            if (trade_price - self.benchmark_ask_price) < -self.min_price_step and self.buy_shocking == 0:
                self.benchmark_ask_price = trade_price
                await self.bid_ask_price(self.signal_last, 'benchmark_price22222')
                await self.hedge()
        # 驱动对冲
        elif self.trade_side == -1: # sell
            self.buy_shocking = 0
            if (trade_price - self.benchmark_bid_price) < -self.min_price_step:
                self.benchmark_bid_price = trade_price - self.shock_dodge_last
                # self.price_log.write(f"self.benchmark_bid_price来源1:trade_price:{trade_price}最新成交价 self.shock_dodge_last:{self.shock_dodge_last}")
                self.sell_shocking = 1
                await self.bid_ask_price(self.signal_last, 'benchmark_price33333')
            if (trade_price - self.benchmark_bid_price) > self.min_price_step and self.sell_shocking == 0:
                self.benchmark_bid_price = trade_price
                # self.price_log.write(f"self.benchmark_bid_price来源2:trade_price:{trade_price}")
                await self.bid_ask_price(self.signal_last, 'benchmark_price44444')
                # 驱动对冲
                await self.hedge()
        self.refer_price = trade_price    # 上一次价格
        
    # Step 3 计算bid/ask price
    async def bid_ask_price(self, signal, tag=''):
        if self.buy_shocking != 0 or self.sell_shocking != 0:
            return
        bid_price= self.benchmark_bid_price*(1+0.00004*signal) - self.bid_spread
        # self.price_log.write(f"bid_price来源:self.benchmark_bid_price:{self.benchmark_bid_price} signal:{signal} self.bid_spread:{self.bid_spread}")
        ask_price= self.benchmark_ask_price*(1+0.00004*signal) + self.ask_spread

        # websea最新盘口数据
        bid1_wb = self.bid1_wb
        ask1_wb = self.ask1_wb
        # TODO 这个公式有疑问??? signal=1时,ask_price_adj价格反而下降了
        self.bid_price_adj = max(bid_price*(1-self.taker_fee), \
                                 min(ask1_wb-self.min_price_step, (1-self.maker_fee*bid_price)))
        # self.price_log.write(f"bid_price_adj来源:bid_price:{bid_price} ask1_wb:{ask1_wb}来源是depth推送的一档价格")
        # TODO 确认这里逻辑是否正确,self.ask_price_adj和ask_price的区别是什么?
        self.ask_price_adj = min(ask_price*(1+self.taker_fee), \
                                 max(bid1_wb+self.min_price_step, (1+self.maker_fee*ask_price)))

        if not self.main_clock:
            try:
                await self.main('bid_ask_price函数')   # 去下单
            except:
                self.main_clock = False
                self.log.warning(f"main error:{traceback.format_exc()}")
        else:
            pass
        
    # step 4 计算bid、ask的价差
    async def bid_ask_spread(self, shock_dodge):
        # 价格冲击后计算shock dodge ——> 计算spread
        self.ask_spread = shock_dodge
        self.bid_spread = shock_dodge
        
    async def main(self, source=''):
        self.main_clock = True
        open_buy_list = []      # buy当前委托
        open_sell_list = []     # sell当前委托
        to_cancel_list = []     # 保存撤单的order_id
        cancel_buy_list = []  # 保存撤单的index
        cancel_sell_list = []

        for order in self.local_open_buy_list:
            if order[0] > self.bid_price_adj:
                to_cancel_list.append(order[1])
                cancel_buy_list.append(order)
            else:
                open_buy_list.append(order)
        for order in self.local_open_sell_list:
            if order[0] < self.ask_price_adj:
                to_cancel_list.append(order[1])
                cancel_sell_list.append(order)
            else:
                open_sell_list.append(order)
        
        for i in cancel_buy_list:
            self.local_open_buy_list.remove(i)
        for i in cancel_sell_list:
            self.local_open_sell_list.remove(i)
        
        if to_cancel_list:
            await self.cancel_orders(to_cancel_list, '因价格撤近端')
        
        # 补 or 撤单
        await self.bid_ask_budget()     # 更新挂单量
        sell_target_cum_qty = 0         # sell当前档位之前所有档的累计挂单量
        sell_sending_sum_qty = 0
        buy_target_cum_qty = 0          # buy当前档位之前所有档的累计挂单量
        buy_sending_sum_qty = 0
        sell_target_add = 1 / self.place_num * self.sell_budget     # 每一档挂单量
        buy_target_add = 1 / self.place_num * self.buy_budget       # 每一档挂单量
        # self.vol_log.write(f"buy_target_add来源:self.buy_budget:{self.buy_budget}")
        to_sell_create_orders = {}   # sell本次循环要新挂的订单
        to_buy_create_orders = {}    # buy本次循环要新挂的订单
        to_cancel_orders = []        # buy和sell放一起
        cancel_buy_list = []
        cancel_sell_list = []
        buy_price_last = 0           # 上一次buy的价格
        sell_price_last = 0          # 上一次sell的价格

        # 测试数据
        for num in range(1, self.place_num+1):
            buy_price = self.bid_price_adj - (num / (self.place_num-1)) ** self.buy_shape * self.range  # 这个算法很NB
            sell_price = self.ask_price_adj + (num / (self.place_num-1)) ** self.sell_shape * self.range
            buy_target_cum_qty = (num + 0.5*self.place_num) / (self.place_num + 0.5*self.place_num) * self.buy_budget  # 这里是buy累计的挂单量
            sell_target_cum_qty = (num + 0.5*self.place_num) / (self.place_num + 0.5*self.place_num) * self.sell_budget  # 这里是sell累计的挂单量
            # self.price_log.write(f'buy_price:{buy_price}来源:self.bid_price_adj:{self.bid_price_adj} - (num:{num} / (self.place_num:{self.place_num}-1)) ** self.buy_shape:{self.buy_shape} * self.range:{self.range}')
            sell_cum_qty = 0    # sell需要撤单的总量
            buy_cum_qty = 0     # buy需要撤单的总量
            for i in range(len(open_sell_list)):
                if open_sell_list[i][0] <= sell_price:
                    sell_cum_qty += open_sell_list[i][2]
            for i in range(len(open_buy_list)):
                if open_buy_list[i][0] >= buy_price:
                    buy_cum_qty += open_buy_list[i][2]
            # 需要撤单的量+新挂的量
            sell_cum_qty_adj = sell_cum_qty + sell_sending_sum_qty
            buy_cum_qty_adj = buy_cum_qty + buy_sending_sum_qty

            # 处理sell数据
            # gap(本次应该挂的量)=到num这一档之前的累计挂单量-(需要撤单的量+新挂的量)
            sell_gap = sell_target_cum_qty - sell_cum_qty_adj
            if sell_gap >= max((0.5+0.5*(self.frequency_controller-1))*sell_target_add, self.symbol_unit):
                # to_sell_qty = round(sell_gap)    # 精度优化
                to_sell_qty = sell_gap    # 测试数据
                price = sell_price
                sell_sending_sum_qty += to_sell_qty
                if price not in self.local_open_sell_list and \
                    len(self.local_open_sell_list) < int(self.place_num):
                    to_sell_create_orders[price] = to_sell_qty    # 记录下单数据
            elif sell_gap <= -(1+0.3*self.deviation)*sell_target_add:  # 减仓逻辑 对卖
                # 从sell price_last(不含)向上找3个订单 #对buy 向下
                temp_sell = [open_sell_list[i] for i in range(len(open_sell_list)) \
                             if open_sell_list[i][0] > sell_price_last]
                if temp_sell:
                    # 对temp_sell从小到大排序
                    temp_sell.sort(key=lambda x: x[0])
                    for i in range(min(len(temp_sell), 3)):
                        sell_sending_sum_qty -= temp_sell[i][2]
                        to_cancel_orders.append(temp_sell[i][1])  # 记录撤单数据
                        # self.price_log.write(f"sell撤单数据:{temp_sell[i]}")
                        cancel_sell_list.append(temp_sell[i])
                        sell_gap += temp_sell[i][2]
                        if sell_gap >= -(1+0.3*self.deviation)*sell_target_add:
                            break
            sell_price_last = sell_price
            
            # 处理buy数据
            buy_gap = buy_target_cum_qty - buy_cum_qty_adj
            if buy_gap >= max((0.5+0.5*(self.frequency_controller-1))*buy_target_add, self.symbol_unit):
                # to_buy_qty = round(buy_gap)    # 精度优化
                to_buy_qty = buy_gap    # 测试数据
                price = buy_price
                buy_sending_sum_qty += to_buy_qty
                if price not in self.local_open_buy_list and \
                    len(self.local_open_buy_list) < int(self.place_num):
                    to_buy_create_orders[price] = to_buy_qty      # 记录下单数据
            elif buy_gap <= -(1-0.3*self.deviation)*buy_target_add:  # 对买
                temp_buy = [open_buy_list[i] for i in range(len(open_buy_list)) \
                             if open_buy_list[i][0] < buy_price_last]
                if temp_buy:
                    # 对temp_sell从大到小排序
                    temp_buy.sort(key=lambda x: x[0], reverse=True)
                    for i in range(min(len(temp_buy), 3)):
                        buy_sending_sum_qty -= temp_buy[i][2] # 撤单所以要减去
                        to_cancel_orders.append(temp_buy[i][1])  # 记录撤单数据
                        # self.price_log.write(f"buy撤单数据:{temp_buy[i]}")
                        cancel_buy_list.append(temp_buy[i])
                        buy_gap += temp_buy[i][2]
                        if buy_gap >= -(1-0.3*self.deviation)*buy_target_add:  # 对买
                            break
            buy_price_last = buy_price
            
        if to_cancel_orders:
            try:
                for i in cancel_buy_list:
                    self.local_open_buy_list.remove(i)
                for i in cancel_sell_list:
                    self.local_open_sell_list.remove(i)
            except:
                self.log.warning(f"975报错:{traceback.format_exc()}\nsell原始数据:{cancel_sell_list}\n{self.local_open_sell_list}\nbuy原始数据:{cancel_buy_list}\n{self.local_open_buy_list}")
            await self.cancel_orders(to_cancel_orders, '因数量撤近端')
        
        # 下单
        if to_buy_create_orders or to_sell_create_orders:
            await asyncio.gather(self.make_order(to_buy_create_orders, 'BUY'), \
                                 self.make_order(to_sell_create_orders, 'SELL'))
        
        # 撤远单:
        cancel_buy_list = []
        cancel_sell_list = []
        # 按数量撤单
        # 在open buy list 中, if order num按price排序(从大到小),剔除后 self.place_num* 1.3名外的订单
        # 在open sell list 中,if order num按price排序(从小到大),剔除后 self.place_num* 1.3名外的订单
        # 按价格撤单
        # 在open buy list 中，撤 price < 0.999 * base price - buy range的订单
        # 在open sell list 中，撤 price > 1.001 * base price + sell range的订单
        open_buy_list = sorted(open_buy_list, key=lambda x: x[0], reverse=True)     # 从大到小排序
        open_sell_list = sorted(open_sell_list, key=lambda x: x[0], reverse=False)  # 从小到大排序
        far_to_cancel_list = []
        price_cancel_list = []
        num_cancel_list = []
        for i in open_buy_list:
            if i[0] < (1-self.cancel_price_ratio) * self.trade_price - self.range:
                price_cancel_list.append(i[1])
        for i in open_sell_list:
            if i[0] > (1+self.cancel_price_ratio) * self.trade_price + self.range:
                price_cancel_list.append(i[1])
        for i in open_buy_list[int(self.place_num/2*1.3):]:
            num_cancel_list.append(i[1])
        for i in open_sell_list[int(self.place_num/2*1.3):]:
            num_cancel_list.append(i[1])
        far_to_cancel_list = price_cancel_list + num_cancel_list
        
        if far_to_cancel_list:
            await self.cancel_orders(far_to_cancel_list, '撤远端')
            for i in cancel_buy_list:
                self.local_open_buy_list.remove(i)
            for i in cancel_sell_list:
                self.local_open_sell_list.remove(i)
        self.main_clock = False

    async def make_order(self, grid_list, side, tag=''):
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        for price, vol in grid_list.items():
            try:
                vol = int(vol/self.symbol_unit)
                if self.is_open:
                    res = await self.rest.order_create(self.symbol, od_type=od_type, \
                                                price=price, amount=vol, \
                                                precision=self.precision)   #, quan=True)
                else:
                    res = await self.rest.order_create(self.symbol, od_type=od_type, \
                                                    price=price, amount=vol, \
                                                    precision=self.precision, \
                                                    contract_type='close')  #, quan=True)
                if side == 'BUY':
                    self.local_open_buy_list.append([price, res.order_id, vol*self.symbol_unit])
                else:
                    self.local_open_sell_list.append([price, res.order_id, vol*self.symbol_unit])
                # self.log.info(f"{tag}开仓信息:{side} 价:{price} 量:{vol} 回报:{res}")
            except Exception as e:
                self.log.warning(f"{tag} {self.symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")
    
    async def close_order(self, side, price, vol, tag=''):
        if vol == 0:
            return
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        try:
            self.log.info(f"{tag}平仓信息:{side} 价:{price} 量:{vol}")
            # res = await self.rest.order_create(self.symbol, od_type=od_type, \
            #                                     price=price, amount=vol, \
            #                                     precision=self.precision, contract_type='close', quan=True)
            # self.log.info(f"平仓信息:{side} 价:{price} 量:{vol} 平仓回报:{res}")
        except:
            self.log.warning(f"{self.symbol}-{side}-{price}-{vol} 平仓错误: {traceback.format_exc()}")

    # 对冲。每次BN单次冲击结束后调用该函数
    async def hedge(self):
        # 实时更新 Current position_BN
        # 实时更新 Current position_WB  目前ws不支持
        
        # TODO 计算对冲量的逻辑验证不通过
        # to_hedge_buy = max(0, -self.pos_WB + self.pos_BN - self.hedge_hurdle * self.base_position)
        # to_hedge_sell = max(0, self.pos_WB - self.pos_BN - self.hedge_hurdle * self.base_position)

        # 另一种计算方法
        # if net_pos < 0:
        #     to_hedge_buy = abs(net_pos) - self.hedge_hurdle * self.base_position
        # else:
        #     to_hedge_sell = net_pos - self.hedge_hurdle * self.base_position
        
        # 测试数据
        # self.pos_BN = -1000
        # self.pos_WB = 30000
        # 测试数据
        hedge_vol_precision  = self.hedge_symbol_precision[self.symbol.replace('-','')][1]
        net_pos = self.pos_BN + self.pos_WB
        # 净多单,计算对冲多少空单
        if net_pos > 0:
            # to_hedge_sell = max(0, self.pos_WB - self.pos_BN - self.hedge_hurdle * self.base_position)
            to_hedge_sell = net_pos - self.hedge_hurdle * self.base_position
            # print('对冲:',1111111111, net_pos, to_hedge_sell, hedge_vol_precision)
            if to_hedge_sell <= 0.001:
                open_orders = await self.bn_rest.get_currentList(self.symbol)
                if open_orders is not None:
                    for order in open_orders:
                        # print(f"binance当前sell委托:{order}")
                        if order['status'] in ['NEW','PARTIALLY_FILLED'] and order['side'] == 'SELL':
                            try:
                                res = await self.bn_rest.order_cancel(self.symbol, order['orderId'])
                                # self.log.info(f"binance对冲撤单:{res}")
                            except:
                                self.log.warning(f"撤单报错:{traceback.format_exc()}")
            elif to_hedge_sell > 0.001:
                if not self.main_clock:
                    try:
                        await self.main('hedge函数')
                    except:
                        self.main_clock = False
                        self.log.warning(f"main error:{traceback.format_exc()}")
                else:
                    pass
                    # self.log.info(f"main_clock is True")
                sell_budget_BN = round(to_hedge_sell, hedge_vol_precision)
                ask_price_adj_BN = self.benchmark_ask_price
                # 挂单为maker only
                # print(f"sell对冲下单:价:{ask_price_adj_BN} 量:{sell_budget_BN}")
                # res = await self.bn_rest.order_create(self.symbol, 'SELL', ocb.OrderType.LIMIT, sell_budget_BN, ask_price_adj_BN)
                # print(f"sell对冲下单:{res}")
        # 净空单,计算对冲多少多单
        elif net_pos < 0:
            # to_hedge_buy = max(0, -self.pos_WB + self.pos_BN - self.hedge_hurdle * self.base_position)
            to_hedge_buy = abs(net_pos) - self.hedge_hurdle * self.base_position
            # print(f"对冲:2222222222, 净持仓:{net_pos}, 对冲数量:{to_hedge_buy}, 价格单位:{hedge_vol_precision}, 基础持仓:{self.base_position}")
            if to_hedge_buy <= 0.001:
                open_orders = await self.bn_rest.get_currentList(self.symbol)
                if open_orders is not None:
                    for order in open_orders:
                        # print(f"binance当前buy委托:{order}")
                        if order['status'] in ['NEW','PARTIALLY_FILLED'] and order['side'] == 'BUY':
                            try:
                                res = await self.bn_rest.order_cancel(self.symbol, order['orderId'])
                                # self.log.info(f"binance对冲撤单:{res}")
                            except:
                                self.log.warning(f"撤单报错:{traceback.format_exc()}")
            elif to_hedge_buy > 0.001:
                if not self.main_clock:
                    try:
                        await self.main('hedge函数')
                    except:
                        self.main_clock = False
                        self.log.warning(f"main error:{traceback.format_exc()}")
                else:
                    pass
                    # self.log.info(f"main_clock is True")
                buy_budget_BN = round(to_hedge_buy, hedge_vol_precision)
                bid_price_adj_BN = self.benchmark_bid_price
                # 挂单为maker only
                # print(f"buy对冲下单:价:{bid_price_adj_BN} 量:{buy_budget_BN}")
                # res = await self.bn_rest.order_create(self.symbol, 'BUY', ocb.OrderType.LIMIT, buy_budget_BN, bid_price_adj_BN)
                # print(f"buy对冲下单:{res}")



def main(path='reduce_mm_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()


if __name__ == '__main__':
    main()


