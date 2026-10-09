'''
    在v2基础上改成跟随现货MH铺单
'''
import sys
import time
import random
import traceback
import importlib
import asyncio
import single_mm_config
from collections import OrderedDict
sys.path.append("../..")
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
import objects.contract_request.websea as ocw
import objects.contract_request.binance as ocb
from objects.constant import OrderStatus as ost
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from client.env_pro.rest.websea.spot import WebseaSpot as ws_spot_rest
from client.env_pro.rest.websea.spot import WebseaSpot as ws_spot_rest_online
# from client.env_dev.rest.websea_contract import WebseaContract as RestWSC
# from client.env_dev.rest.websea_contract import WebseaContract as ws_contract_rest    # 测试环境
# from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss      # 测试环境
# from client.env_dev.rest.websea_spot import WebseaSpot as ws_spot_rest                # 测试环境
from utils.functool import format_float
sys.path.append("/usr/local/server/wbfAPI/exchange")
from binanceUsdtSwap import DataWss, AccountRest

strategy_name = 'XXX策略'


class LimitedSizeDict(OrderedDict):
    def __init__(self, *args, max_size=100, **kwargs):
        self._max_size = max_size
        super().__init__(*args, **kwargs)
        
    def __setitem__(self, key, value):
         # 如果字典已满，则先删除最早的项
        while len(self) >= self._max_size:
            self.popitem(last=False)
        super().__setitem__(key, value)


class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数

    async def on_first(self):
        self.log.info("=======first======")
        await self.cancel_orders(tag='开盘撤单')
        await self.spot_get_trade()

        # self.ws_wss = ws_contract_wss()
        # self.ws_wss.on_depth = self.on_depth
        # self.loop.create_task(self.ws_wss.only_subscribe())
        # self.loop.create_task(self.ws_wss.sub_depth(symbol=self.symbol))

        await self.get_precision()          # 更新币对信息
        await self.get_symbol_unit()        # 查询合约单位
        await self.risk()

    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.update_range, CronTrigger(second="*/1"))  # 更新波动率
        self.schedule.add_job(self.check_info, CronTrigger(second="*/2"))    # 每2s执行一次
        self.schedule.add_job(self.update_trade, CronTrigger(second="*/2"))  # 更新内盘最新成交
        self.schedule.add_job(self.main, CronTrigger(second="*/2"))          # 主循环铺单逻辑
        # self.schedule.add_job(self.update_kline, CronTrigger(second="*/2"))  # 获取K线数据
        # self.schedule.add_job(self.check_wss, CronTrigger(second="*/5"))     # 检查wss连接    
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))         # 风控
        self.schedule.add_job(self.reload_config, CronTrigger(second="00"))  # 重新加载配置文件
            
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rest = ws_contract_rest(config['swap_token'], config['swap_secret'])
        self.spot_rest = ws_spot_rest(config['spot_token'], config['spot_secret'])
        # self.bn_rest = bn_rest('', '')

    def _initParams(self):
        """初始化参数
        """
        self.ignore_order_status = {3, 4, 6, ost.AlreadyDeal, ost.Canceling, ost.Canceling}
        self.last_follow_trade_price = 0    # 外盘最后一次成交价格
        self.last_base_trade_price = 0      # 内盘上一次成交价格
        self.long_pos = 0                   # 多头持仓
        self.short_pos = 0                  # 空头持仓
        self.last_price = 0                 # 上次公允价格,用来判断是否重新铺单
        self.first_run = True               # 第一次运行锁
        self.last_mark_price = 0            # 上次标记价格
        # self.buy_shape = self.default_shape
        # self.sell_shape = self.default_shape
        self.kline_dict = LimitedSizeDict(max_size=self.dict_max_size)    # 保存kline

        self.buy_vol = 0            # 总买量,需要本地化的参数
        self.buy_amt = 0            # 总买金额,需要本地化的参数
        self.sell_vol = 0           # 总卖量,需要本地化的参数
        self.sell_amt = 0           # 总卖金额,需要本地化的参数
        self.last_deal_id = 0       # 最新成交id,需要本地化的参数

    async def get_precision(self):
        """更新 币对信息"""
        self.precision = await self.rest.get_precision(self.symbol, quan=True)     # 测试时注释,正式环境要加, quan=True)
        self.log.info(f"precision: {self.precision}")
        self.min_price_step = 10 ** (-self.precision.price)
        if self.symbol == "BTC-USDT":
            self.min_price_step = 0.1
        self.log.info(f"min_price_step: {format_float(self.min_price_step)}")

    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                data = await self.rest.get_symbols(self.symbol, quan=True)
                # 若走这个逻辑,表示交易对下架
                if not isinstance(data, list):
                    self.symbol_unit = data.contract_size
                    self.log.info(f"symbol_unit:{self.symbol_unit}")
                    break
                else:
                    raise f'{self.symbol}交易对已下架,获取不到合约单位'
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(1)

    async def cancel_orders(self, orders=None, tag=''):
        while True:
            try:
                if orders is None:
                    res = await self.rest.order_cancel(symbol=self.symbol, quan=True)
                else:
                    res = await self.rest.order_cancel(order_ids=orders, quan=True)
                self.log.info(f"{tag}cancel回报:{res}")
                break
            except Exception as e:
                self.log.warning(f"{tag}撤单错误:{traceback.format_exc()}")
            await asyncio.sleep(1)

    async def reload_config(self):
        try:
            importlib.reload(single_mm_config)
            [setattr(self, k, v) for k, v in vars(single_mm_config).items()]
        except:
            self.log.warning(f"reload_config函数报错{traceback.format_exc()}")

    # 定时获取当前所有委托
    async def get_orders(self):
        ask_orderbook, bid_orderbook = {}, {}
        order_lst = await self.rest.get_currentList(self.symbol, direct="profit", limit=10000, quan=True)
        if order_lst is None:
            return
        for index, order in enumerate(order_lst):
            if isinstance(order, Exception):
                continue
            if order.status in self.ignore_order_status:
                continue
            if order.type.startswith("sell"):
                ask_orderbook[order.order_id] = order
            else:
                bid_orderbook[order.order_id] = order
        ask_orderbook = dict(sorted(ask_orderbook.items(), key=lambda item: item[1].price))
        bid_orderbook = dict(sorted(bid_orderbook.items(), key=lambda item: item[1].price, reverse=True))
        return ask_orderbook, bid_orderbook
    
    # 检查wss推送是否正常
    async def check_wss(self):
        # 外盘数据
        if time.time() - self.last_on_order_ts/1000 > 5:    # 上一笔成交在5s前,重连wss
            mess = f"{strategy_name}{self.symbol},on_order数据wss推送异常,重新订阅"
            self.log.info(mess)
            # DataWss(self.hedge_symbol, topics=['tick'], rspFunc=self.on_order)
            # tb.warning(mess, 'risk')
            # tb.sendmail(f'websea合约{self.symbol}{strategy_name}wss异常', mess)
            
    def count_mark_price(self, ratio):
        # print(self.last_mark_price, ratio, self.adjust_volatility, '====='*9)
        mark_price = self.last_mark_price*(1+ratio*(1+self.adjust_volatility))
        temp_price1 = self.last_mark_price*(1+ratio*(1+self.adjust_volatility))
        temp_price2 = self.last_mark_price*(1-ratio*(1+self.adjust_volatility))
        # 将标记价格控制在净持仓均价的有利方向,且偏离不超过一定比例
        if self.net_pos_side == 'long' or mark_price < self.pos_price*(1-self.max_deviation):
            if mark_price > self.pos_price:
                mark_price = temp_price1 if temp_price1 < temp_price2 else temp_price2
        elif self.net_pos_side == 'short' or mark_price > self.pos_price*(1+self.max_deviation):
            if mark_price < self.pos_price:
                mark_price = temp_price1 if temp_price1 > temp_price2 else temp_price2
        if self.base_trade_price != self.last_base_trade_price:
            diff_ratio = self.base_trade_price/self.last_mark_price-1
            if abs(diff_ratio) > self.re_mm_tatio:
                mark_price = self.last_mark_price*(1+self.re_mm_tatio) if diff_ratio > 0 else \
                             self.last_mark_price*(1-self.re_mm_tatio)
            else:
                mark_price = self.base_trade_price
            self.last_base_trade_price = self.base_trade_price
        print(f"{self.follow_symbol}价格波动{ratio},上次标记价格:{self.last_mark_price} 本次价格:{mark_price}")
        self.last_mark_price = mark_price
        return mark_price
    
    # 档持仓有变化后更新偏移量。范围在-1到1之间。-1表示开满空仓,1表示开满多仓
    async def update_deviation(self, current_position):
        # 用盘口中间价而不是最新成交价格更合理
        try:
            price = self.mark_price
        except:
            price = self.base_trade_price if getattr(self, 'base_trade_price', 0) else \
                    self.spot_trade_price
        self.base_position = self.base_balance/price*self.leverage
        self.deviation = max(-1, min(1, current_position/self.base_position))
        self.log.info(f"偏差:{self.deviation} 当前持仓:{current_position} 基础持仓:{self.base_position}")
        # await self.update_shape(self.deviation)
    
    # async def update_shape(self, deviation):
    #     self.buy_shape = self.default_shape - max(0, 0.5*deviation)     # 根据持仓的偏移
    #     self.sell_shape = self.default_shape - max(0, -0.5*deviation)
    
    # 计算挂单量
    async def bid_ask_budget(self):
        deviation = self.deviation
        if -1 < deviation < 1:
            buy_budget_adj = self.max_budget_utilization * (1-deviation**3)
            sell_budget_adj = self.max_budget_utilization * (1+deviation**3)
        else:
            buy_budget_adj = self.buy_budget_adj
            sell_budget_adj = self.sell_budget_adj
        try:
            price = self.mark_price
        except:
            price = self.base_trade_price if getattr(self, 'base_trade_price', 0) else \
                    self.spot_trade_price
        buy_budget = max(0, self.leverage*buy_budget_adj*self.base_balance/price)
        sell_budget = max(0, self.leverage*sell_budget_adj*self.base_balance/price)
        return buy_budget, sell_budget
    
    # async def update_range(self):
    #     high_price = [p[0] for p in self.kline_price[-3:]]
    #     low_price = [p[1] for p in self.kline_price[-3:]]
    #     RH_Time = max(high_price)
    #     RL_Time = min(low_price)
    #     # 每秒驱动计算
    #     RH_Time = max(self.last_follow_trade_price, 0.01 * self.last_follow_trade_price + 0.99 * RH_Time)
    #     RL_Time = min(self.last_follow_trade_price, 0.01 * self.last_follow_trade_price + 0.99 * RL_Time)
    #     time_band_f = RH_Time - RL_Time
    #     try:
    #         price = self.mark_price
    #     except:
    #         price = self.base_trade_price if getattr(self, 'base_trade_price', 0) else \
    #                 self.spot_trade_price
    #     denominator = int(self.last_follow_trade_price/price)
    #     print(f"跟随交易对是本交易对价格的倍数:{denominator}")
    #     self.range = max(0.0015 * self.last_follow_trade_price, time_band_f)/denominator

    # 获取现货最新成交价格
    async def spot_get_trade(self):
        res = await self.spot_rest.get_trade(self.symbol)
        self.spot_trade_price = res[0]['price']
        print(f"现货最新成交:{res[0]['price']}")
        self.last_mark_price = self.spot_trade_price

    # 内盘合约最新成交
    async def update_trade(self):
        trade = await self.rest.get_trade(self.symbol, size=1, quan=True)
        # print(f"合约最新成交价格:{trade}")
        self.base_trade_price = trade[0].price

        depth = await self.spot_rest.get_depth(self.symbol)
        # print(f"现货MH最新depth:{depth}")
        try:
            now_mark_price = (float(depth['bids'][0][0]) + float(depth['asks'][0][0]))/2
            self.last_price = self.last_price if self.last_price != 0 else now_mark_price
            self.ratio = now_mark_price/self.last_price-1
            self.last_price = now_mark_price
        except:
            self.log.warning(f"MH现货depth:{depth} {traceback.format_exc()}")

    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    async def on_depth(self, content):
        # bid_num = len(content['bids'])
        # ask_num = len(content['asks'])
        self.log.info(f"内盘现货depth:{content}")
        if content['bids'] != []:
            self.spot_bid = [[float(i['price']), float(i['number'])] for i in content['bids']]
        else:
            self.spot_bid = []
        if content['asks'] != []:
            self.spot_ask = [[float(i['price']), float(i['number'])] for i in content['asks']]
        else:
            self.spot_ask = []
        self.log.info(f"spot bid_one:{self.spot_bid} ask_one:{self.spot_ask}")
        try:
            now_mark_price = (self.spot_bid[0][0] + self.spot_ask[0][0])/2
            self.ratio = now_mark_price/self.last_mark_price-1
            self.last_mark_price = now_mark_price
        except:
            self.log.info(f"现货盘口数据:{content}")
        

    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''
    async def check_info(self):
        open_orders = await self.rest.get_currentList(self.symbol, limit=10000, direct='prev', quan=True)
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
        self.log.info(f"一档价差:{round(min_sell-max_buy, self.precision.price)} {diff_ratio}% "
                      f"buy档位数:{buy_num} sell档位数:{sell_num} min_sell:{min_sell} max_buy:{max_buy}")
        
    # 风控模块
    async def risk(self):
        try:
            # =====测试数据======
            self.base_balance = 200000
            # =====测试数据======

            pos_data = await self.rest.get_position(self.symbol, quan=True)
            # print(f"持仓信息:{pos_data}")
            net_pos = 0         # 净持仓
            buy_price_avg = 0   # 无持仓
            sell_price_avg = 0  # 无持仓
            for i in pos_data:
                if i.symbol == self.symbol:
                    if i.type == 1:
                        net_pos += i.amount
                        buy_price_avg = i.open_price_avg
                        self.long_pos += i.amount*self.symbol_unit  # 币的数量
                    else:
                        net_pos -= i.amount
                        sell_price_avg = i.open_price_avg
                        self.short_pos -= i.amount*self.symbol_unit
            # 净持仓均价&方向
            if net_pos > 0:
                self.pos_price = buy_price_avg
                self.net_pos_side = 'long'
            elif net_pos < 0:
                self.pos_price = sell_price_avg
                self.net_pos_side = 'short'
            else:
                self.pos_price = 0
                self.net_pos_side = 0
            self.log.info(f"持仓均价:多单:{buy_price_avg} 空单:{sell_price_avg}\n净持仓:{net_pos} 多单:{self.long_pos} 空单:{self.short_pos}")
            await self.update_deviation(net_pos)
        except Exception as e:
            self.log.warning(f"风控异常:{traceback.format_exc()}")
            
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''

    # 定时驱动重新铺单
    async def main(self):
        '''
            策略思路:
            1、根据eth价格波动*放大系数,确定合约的标记价格
            2、当多空都有持仓时,价格一直保持在净持仓方向平仓时可以盈利的价格区间内
            3、铺单量根据持仓情况做调整,并维持一个最小持仓量
            做市脚本760行,更新价格逻辑
        '''
        # 波动太小不下单
        if abs(self.ratio) <= self.re_mm_tatio and not self.first_run:
            self.log.info(f"波动{self.ratio}<{self.re_mm_tatio},不下单")
            return
        self.first_run = False
        
        ask_orderbook, bid_orderbook = await self.get_orders()
        open_buy_list = []      # buy当前委托
        open_sell_list = []     # sell当前委托
        to_cancel_list = []     # 保存撤单的order_id
        
        # 计算标记价格
        mark_price = self.count_mark_price(self.ratio)
        self.log.info(f"最新标记价格:{mark_price}")

        for _, order in bid_orderbook.items():
            if order.price > mark_price:
                to_cancel_list.append(order.order_id)
            else:
                open_buy_list.append([order.price, order.order_id, order.amount])
        for _, order in ask_orderbook.items():
            if order.price < mark_price:
                to_cancel_list.append(order.order_id)
            else:
                open_sell_list.append([order.price, order.order_id, order.amount])
        
        if to_cancel_list:
            await self.cancel_orders(to_cancel_list, '因价格撤近端')

        # 计算挂单量
        buy_budget, sell_budget = await self.bid_ask_budget()
        sell_target_cum_qty = 0         # sell当前档位之前所有档的累计挂单量
        sell_sending_sum_qty = 0
        buy_target_cum_qty = 0          # buy当前档位之前所有档的累计挂单量
        buy_sending_sum_qty = 0
        sell_target_add = 1 / self.place_num * sell_budget     # 每一档挂单量
        buy_target_add = 1 / self.place_num * buy_budget       # 每一档挂单量
        to_sell_create_orders = {}      # sell本次循环要新挂的订单
        to_buy_create_orders = {}       # buy本次循环要新挂的订单
        to_cancel_orders = []           # buy和sell放一起
        cancel_buy_list = []
        cancel_sell_list = []
        buy_price_last = 0              # 上一次buy的价格
        sell_price_last = 0             # 上一次sell的价格

        # 计算每档价格+委托量,去挂单
        for num in range(1, self.place_num+1):
            # buy_shape是根据持仓调整价差;range是根据价格波动调整价差
            buy_price = mark_price*(1-self.bid_offset) - (num / (self.place_num-1))*(mark_price-mark_price*(1-self.step_ratio))
            sell_price = mark_price*(1+self.ask_offset) + (num / (self.place_num-1))*(mark_price*(1+self.step_ratio)-mark_price)
            #print(f"mark_price:{mark_price}=====buy_price:{buy_price}=====sell_price:{sell_price} ===========")
            #print(self.bid_offset, self.ask_offset, num / (self.place_num-1), mark_price-mark_price*(1-self.step_ratio))
            buy_target_cum_qty = (num + 0.5*self.place_num) / (self.place_num + 0.5*self.place_num) * buy_budget  # buy累计的挂单量
            sell_target_cum_qty = (num + 0.5*self.place_num) / (self.place_num + 0.5*self.place_num) * sell_budget  # sell累计的挂单量
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
                to_sell_qty = random.randint(int(sell_gap*0.1), int(sell_gap*2))    # 测试数据
                price = sell_price
                sell_sending_sum_qty += to_sell_qty
                if price not in ask_orderbook and \
                    len(ask_orderbook) < int(self.place_num):
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
                to_buy_qty = random.randint(int(buy_gap*0.1), int(buy_gap*2))    # 测试数据
                price = buy_price
                buy_sending_sum_qty += to_buy_qty
                if price not in bid_orderbook and \
                    len(bid_orderbook) < int(self.place_num):
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
            await self.cancel_orders(to_cancel_orders, '因数量撤近端')
        
        # 下单
        # buy = list(to_buy_create_orders.keys())
        # sell = list(to_sell_create_orders.keys())
        # sorted(buy, reverse=True)
        # sorted(sell, reverse=False)
        # print(f"\n下单数量buy:{len(buy)} {buy}\nsell:{len(sell)} {sell}\n")
        if to_buy_create_orders or to_sell_create_orders:
            await asyncio.gather(self.make_order(to_buy_create_orders, 'BUY'), \
                                 self.make_order(to_sell_create_orders, 'SELL'))
        
        # 撤远单:
        cancel_buy_list = []
        cancel_sell_list = []
        cancel_list = []
        # 按数量撤单
        # 在open buy list 中, if order num按price排序(从大到小),剔除后 self.place_num* 1.3名外的订单
        # 在open sell list 中,if order num按price排序(从小到大),剔除后 self.place_num* 1.3名外的订单
        sorted(open_buy_list, key=lambda x: x[0], reverse=True)     # 从大到小排序
        sorted(open_sell_list, key=lambda x: x[0], reverse=False)  # 从小到大排序
        for i in open_buy_list[int(self.place_num/2*1.3):]:
            cancel_list.append(i[1])
            cancel_buy_list.append(i)
        for i in open_sell_list[int(self.place_num/2*1.3):]:
            cancel_list.append(i[1])
            cancel_sell_list.append(i)
        
        if cancel_list:
            await self.cancel_orders(cancel_list, '撤远端')
        try:
            max_buy = max(to_buy_create_orders.keys())
            min_sell = min(to_sell_create_orders.keys())
            self.mark_price = (max_buy+min_sell)/2  # 取中间价为标记价格
        except:
            pass

    # 下单函数
    async def make_order(self, grid_list, side, tag=''):
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        for price, vol in grid_list.items():
            vol = int(vol/self.symbol_unit)
            try:
                self.log.info(f"{tag}开仓信息:{side} 价:{price} 量:{vol}")
                #continue
                res = await self.rest.order_create(self.symbol, od_type=od_type, \
                                            price=price, amount=vol, \
                                            precision=self.precision, quan=True)
                self.log.info(f"{tag}开仓信息:{side} 价:{price} 量:{vol} 回报:{res}")
            except Exception as e:
                self.log.warning(f"{tag} {self.symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")
    
    async def close_order(self, price, vol, side):
        if vol == 0:
            return
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        try:
            self.log.info(f"平仓信息:{side} 价:{price} 量:{vol}")
            #return
            res = await self.rest.order_create(self.symbol, od_type=od_type, \
                                                price=price, amount=vol, \
                                                precision=self.precision, contract_type='close', quan=True)
            self.log.info(f"平仓信息:{side} 价:{price} 量:{vol} 平仓回报:{res}")
        except:
            self.log.warning(f"{self.symbol}-{side}-{price}-{vol} 平仓错误: {traceback.format_exc()}")


def main(path='single_mm_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()
    while True:
        time.sleep(100)


if __name__ == '__main__':
    main()



