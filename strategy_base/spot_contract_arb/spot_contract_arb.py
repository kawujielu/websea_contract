'''
    MH现货-合约,基差套利策略
'''
import sys
import time
import traceback
import importlib
import asyncio
import schedule
import numpy as np
import spot_contract_arb_config
sys.path.append("../..")
from typing import Optional, Dict, List
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.spot import WebseaSpot as ws_spot_rest
from client.env_pro.wss.websea.spot import WebSeaSpot as ws_spot_wss
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
# from client.env_dev.rest.websea_contract import WebseaContract as ws_contract_rest    # 测试环境
# from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss      # 测试环境
from utils.functool import format_float
from template.template_timer import TemplateTimer
import objects.contract_request.websea as ocw
from utils.aio_redis import MyAioredis, MyAioredisFunctools

class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
           
    async def on_first(self):
        self.redis_conn = await self.redis_pool.open()
        await self.cancel_orders(tag='开盘撤单')
        await self.spot_get_precision()      # 获取现货精度
        await self.contract_get_precision()  # 获取合约精度
        await self.get_symbol_unit()         # 获取合约单位
        self.min_amount = min(self.spot_min_amount, self.contract_min_amount*self.symbol_unit)  # 最小成交量

        # spot wss
        self.ws_spot_wss = ws_spot_wss()
        # self.ws_spot_wss.on_trade = self.spot_on_trade
        self.ws_spot_wss.on_depth = self.spot_on_depth
        self.loop.create_task(self.ws_spot_wss.only_subscribe())
        # self.loop.create_task(self.ws_spot_wss.sub_trade(symbol=self.spot_symbol))
        self.loop.create_task(self.ws_spot_wss.sub_depth(symbol=self.spot_symbol))

        # contract wss
        self.ws_contract_wss = ws_contract_wss()
        # self.ws_contract_wss.on_trade = self.contract_on_trade
        self.ws_contract_wss.on_depth = self.contract_on_depth
        self.loop.create_task(self.ws_contract_wss.only_subscribe())
        # self.loop.create_task(self.ws_contract_wss.sub_trade(symbol=self.contract_symbol))
        self.loop.create_task(self.ws_contract_wss.sub_depth(symbol=self.contract_symbol))

        contract_trade = await self.contract_rest.get_trade(self.contract_symbol, size=1, quan=True)
        self.log.info(f"合约最新成交价格:{contract_trade}")
        self.contract_trade = contract_trade[0].price
        # [Trade(symbol='ETH-USDT', ret_ts=1736725398890, id=1736725398577160, 
        #        amount=474.0, price=3262.87, direction='sell', ts=1736725398)]
        spot_trade = await self.spot_rest.get_trade(self.spot_symbol, size=1)
        # self.log.info(f"现货最新成交价格:{spot_trade}")
        self.spot_trade = spot_trade[0]['price']
        
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.get_trades, CronTrigger(second="*/2"))
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))        # 每10s执行一次
        self.schedule.add_job(self.reload_config, CronTrigger(second='00')) # 每分钟的00s执行
    
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.spot_rest = ws_spot_rest(config['spot_token'], config['spot_secret'])
        self.contract_rest = ws_contract_rest(config['contract_token'], config['contract_secret'])

    def _initParams(self):
        """初始化参数
        """
        self.main_lock = False          # 主逻辑锁
        self.buy_close = False          # buy开始平仓
        self.sell_close = False         # sell开仓平仓
        self.only_reduce_buy = False    # buy只平仓
        self.only_reduce_sell = False   # sell只平仓
        self.close_arb = False          # 暂停套利锁
        self.redis_pool = MyAioredis(db=1)
        self.redis_conn: Optional[MyAioredisFunctools] = None

    async def spot_get_precision(self):
        spot_precision = await self.spot_rest.get_precision(self.spot_symbol)
        self.spot_precision = spot_precision[self.spot_symbol]
        # 测试数据
        # self.spot_precision = {'ETH-USDT': {'amount': 4, 'minAmount': 0.002, 'maxAmount': 10000.0, 'price': 2, 'minPrice': 1.0, 'maxPrice': 10000.0}}
        self.spot_min_price_step = 10 ** (-self.spot_precision['price'])
        self.spot_min_amount = self.spot_precision['minAmount']
        self.log.info(f"spot_precision: {self.spot_precision}")
        self.log.info(f"spot_min_price_step: {format_float(self.spot_min_price_step)}")

    async def contract_get_precision(self):
        """更新 币对信息"""
        self.contract_precision = await self.contract_rest.get_precision(self.contract_symbol, quan=True)
        self.contract_min_amount = self.contract_precision.minQuantity
        self.log.info(f"contract_precision: {self.contract_precision}")
        self.contract_min_price_step = 10 ** (-self.contract_precision.price)
        if self.contract_symbol == "BTC-USDT":
            self.contract_min_price_step = 0.1
        self.log.info(f"contract_min_price_step: {format_float(self.contract_min_price_step)}")

    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                data = await self.contract_rest.get_symbols(self.contract_symbol, quan=True)
                # 若走这个逻辑,表示交易对下架
                if not isinstance(data, list):
                    self.symbol_unit = data.contract_size
                    self.log.info(f"symbol_unit:{self.symbol_unit}")
                    break
                else:
                    raise f'{self.contract_symbol}交易对已下架,获取不到合约单位'
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    async def cancel_orders(self, orders=None, tag=''):
        while True:
            try:
                if orders is None:
                    res = await self.contract_rest.order_cancel(symbol=self.contract_symbol, quan=True)
                else:
                    res = await self.contract_rest.order_cancel(order_ids=orders, quan=True)
                self.log.info(f"{tag}cancel回报:{res}")
                break
            except Exception as e:
                self.log.warning(f"{self.base_exchange}撤单错误:{traceback.format_exc()}")
            await asyncio.sleep(1)
    
    def adjust_precision(self, number, unit):
        factor = 10 ** unit
        return int(number * factor) / factor

    async def reload_config(self):
        try:
            importlib.reload(spot_contract_arb_config)
            [setattr(self, k, v) for k, v in vars(spot_contract_arb_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"main循环报错{traceback.format_exc()}")
            except:
                pass 
    
    async def get_trades(self):
        contract_trade = await self.contract_rest.get_trade(self.contract_symbol, size=1, quan=True)
        self.log.info(f"合约最新成交价格:{contract_trade}")
        self.contract_trade = contract_trade[0].price
        # [Trade(symbol='ETH-USDT', ret_ts=1736725398890, id=1736725398577160, 
        #        amount=474.0, price=3262.87, direction='sell', ts=1736725398)]
        spot_trade = await self.spot_rest.get_trade(self.spot_symbol, size=1)
        self.log.info(f"现货最新成交价格:{spot_trade}")
        self.spot_trade = spot_trade[0]['price']
        [{'id': 1736725397321329, 'price': 3264.07, 'amount': 5.1102, 'direction': 'buy', 'ts': 1736725397}]

    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    # async def on_markprice(self, content):
    #     self.markprice = content.markPrice

    async def spot_on_trade(self, content):
        self.log.info(f"spot trade:{content}")
    
    async def contract_on_trade(self, content):
        self.log.info(f"contract trade:{content}")

    async def spot_on_depth(self, content):
        # bid_num = len(content['bids'])
        # ask_num = len(content['asks'])
        # self.log.info(f"内盘现货depth:{content} {bid_num} {ask_num}")
        # {'asks': [{'gear': '1', 'number': '17.6918', 'price': '3226.09'}, 
        #           {'gear': '2', 'number': '0.1399', 'price': '3226.12'}, 
        #           {'gear': '3', 'number': '0.0055', 'price': '3226.13'}, 
        #           {'gear': '4', 'number': '24.1737', 'price': '3226.14'}, 
        #           {'gear': '5', 'number': '0.0045', 'price': '3226.15'}], 
        #  'bids': [{'gear': '1', 'number': '26.3122', 'price': '3226.06'}, 
        #           {'gear': '2', 'number': '10.7442', 'price': '3226.05'}, 
        #           {'gear': '3', 'number': '10.4467', 'price': '3226.02'}, 
        #           {'gear': '4', 'number': '10.8674', 'price': '3225.99'}, 
        #           {'gear': '5', 'number': '1.5238', 'price': '3225.98'}], 
        #  'ts': 1736522769030}
        if content['bids'] != []:
            self.spot_bid = [[float(i['price']), float(i['number'])] for i in content['bids']]
        else:
            self.spot_bid = []
        if content['asks'] != []:
            self.spot_ask = [[float(i['price']), float(i['number'])] for i in content['asks']]
        else:
            self.spot_ask = []
        # self.log.info(f"spot bid_one:{self.spot_bid} ask_one:{self.spot_ask}")
        if not self.main_lock and not self.close_arb:
            try:
                await self.main()
            except:
                self.main_lock = False
                self.log.warning(f"main error:{traceback.format_exc()}")
        
    async def contract_on_depth(self, content):
        # bid_num = len(content.bids)
        # ask_num = len(content.asks)
        # self.log.info(f'内盘合约推送depth:{content} {bid_num} {ask_num}')
        # symbol='ETH-USDT' ts=1736131894401 
        # bids={1: _WssDepthData(gear=1, number=24.0, price=3665.33), 2: _WssDepthData(gear=2, number=64.0, price=3665.32), 3: _WssDepthData(gear=3, number=299.0, price=3665.29), 4: _WssDepthData(gear=4, number=421.0, price=3665.21), 5: _WssDepthData(gear=5, number=32.0, price=3665.16)} 
        # asks={1: _WssDepthData(gear=1, number=43.0, price=3665.38), 2: _WssDepthData(gear=2, number=81.0, price=3665.4), 3: _WssDepthData(gear=3, number=117.0, price=3665.43), 4: _WssDepthData(gear=4, number=421.0, price=3665.51), 5: _WssDepthData(gear=5, number=30.0, price=3665.52)}
        if content.bids is not None:
            temp_bid = []
            for i in range(len(content.bids)):
                temp_bid.append([content.bids[i+1].price, content.bids[i+1].number*self.symbol_unit])
            self.contract_bid = temp_bid
        else:
            self.contract_bid = []
        if content.asks is not None:
            temp_ask = []
            for i in range(len(content.asks)):
                temp_ask.append([content.asks[i+1].price, content.asks[i+1].number*self.symbol_unit])
            self.contract_ask = temp_ask
        else:
            self.contract_ask = []
        # self.log.info(f"contract bid_one:{self.contract_bid} ask_one:{self.contract_ask}")
        if not self.main_lock and not self.close_arb:
            try:
                await self.main()
            except:
                self.main_lock = False
                self.log.warning(f"main error:{traceback.format_exc()}")
        
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''
    # 风控模块
    async def risk(self):
        '''
            现货 合约手续费为0,用户方面手续费最小可以按万2。
            现货 合约的挂单量不一定,资金大概率是无限的,暂时可以
            持仓超出 5000U报警, 1万U开始平仓,超过2万U单边挂单平仓,3万U就先暂停套利
        '''
        try:
            res = await self.contract_rest.get_position(self.contract_symbol, quan=True)
            self.log.info(f"合约持仓:{res}")
            # 现货有固定资金
            res = await self.spot_rest.get_account()
            self.log.info(f"spot balance:{res}")
            for k, v in res.items():
                if v['available'] != 0.:
                    self.log.info(f"spot balance:{k}:{v}")
                    if k == self.spot_symbol.split('-')[1]:
                        if abs(v['available']) < self.init_balance * self.bal_risk_ratio:
                            self.only_reduce_buy  = True
                            mess = f"{self.spot_symbol}基差套利策略,现货资金{k}不足,立即处理"
                            tb.warning(mess, 'risk')
                            tb.sendmail(f'websea基差策略{self.spot_symbol}资金不足', mess)
                        else:
                            self.only_reduce_buy = False
                    if k == self.spot_symbol.split('-')[0]:
                        if abs(v['available']) < self.init_coin * self.bal_risk_ratio:
                            self.only_reduce_sell  = True
                            mess = f"{self.spot_symbol}基差套利策略,现货资金{k}不足,立即处理"
                            tb.warning(mess, 'risk')
                            tb.sendmail(f'websea基差策略{self.spot_symbol}资金不足', mess)
                        else:
                            self.only_reduce_sell = False

            # 测试数据
            # res = await self.spot_rest.order_create(symbol=self.spot_symbol, \
            #                                         precision=self.spot_precision, \
            #                                         order_type='buy-limit', amount=1, price=0.1)
            # print('下单回报:',res)
            # 持仓
            self.net_pos = 0
            self.buy_pos = 0
            self.sell_pos = 0
            res = await self.contract_rest.get_position(self.contract_symbol, quan=True)
            self.log.info(f"合约持仓:{res}")
            # 标准化信息
            # [Position(symbol='CRV-USDT', userId=14114, type=1, is_full=1, lever_rate=5, mark_price=None, 
            # open_price_avg=0.7869027157231197, amount=1220567423, avail_amount=1220567423, contract_frozen=0, 
            # profit=0.0, un_profit=0.0, bood=167105129.5906, avail=21039992.28387178, equity=21039992.28387178, 
            # settle_rate=None, risk_rate=None, liquidation_price=None), 
            # Position(symbol='CRV-USDT', userId=14114, type=2, is_full=1, lever_rate=5, mark_price=None, 
            # open_price_avg=1.103866023221077, amount=14922781008, avail_amount=14922781008, contract_frozen=0, 
            # profit=0.0, un_profit=0.0, bood=9187724698.9959, avail=21039992.28387178, equity=21039992.28387178, 
            # settle_rate=None, risk_rate=None, liquidation_price=None)]
            # 原始信息
            # {"errno":0,"errmsg":"\u6210\u529f","result":[
            #     {"type":1,"userId":14114,"symbol":"CRV-USDT","lever_rate":5,"amount":"42732396",
            #      "profit":"0","open_price_avg":"0.786999000266682889","bood":"6716287.0919",
            #      "avail_amount":"42732396","contract_frozen":"0","settle_rate":null,"equity":"0",
            #      "avail":null,"risk_rate":null,"liquidation_price":null,"un_profit":"0","is_full":1,
            #      "mark_price":null},
            #      {"type":2,"userId":14114,"symbol":"CRV-USDT","lever_rate":5,"amount":"2890795707",
            #       "profit":"0","open_price_avg":"0.988949887920590607","bood":"666385947.2723",
            #       "avail_amount":"2890795707","contract_frozen":"0","settle_rate":null,"equity":"0",
            #       "avail":null,"risk_rate":null,"liquidation_price":null,"un_profit":"0","is_full":1,
            #       "mark_price":null}]}
            for pos in res:
                if pos.type == 1:
                    self.net_pos += int(pos.avail_amount)
                    self.buy_pos = int(pos.avail_amount)
                else:
                    self.net_pos -= int(pos.avail_amount)
                    self.sell_pos = int(pos.avail_amount)

            '持仓超出 5000U报警, 1万U开始平仓,超过2万U单边挂单平仓,3万U就先暂停套利'
            
            pos = self.pos_risk/self.contract_trade/self.symbol_unit  # 张数 = 金额/价格/合约单位
            
            # 开始平仓
            self.buy_close = True if self.buy_pos > pos else False
            self.sell_close = True if self.sell_pos > pos else False
            # 只平仓
            self.only_reduce_buy  = True if self.sell_pos > pos*2 else False
            self.only_reduce_sell  = True if self.buy_pos > pos*2 else False
            if self.net_pos > pos:
                mess = f"{self.spot_symbol}基差套利策略,净持仓达到{self.net_pos}张,超过阈值{pos}张"
                tb.warning(mess, 'risk')
                tb.sendmail(f'websea基差策略{self.spot_symbol}持仓过大', mess)
            # 暂停套利
            if abs(self.net_pos) > pos*3 and not self.close_arb:
                mess = f"{self.spot_symbol}基差套利策略,净持仓达到{self.net_pos}张,超过阈值{pos*3}张,暂停套利"
                tb.warning(mess, 'risk')
                tb.sendmail(f'websea基差策略{self.spot_symbol}暂停套利', mess)
                self.close_arb = True
                # 全撤
                res = await self.cancel_orders()
                self.log.info(f"合约全撤:{res}")
                res = await self.spot_rest.order_cancel(self.spot_symbol)
                self.log.info(f"现货全撤:{res}")
            else:
                self.close_arb = False
            self.log.info(f"净持仓:{self.net_pos} 多单持仓:{self.buy_pos} 空单持仓:{self.sell_pos} 开始平仓:{self.buy_close} {self.sell_close} 只平仓:{self.only_reduce_buy} {self.only_reduce_sell} 暂停套利:{self.close_arb}")
            
        except Exception as e:
            self.log.warning(f"risk error:{traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' ================================== funcation =============================='''
    ''' ==========================================================================='''
    # 判断买卖盘第几档满足最小成交量
    def count_min_vol(self, bid_list, ask_list):
        bid_sum, ask_sum = 0, 0
        bid_num, ask_num = 0, 0
        num = min(len(bid_list), len(ask_list))
        for i in range(num):
            bid_sum += bid_list[i][1]
            if bid_sum > self.min_amount:
                bid_num = i
                break
        for i in range(num):
            ask_sum += ask_list[i][1]
            if ask_sum > self.min_amount:
                ask_num = i
                break
        return bid_num, ask_num
    
    # 判断套利价格可以到第几档
    def count_spread(self, bid_list, ask_list):
        last_ratio = 0
        for i in range(min(len(bid_list), len(ask_list))):
            ratio = (bid_list[i]-ask_list[i])/bid_list[i]
            con = ratio > self.arb_ratio_limit   # (self.spot_fee+self.contract_fee)
            if not con:
                return i, last_ratio if last_ratio else ratio
            else:
                last_ratio = ratio
        return min(len(bid_list), len(ask_list)), last_ratio
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''

    async def main(self):
        self.main_lock = True
        t1 = time.time()

        # 判断是否有数据
        if not self.spot_bid or not self.contract_bid or not self.spot_ask or not self.contract_ask:
            self.log.info('spot and contract depth is None')
            self.main_lock = False
            return
                
        spot_bid_price = [i[0] for i in self.spot_bid]
        spot_ask_price = [i[0] for i in self.spot_ask]
        contract_bid_price = [i[0] for i in self.contract_bid]
        contract_ask_price = [i[0] for i in self.contract_ask]

        # 计算公允价格
        # 只根据depth计算公允价格
        max_spot_ask_price = max(spot_ask_price) if spot_ask_price else 0
        max_contract_ask_price = max(contract_ask_price) if contract_ask_price else 0
        min_spot_bid_price = min(spot_bid_price) if spot_bid_price else min(contract_bid_price)
        min_contract_bid_price = min(contract_bid_price) if contract_bid_price else min(spot_bid_price)
        max_ask_price = max(max_spot_ask_price, max_contract_ask_price)
        min_bid_price = min(min_spot_bid_price, min_contract_bid_price)
        pair_depth = (max_ask_price+min_bid_price)/2
        
        # 根据trade+dept计算公允价格
        pair_trade = (self.spot_trade+self.contract_trade)/2
        max_price_step = min(self.contract_precision.price, self.spot_precision['price'])
        pair_price = round((pair_depth+pair_trade)/2, max_price_step)
        # self.log.info(f"depth价格:{pair_depth} trade价格:{pair_trade} 公允价格:{pair_price} 精度:{max_price_step}")

        # 存redis
        # await self.redis_conn.hset(
        #     self.spot_symbol,
        #     key="spot_contract_arb",
        #     value={"fair_price": pair_price,}
        # )

        # 判断买卖盘第几档满足最小成交量
        vol_bid_num1, vol_ask_num1 = self.count_min_vol(self.contract_bid, self.spot_ask)
        vol_bid_num2, vol_ask_num2 = self.count_min_vol(self.spot_bid, self.contract_ask)
        # self.log.info(f"arb1:{vol_bid_num1},{vol_ask_num1}  arb2:{vol_bid_num2}, {vol_ask_num2}")

        # 计算多少档有套利空间,一次吃到位
        price_num1, ratio = self.count_spread(contract_bid_price, spot_ask_price)
        price_num2, ratio2 = self.count_spread(spot_bid_price, contract_ask_price)
        ratio = round(ratio*100, 2)
        ratio2 = round(ratio2*100, 2)
        self.log.info(f"arb1:{ratio}% arb2:{ratio2}%")
        
        # 判断是否满足套利条件。只要有价格位置,就肯定有套利空间
        con1 = True if price_num1 > 0 and price_num1 > max(vol_bid_num1, vol_ask_num1) else False
        con2 = True if price_num2 > 0 and price_num2 > max(vol_bid_num2, vol_ask_num2) else False
        # self.log.info(f"con1:{con1} {price_num1} {vol_bid_num1}, {vol_ask_num1}")
        # self.log.info(f"con2:{con2} {price_num2} {vol_bid_num2}, {vol_ask_num2}")
        
        if con1:
            # 判断是否大于最小下单量
            bid_vol = sum(np.array(self.contract_bid)[:(price_num1), 1].tolist())
            ask_vol = sum(np.array(self.spot_ask)[:(price_num1), 1].tolist())
            arb_vol = min(ask_vol, bid_vol, self.buy_pos) if self.buy_close else min(ask_vol, bid_vol)
            spot_price = spot_ask_price[price_num1-1]
            contract_price = contract_bid_price[price_num1-1]
            if arb_vol * contract_ask_price > self.deal_limit:
                self.log.info(f"arb1:{ratio}%  arb_vol:{arb_vol} 超过最大下单金额{self.deal_limit}")
                return
            if self.only_reduce_buy:
                await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'BUY'), \
                                     self.close_order(contract_price, arb_vol/self.symbol_unit, 'SELL'))
            else:
                if not self.buy_close:
                    await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'BUY'), \
                                         self.contract_make_order(contract_price, arb_vol/self.symbol_unit, 'SELL'))
                else:
                    await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'BUY'), \
                                         self.close_order(contract_price, arb_vol/self.symbol_unit, 'SELL'))
            
            mess = f'arb1:{ratio}%  spot buy:{spot_price}  contract sell:{contract_price} vol:{arb_vol}'
        elif con2:
            # 判断是否大于最小下单量
            bid_vol = sum(np.array(self.spot_bid)[:(price_num2+1), 1].tolist())
            ask_vol = sum(np.array(self.contract_ask)[:(price_num2+1), 1].tolist())
            arb_vol = min(bid_vol, ask_vol, self.sell_pos) if self.sell_close else min(bid_vol, ask_vol)
            spot_price = spot_bid_price[price_num2-1]
            contract_price = contract_ask_price[price_num2-1]
            # hedge_vol2 = round(arb_vol*(1+self.contract_fee), self.binance_qty_digital)
            if self.only_reduce_sell:
                await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'SELL'), \
                                     self.close_order(contract_price, arb_vol/self.symbol_unit, 'BUY'))
            else:
                if not self.sell_close:
                    await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'SELL'), \
                                         self.contract_make_order(contract_price, arb_vol/self.symbol_unit, 'BUY'))
                else:
                    await asyncio.gather(self.spot_make_order(spot_price, arb_vol, 'SELL'), \
                                         self.close_order(contract_price, arb_vol/self.symbol_unit, 'BUY'))
            mess = f'arb2:{ratio2}%  contract buy:{contract_price}  spot sell:{spot_price} vol:{arb_vol}'
        else:
            self.log.info('no arb opportunity')
            self.main_lock = False
            return

        self.log.info(f"套利信息:{mess} 计算耗时:{(time.time()-t1)*1000}ms")
        self.main_lock = False

    async def spot_make_order(self, price, vol, side, tag=''):
        od_type = 'buy-limit' if side == 'BUY' else 'sell-limit'
        try:
            self.log.info(f"{tag}spot开仓信息:{side} 价:{price} 量:{vol}")
            res = await self.spot_rest.order_create(symbol=self.spot_symbol, \
                                                    precision=self.spot_precision, \
                                                    order_type=od_type, amount=vol, price=price)
            self.log.info(f"spot{tag} 开仓下单信息:{side} 价:{price} 量:{vol} 回报:{res}")
        except:
            self.log.warning(f"spot{tag} {self.spot_symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")

    async def contract_make_order(self, price, vol, side, tag=''):
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        try:
            self.log.info(f"{tag}contract开仓信息:{side} 价:{price} 量:{vol}")
            res = await self.contract_rest.order_create(self.contract_symbol, od_type=od_type, \
                                        price=price, amount=vol, \
                                        precision=self.contract_precision, quan=True)
            self.log.info(f"contract{tag} 开仓下单信息:{side} 价:{price} 量:{vol} 回报:{res}")
        except Exception as e:
            self.log.warning(f"contract{tag} {self.contract_symbol}-{side}-{price}-{vol} 下单错误: {traceback.format_exc()}")
    
    async def close_order(self, price, vol, side):
        if vol == 0:
            return
        od_type = ocw.OrderType.buy_limit if side == 'BUY' else ocw.OrderType.sell_limit
        try:
            self.log.info(f"contract 平仓下单信息:{side} 价:{price} 量:{vol}")
            res = await self.contract_rest.order_create(self.contract_symbol, od_type=od_type, \
                                                price=price, amount=vol, \
                                                precision=self.contract_precision, contract_type='close', quan=True)
            self.log.info(f"contract 平仓下单信息:{side} 价:{price} 量:{vol} 平仓回报:{res}")
        except:
            self.log.warning(f"{self.contract_symbol}-{side}-{price}-{vol} 平仓错误: {traceback.format_exc()}")

def main(path='spot_contract_arb_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()


if __name__ == '__main__':
    main()



