'''
    实时跟单策略
'''

import sys
import time
import copy
import datetime
import importlib
import traceback
import asyncio
import numpy as np
import follow_hedge_config
sys.path.append('../..')
from utils import Toolbox as tb
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

from crypto_center.client.rest.okex import contract as okx_rest


strategy_name = "follow_hedge实时对冲策略"

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, id, config):
        super().__init__(scheduler=True, gcc=True)
        self.id = int(id)
        self.loop = asyncio.get_event_loop()
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1

        self._load_config(config)   # 读取配置
        self._initParams()          # 初始化参数
        self._setLocalDict()        # 配置本地数据
        
        self.wss_log = tb.Log(f'wss_log/{self.id}_wss_data.log')
        self.deal_log = tb.Log(f'deal_log/{self.id}_hedge_deals.log')
        
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))  # CHECK self.tag
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号     # CHECK 账户
        self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])

    def _initParams(self):
        """初始化参数
        """
        self.symbols_markprice = {}         # 保存内盘标记价格
        self.contract_unit = {}             # 保存内盘合约单位
        self.symbols_bid_ask = {}           # 保存外盘wss推送来的一档价格
        self.deals_dict = {}                # 保存内盘成交数据
        self.hedge_symbol_precision = {}    # 保存交易对精度
        self.symbol_precision = {}          # 保存ok交易对精度+面值
        self.new_orders = {}                # 保存未完成的对冲单
        self.unhedge_vol_dict = {}          # 保存未对冲量
        self.hedge_clock = False            # 对冲锁
        self.check_hedge_clock = False      # 追单锁
        self.hedge_pos = {}                 # 保存对冲端仓位
        # self.id = list(self.leads.keys())[0]  # 保存用户id
        self.uid = int(self.leads[self.id])      # 带单员UID列表
        self.this_wss_data = ""
    
    def _setLocalDict(self):
        """配置本地数据
        """
        self._local_deals = tb.LocalDict('local_follow_data.log')
        self.local_hedge_pos = self._local_deals.load().get('hedge_pos', {})    # 保存所有成交聚合数据
    
    async def on_first(self):
        self.log.add(f"log/{self.id}_follow_hedge.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_hedge_message))
        await asyncio.sleep(0.1)
        await self.hedge_contract_info()    # 更新币对信息
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/1"))  # 每1s执行一次
        self.schedule.add_job(self.check_hedge_order, CronTrigger(second="*/1"))
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*/5"))  # 每5min执行一次
    
    # 订阅内盘标记价格 + okx一档价格    测试成功
    async def sub_bid_ask(self, symbol):
        await self.redis_conn.sub_channel(f"contract.bids_asks.{symbol}")
        self.log.info(f"subscribe {symbol}: 最优挂单")
    
    async def reload_config(self):
        try:
            importlib.reload(follow_hedge_config)
            [setattr(self, k, v) for k, v in vars(follow_hedge_config).items()]
        except:
            self.log.warning(f"reload_config报错{traceback.format_exc()}")

    # rest获取对冲端交易对详情
    async def hedge_contract_info(self):
        res = await self.okx_rest.fetch_precision()
        for s, v in res.items():
            price_precision = int(-np.log10(v['price']))    # 价格精度
            amount_precision = int(-np.log10(v['amount']))  # 数量精度
            face_value = v['faceValue']  # 合约面试,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']
            self.symbol_precision[s] = (price_precision, amount_precision, face_value, hedge_vol_limit)
        print(f"交易对精度+面值:{self.symbol_precision}")
    
    # 发送报警信息    测试成功
    async def send_warning(self, title, warning_mess):
        try:
            self.log.error(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            self.log.warning(f"send_warning发送报警信息失败:{traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    # 接收订阅的外盘一档价格    测试成功
    async def on_hedge_message(self, channel: str, item: dict):
        # print('redis订阅信息:', channel, item)
        if 'bids_asks' in channel and item['messageType'] == 'message':
            symbol = channel.split('.')[2]
            self.symbols_bid_ask[symbol] = [item['bid'], item['ask']]
        # print(f"redis订阅一档价格:{self.symbols_bid_ask}")
    
    # 获取对冲端一档最新价格    测试成功
    async def get_price(self, symbol, side):
        while True:
            try:
                price = self.symbols_bid_ask[symbol][1] if side == 'buy' else self.symbols_bid_ask[symbol][0]
                return price
            except:
                self.log.error(f"获取{symbol}最新价格失败{traceback.format_exc()}")
                await self.sub_bid_ask(symbol)
            await asyncio.sleep(0.5)

    # 内盘wss成交推送
    async def on_adl(self, content):
        # self.log.info(f"ws成交推送数据:{content}")
        self.wss_log.write(f"ws成交推送数据:{content}")
        notice = f"{strategy_name} 有新推送,检查判断条件是否正常"
        tb.warning(notice, 'notice')
        if content['tag'] == self.tag and content['userId'] == self.uid and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.deal_wss(dict(content))
        
    # 内盘成交处理
    async def deal_wss(self, content):
        symbol = content['market']
        uid = content['userId']
        if 'positionAmt' in content:
            pos = float(content['positionAmt'])
        else:
            pos = 0 # 没有positionAmt字段表示持仓清零
        if symbol in self.deals_dict:
            self.deals_dict[symbol][uid] = pos
        else:
            self.deals_dict[symbol] = {uid: pos}
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self):
        # self.log.info(f"风控线程")
        try:
            # 获取账户信息
            balance = await self.okx_rest.fetch_balance()
            text = f"USDT权益:{balance['USDT']['total']} 可用:{balance['USDT']['free']} 冻结:{balance['USDT']['used']}"
            self.log.info(f"ok账户:{text}")
            # 获取内盘仓位
            while 1:
                temp_hold_list = []
                try:
                    data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id=self.id)
                    hold_list_page = data['data']['pager']['total_page']
                    temp_hold_list += data['data']['data']
                    for n in range(2, hold_list_page+1):
                        data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20, user_id=self.id)
                        temp_hold_list += data['data']['data']
                        await asyncio.sleep(1)
                    break
                except:
                    print(f"获取持仓报错:{traceback.format_exc()}")
                await asyncio.sleep(5)
            base_pos = {}
            for i in temp_hold_list:
                side = 1 if i['openDirection'] == 1 else -1
                if i['symbol'] in base_pos:
                    base_pos[i['symbol']] += float(i['amount'])*side
                else:
                    base_pos[i['symbol']] = float(i['amount'])*side
            self.log.info(f"当前用户持仓:{base_pos}")
            # 获取外盘仓位
            text = 'ok账户持仓:\n'
            hedge_pos = {}
            res = await self.okx_rest.fetch_position()  # 仓位带正负
            for i in res:
                hedge_pos[i['symbol']] = i['contracts']
                text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n\n"
                setattr(self, i['symbol'], i['contracts'])
            self.log.info(text)
            self.hedge_pos = hedge_pos
            # 外盘大于内盘持仓,报警
            return
            for symbol, pos in hedge_pos.items():
                base = base_pos.get(symbol, 0)
                if abs(pos) > abs(base):
                    #self.log.warning(f"风险警告: {symbol}对冲仓位:{pos} 大于 内盘持仓:{base}")
                    title = f"{strategy_name} 内外盘持仓不匹配"
                    #warning_mess = f"{strategy_name} {symbol}合约对冲仓位:{pos} 大于 内盘持仓:{base},立即检查持仓问题!!!"
                    #await self.send_warning(title, warning_mess)
            for symbol, pos in base_pos.items():
                if pos != 0 and symbol not in hedge_pos:
                    self.log.warning(f"风险警告: {symbol}内盘持仓:{pos} 外盘无持仓")
                    title = f"{strategy_name} 内外盘持仓不匹配"
                    warning_mess = f"{strategy_name} {symbol}内盘持仓:{pos} 外盘无持仓,立即检查持仓问题!!!"
                    await self.send_warning(title, warning_mess)
        except:
            self.log.error(f"风险验证报错 {traceback.format_exc()}")
        
    # 减仓对冲单是否完全成交,将未全部成交的订单撤单后,taker成交
    async def check_hedge_order(self):
        # self.log.info(f"追单线程:{self.check_hedge_clock}")
        if self.check_hedge_clock:
            return
        self.check_hedge_clock = True
        new_orders = copy.deepcopy(self.new_orders)
        # self.log.info(f"追单前的new_orders:{new_orders}\nunhedge_vol_dict:{self.unhedge_vol_dict}")
        for k, v in new_orders.items():
            symbol = v[0]
            self.log.info(f"追单ID:{k}")
            if int(time.time()*1000)-v[1] > 10000:
                price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
                # 第一步撤单
                try:
                    cancel_res = await self.okx_rest.cancel_order(symbol, order_id=k)
                    self.log.info(f"撤单 {cancel_res}")
                except:
                    self.log.error(f"撤单报错:{traceback.format_exc()}")
                # 第二步查询未成交量
                while 1:
                    try:
                        res = await self.okx_rest.fetch_order_detail(symbol, order_id=k)
                        self.log.info(f"对冲订单状态:{res}")
                    except:
                        self.log.error(f"查询订单状态报错:{traceback.format_exc()}")
                        asyncio.sleep(0.2)
                        continue
                    # if res['status'] == 'closed':
                    #     self.log.info(f"maker单成交了")
                    # 挂单未成交 TODO amount是张数
                    # {'symbol': 'ADA-USDT', 'id': '2288780619056668672', 'clientOrderId': '', 'price': 0.1, 
                    # 'stopPrice': None, 'triggerPrice': None, 'amount': 0.1, 'amountIsNum': True, 'side': 'buy', 
                    # 'type': 'limit', 'status': 'open', 'leverage': 3.0, 'timeInForce': None, 'postOnly': None, 
                    # 'reduceOnly': False, 'marginMode': 'crossed', 'average': None, 'filled': 0.0, 'cost': None, 
                    # 'remaining': 0.1, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': 
                    # {'feeCurrency': 'USDT', 'cost': 0.0}, 'timestamp': 1740713377883, 'info': 
                    # {'accFillSz': '0', 'algoClOrdId': '', 'algoId': '', 'attachAlgoClOrdId': '', 
                    # 'attachAlgoOrds': [], 'avgPx': '', 'cTime': '1740713377883', 'cancelSource': '', 
                    # 'cancelSourceReason': '', 'category': 'normal', 'ccy': '', 'clOrdId': '', 'fee': '0', 
                    # 'feeCcy': 'USDT', 'fillPx': '', 'fillSz': '0', 'fillTime': '', 'instId': 'ADA-USDT-SWAP', 
                    # 'instType': 'SWAP', 'isTpLimit': 'false', 'lever': '3', 'linkedAlgoOrd': {'algoId': ''}, 
                    # 'ordId': '2288780619056668672', 'ordType': 'limit', 'pnl': '0', 'posSide': 'net', 'px': '0.1', 
                    # 'pxType': '', 'pxUsd': '', 'pxVol': '', 'quickMgnType': '', 'rebate': '0', 'rebateCcy': 'USDT', 
                    # 'reduceOnly': 'false', 'side': 'buy', 'slOrdPx': '', 'slTriggerPx': '', 'slTriggerPxType': '', 
                    # 'source': '', 'state': 'live', 'stpId': '', 'stpMode': 'cancel_maker', 'sz': '0.1', 'tag': '', 
                    # 'tdMode': 'cross', 'tgtCcy': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 
                    # 'tradeId': '', 'uTime': '1740713377883'}}
                    
                    side = res['side']
                    unhedge_vol = res['remaining'] if side == 'buy' else -res['remaining']  # 未成交量,表示张数,不用处理精度
                    # self.log.info(f"本次对冲:{unhedge_vol} 过往未对冲:{self.unhedge_vol_dict.get(symbol, 0)}")
                    unhedge_vol += self.unhedge_vol_dict.get(symbol, 0)
                    break
                
                # 第三步追单
                if unhedge_vol != 0:
                    try:
                        depth = await self.okx_rest.fetch_depth(symbol)
                    except:
                        self.log.error(f"查询深度报错:{traceback.format_exc()}")
                        continue
                    bid = depth['bids'][0][0]
                    ask = depth['asks'][0][0]
                    
                    if unhedge_vol > 0:
                        price = round(ask*(1+self.slip), price_precision)
                        self.log.info(f"追单参数reorder:{symbol} price:{price} vol:{unhedge_vol} side:{side}")
                        # return
                        try:
                            t1 = time.time()*1000
                            reorder_res = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='buy', amount=unhedge_vol, 
                                                          price=price, tdMode='cross')
                            deal_log = f"{datetime.datetime.now()} 数据来源:{self.this_wss_data} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{unhedge_vol} 方向:buy 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{reorder_res}"
                            self.deal_log.write(deal_log)
                        except:
                            self.log.error(f"reorder error:{traceback.format_exc()} 数据来源:{self.this_wss_data}")
                            self.unhedge_vol_dict[symbol] = unhedge_vol # 下单失败时将未对冲数量记录下来
                            title = f"{strategy_name} 下单失败"
                            warning_mess = f"{strategy_name} check_hedge_order函数下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                            await self.send_warning(title, warning_mess)
                            continue
                    elif unhedge_vol < 0:
                        price = round(bid*(1-self.slip), price_precision)
                        self.log.info(f"追单参数reorder:{symbol} price:{price} vol:{unhedge_vol} side:{side}")
                        # return
                        try:
                            t1 = time.time()*1000
                            reorder_res = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='sell', amount=abs(unhedge_vol), 
                                                          price=price, tdMode='cross')
                            deal_log = f"{datetime.datetime.now()} 数据来源:{self.this_wss_data} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{abs(unhedge_vol)} 方向:sell 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{reorder_res}"
                            self.deal_log.write(deal_log)
                        except:
                            self.log.error(f"reorder error:{traceback.format_exc()} 数据来源:{self.this_wss_data}")
                            self.unhedge_vol_dict[symbol] = unhedge_vol # 下单失败时将未对冲数量记录下来
                            title = f"{strategy_name} 下单失败"
                            warning_mess = f"{strategy_name} check_hedge_order函数下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                            await self.send_warning(title, warning_mess)
                            continue
                    # 测试数据,要把res改成reorder_res
                    # TODO 不能直接修改全局变量,局部变量修改后跳出循环再修改全局变量
                    self.new_orders[reorder_res['id']] = [symbol, 0]    # 时间戳给0,下一次循环立即检查成交情况
                    self.unhedge_vol_dict[symbol] = 0                   # 将未对冲数量清零
                    notice = f"{strategy_name}发生对冲"
                    tb.warning(notice, 'notice')
                # TODO 删除已成交或已撤销的订单,即使下单失败,也会记录在unhedge_vol_dict中
                del self.new_orders[k]
                self.log.info(f"追单后的new_orders:{self.new_orders}\nunhedge_vol_dict:{self.unhedge_vol_dict}\n")
        self.check_hedge_clock = False

    
    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''
    
    async def handle_deals(self):
        # self.log.info(f"对冲线程")
        if self.hedge_clock:    # 正在对冲中
            return
        self.hedge_clock = True
        self.t1 = time.time()*1000
        matchs = copy.deepcopy(self.deals_dict)
        self.this_wss_data = matchs
        # self.log.info(f"要对冲的数据{matchs}")
        for symbol, match in matchs.items():
            # 按交易对并行处理对冲
            if len(match) != 0:
                try:
                    await self.agg_deals(symbol, match)
                except:
                    self.log.error(f"agg报错记录,为了防止多线程警告 {traceback.format_exc()}")
                    tb.warning(f"{strategy_name} {symbol}合约对冲策略agg报错:{traceback.format_exc()}", 'notice')
        self.hedge_clock = False
    
    # 聚合
    async def agg_deals(self, symbol, matchs):
        # 计算聚合成交量
        sum_pos = 0
        for uid, pos in matchs.items():
            sum_pos += pos
        # 多种情况:0——多;0——空;多——0;空——0;多——加多;多——减多;空——加空;空——减空;多——空;空——多
        last_pos = self.local_hedge_pos.get(self.uid, {}).get(symbol, 0)
        hedge_size = sum_pos - last_pos
        
        # 下单
        if hedge_size != 0:
            # 本地化要对冲的数量,若对冲失败,也记录进来。只需要处理对冲失败的问题。不要因为对冲失败就不记录,让后面的仓位去累计对冲。价格可能不好,也不容易做盈亏统计。
            await self.local_save_hedge_pos(uid, symbol, sum_pos)
            # 调整对冲量,大于1则多对冲,小于1则部分对冲
            hedge_size = hedge_size*getattr(self, 'follow_ratio', 1)
            
            self.log.info(f"聚合数据:{matchs}, {symbol}切片持仓:{sum_pos}, 上次持仓:{last_pos}, 对冲数量:{hedge_size}")
            # 判断内外盘持仓是否合理
            ok_pos = self.hedge_pos.get(symbol, 0)
            if abs(ok_pos) > abs(sum_pos) * getattr(self, 'follow_ratio', 1) and len(self.new_orders) == 0:
                self.log.error(f"{symbol}外盘持仓{ok_pos}大于等于内盘持仓{sum_pos},检查持仓")
                # return
            try:
                await self.hedge(symbol, hedge_size)
                self.hedge_clock = False
                self.log.info(f"对冲耗时:{time.time() * 1000 - self.t1}ms")
            except KeyboardInterrupt as e:   # 修改
                self.hedge_clock = False
                self.log.error(f"手动停止 {traceback.format_exc()}")
                raise e
            except:
                self.hedge_clock = False
                self.log.error((f'此条pendingTask处理失败! {traceback.format_exc()}'))
                title = f"{strategy_name}对冲报错"
                warning_mess = f"{strategy_name}{symbol}合约对冲策略hedge报错:hedge函数报错信息:{traceback.format_exc()}"
                await self.send_warning(title, warning_mess)
    
    # 本地化对冲仓位  测试成功
    async def local_save_hedge_pos(self, uid, symbol, pos):
        if uid in self.local_hedge_pos:
            self.local_hedge_pos[uid][symbol] = pos
        else:
            self.local_hedge_pos[uid] = {symbol: pos}
        self.local_hedge_pos = self._local_deals.save(
            {'hedge_pos': self.local_hedge_pos})['hedge_pos']
        self.log.info(f"本地化对冲仓位:{self.local_hedge_pos}")
        
    # 对冲
    async def hedge(self, symbol, vol):
        hedgeMess = ''
        
        # 更新交易对精度
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            title = f"{strategy_name}合约对冲失败,无法获取交易对信息"
            warning_mess = f"{strategy_name} {symbol}在ok没有获取到交易对信息,无法对冲,立即人工介入"
            self.log.warning(warning_mess)
            await self.send_warning(title, warning_mess)
            return

        vol = round(vol, amount_precision)
        hedgeMess += f"需要外盘对冲 {vol}\n"

        price = await self.get_price(symbol, 'buy')
        # 设置最大对冲量
        if abs(vol)*price > self.hedge_amt_limit:
            side = 1 if vol > 0 else -1
            vol = self.hedge_amt_limit/price*side
        
        thisVol = round(vol, amount_precision) if amount_precision > 0 else int(vol)
        hedgeMess += f"总对冲量{thisVol}\n"
        if thisVol == 0.0:
            self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
            return
        # depth = await self.okx_rest.fetch_depth(symbol)
        # bid = depth['bids'][0][0]*(1-self.slip)    # CHECK self.slip
        # ask = depth['asks'][0][0]*(1+self.slip)
        
        # 测试数据
        # thisVol = 0.1  # CHECK 注释测试数据

        if vol > 0:
            price = round(price*(1+self.slip), price_precision)
            hedgeMess += f"交易对:{symbol} 对冲价格:{price}  对冲数量:{thisVol}  方向:buy\n"
            self.log.info(f"最终下单参数:{hedgeMess}")
            # return    # CHECK 注释return
            t1 = time.time()*1000
            try:
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                            side='buy', amount=abs(thisVol), 
                                                            price=price, tdMode='cross')
                deal_log = f"{datetime.datetime.now()} 数据来源:{self.this_wss_data} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{abs(thisVol)} 方向:buy 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
                self.deal_log.write(deal_log)
            except:
                self.log.error(f"hedge error:{traceback.format_exc()} 数据来源:{self.this_wss_data}")
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} hedge函数下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                return
        elif vol < 0:
            price = await self.get_price(symbol, 'sell')
            price = round(price*(1-self.slip), price_precision)
            hedgeMess += f"交易对:{symbol} 对冲价格:{price}  对冲数量:{thisVol}  方向:sell\n"
            self.log.info(f'最终下单参数:{hedgeMess}')
            # return    # CHECK 注释return
            t1 = time.time()*1000
            try:
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                            side='sell', amount=abs(thisVol), 
                                                            price=price, tdMode='cross')
                deal_log = f"{datetime.datetime.now()} 数据来源:{self.this_wss_data} 对冲下单 交易对:{symbol} 对冲价格:{price} 对冲数量:{abs(thisVol)} 方向:sell 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{result}"
                self.deal_log.write(deal_log)
            except:
                self.log.error(f"hedge error:{traceback.format_exc()} 数据来源:{self.this_wss_data}")
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} hedge函数下单失败:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                return
        # {'symbol': 'ADA-USDT', 'result': True, 'id': '2269739899423547392', 'clientOrderId': '', 'timestamp': 1740145920218, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2269739899423547392', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1740145920218'}], 'inTime': '1740145920217640', 'msg': '', 'outTime': '1740145920219807'}}
        hedge_order_id = result['id']
        self.new_orders[hedge_order_id] = [result['symbol'], result['timestamp']]
        # self.log.info(f"对冲下单 {result} 延时:{round(time.time()*1000-t1, 2)}ms hedge_order_id:{hedge_order_id}")
        notice = f"{strategy_name}发生对冲"
        tb.warning(notice, 'notice')


def main(id, path='follow_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(id, config).run()



if __name__ == '__main__':
    id = sys.argv[1]
    main(id)


