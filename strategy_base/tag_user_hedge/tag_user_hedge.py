'''
    策略逻辑：
    订阅内盘指定标签组用户的成交数据
    推送数据记录在字典中
    每1s读取数据做聚合去OK合约对冲
    实时完全对冲
'''

import sys
import time
import datetime
import requests
import traceback
import asyncio
import json
import numpy as np
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

# sys.path.append('/home/ubuntu/crypto_center/client/rest/okex')
# import contract as okx_rest

from crypto_center.client.rest.okex import contract as okx_rest


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__()
        self.loop = asyncio.get_event_loop()

        self._load_config(config)  # 读取配置
        self._initParams()    # 初始化参数
        self._setLocalDict()  # 配置本地数据
        
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest1 = ws_contract_rest(config['base_token'], config['base_secret'])
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
        self.thisVol = 0                    # 本次下单量
        self.hedge_clock = False            # 对冲锁
        self.risk_clock = False             # 风控锁
        self.exposeRiskAmt = getattr(self, 'exposeRiskAmt', 5000*1.1)   # 净敞口报警

    def _setLocalDict(self):
        """配置本地数据
        """
        pass
    
    async def on_first(self):
        await self.hedge_contract_info()       # 更新币对信息
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/2"))  # 每1s执行一次 
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    
    async def on_ticker_order(self, content):
        # print(f"外盘一档数据:{content}")
        symbol = content['symbol']
        bid = content['bid_price']
        ask = content['ask_price']
        bid_vol = content['bid_qty']
        ask_vol = content['ask_qty']
        self.symbols_bid_ask[symbol] = [[bid, bid_vol], [ask, ask_vol]]
        # print(f"外盘一档价格:{self.symbols_bid_ask}")

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        # 开仓挂单
        {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'side': 'BUY', 'origQty': 1, 'cumfilledSize': '', 'userId': 55970796, 'market': 'ADA-USDT', 'uid': '', 'cumfilledVol': '', 'price': '0.80258', 'lastfilledprice': '', 'tag': 'J', 'status': 'NEW', 'direction': 'MANY'}
        # 开仓全部成交
        {'lastfilledVolume': 0.8, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0401', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '+1', 'markPrice': '0.80232', 'price': '0.80258', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117169102, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80257', 'cumfilledVol': '0.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0002', 'marginType': 'full', 'lastfilledprice': '0.80257', 'status': 'FILLED'}
        # 开多加仓委托
        {'lastfilledVolume': '', 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0400', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '+1', 'markPrice': '0.80116', 'price': '0.80160', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117769912, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80257', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0014', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 开多加仓全部成交
        {'lastfilledVolume': 0.8, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0801', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '+2', 'markPrice': '0.80116', 'price': '0.80160', 'tag': 'J', 'direction': 'MANY', 'side': 'BUY', 'origQty': 1, 'positionSide': 'LONG', 'updateTime': 1740117769932, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80201', 'cumfilledVol': '0.80', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0017', 'marginType': 'full', 'lastfilledprice': '0.80145', 'status': 'FILLED'}
        # 平多挂单
        {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0798', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '+2', 'markPrice': '0.79829', 'price': '', 'tag': 'J', 'direction': 'MANY', 'side': 'SELL', 'origQty': 2, 'positionSide': 'LONG', 'updateTime': 1740119303294, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79844', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0003', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 平多全部成交
        {'lastfilledVolume': 1.59, 'orderType': 'market', 'lastfilledSize': 2, 'side': 'SELL', 'origQty': 2, 'cumfilledSize': 2, 'userId': 55970796, 'market': 'ADA-USDT', 'uid': 100022, 'cumfilledVol': '1.59', 'price': '', 'lastfilledprice': '0.79793', 'tag': 'J', 'status': 'FILLED', 'direction': 'MANY'}

        # 开空挂单
        {'lastfilledVolume': '', 'orderType': 'limit', 'lastfilledSize': '', 'side': 'SELL', 'origQty': 1, 'cumfilledSize': '', 'userId': 55970796, 'market': 'ADA-USDT', 'uid': '', 'cumfilledVol': '', 'price': '0.79798', 'lastfilledprice': '', 'tag': 'J', 'status': 'NEW', 'direction': 'SPACE'}
        # 开空全部成交
        {'lastfilledVolume': 0.79, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 1, 'isolatedMargin': '0.0399', 'liquidationPrice': '--', 'cumfilledSize': 1, 'uid': 100022, 'positionAmt': '-1', 'markPrice': '0.79845', 'price': '0.79798', 'tag': 'J', 'direction': 'SPACE', 'side': 'SELL', 'origQty': 1, 'positionSide': 'SHORT', 'updateTime': 1740119045121, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79822', 'cumfilledVol': '0.79', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0002', 'marginType': 'full', 'lastfilledprice': '0.79822', 'status': 'FILLED'}
        # 平空挂单
        {'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.0399', 'liquidationPrice': '--', 'cumfilledSize': '', 'uid': '', 'positionAmt': '-1', 'markPrice': '0.79854', 'price': '', 'tag': 'J', 'direction': 'SPACE', 'side': 'BUY', 'origQty': 1, 'positionSide': 'SHORT', 'updateTime': 1740119094534, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79822', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0003', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}
        # 平空全部成交
        {'lastfilledVolume': 0.79, 'orderType': 'market', 'lastfilledSize': 1, 'side': 'BUY', 'origQty': 1, 'cumfilledSize': 1, 'userId': 55970796, 'market': 'ADA-USDT', 'uid': 100022, 'cumfilledVol': '0.79', 'price': '', 'lastfilledprice': '0.79884', 'tag': 'J', 'status': 'FILLED', 'direction': 'SPACE'}

        # 逐仓开空全部成交
        {'lastfilledVolume': 2.39, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 3, 'isolatedMargin': '0.1197', 'liquidationPrice': '0.83417', 'cumfilledSize': 3, 'uid': 100022, 'positionAmt': '-3', 'markPrice': '0.79863', 'price': '0.79815', 'tag': 'J', 'direction': 'SPACE', 'side': 'SELL', 'origQty': 3, 'positionSide': 'SHORT', 'updateTime': 1740119888632, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.79845', 'cumfilledVol': '2.39', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0005', 'marginType': 'isolated', 'lastfilledprice': '0.79845', 'status': 'FILLED'}

        if content['tag'] == self.tag and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.deal_wss(dict(content))
        
    # 内盘成交处理
    async def deal_wss(self, content):
        symbol = content['market']
        if symbol in self.deals_dict:
            self.deals_dict[symbol].append(content)
        else:
            self.deals_dict[symbol] = [content]

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
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self):
        if self.risk_clock:
            return
        self.risk_clock = True
        self.log.info(f"风控线程")
        try:
            # 获取账户信息
            # balance = await self.okx_rest.fetch_balance()
            # print(f"ok账户权益:{balance}")
            # 获取外盘仓位
            hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
            self.log.info(f"ok账户持仓:{hedge_pos}")
        except:
            self.log.error(f"风险验证报错 {traceback.format_exc()}")
        self.risk_clock = False
    
    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''
    
    async def handle_deals(self):
        if self.hedge_clock:    # 正在对冲中
            return
        self.hedge_clock = True
        self.t1 = time.time()*1000
        # 测试数据
        # self.deals_dict = {'ADA-USDT': [{'lastfilledVolume': 8.03, 'orderType': 'limit', 'leverage': 20, 'lastfilledSize': 10, 'isolatedMargin': '0.4017', 'liquidationPrice': '0.76713', 'cumfilledSize': 10, 'uid': 100022, 'positionAmt': '+10', 'markPrice': '0.80296', 'price': '0.80347', 'tag': 'J', 'direction': 'MANY', 'side': 'SELL', 'origQty': 10, 'positionSide': 'LONG', 'updateTime': 1740135870195, 'userId': 55970796, 'market': 'ADA-USDT', 'entryPrice': '0.80347', 'cumfilledVol': '8.03', 'isAutoAddMargin': 'false', 'unRealizedProfit': '-0.0051', 'marginType': 'isolated', 'lastfilledprice': '0.80347', 'status': 'FILLED'}]}
        matchs = self.deals_dict.copy()
        for symbol, match in matchs.items():
            # 按交易对并行处理对冲
            if len(match) != 0:
                try:
                    await self.agg_deals(symbol, match)
                except:
                    self.log.error(f"agg报错记录,为了防止多线程警告 {traceback.format_exc()}")
        self.hedge_clock = False
    
    # 聚合
    async def agg_deals(self, symbol, matchs):
        match_amt = 0   # 计算聚合成交金额
        match_size = 0  # 计算聚合成交量
        length = len(matchs)    # 聚合的订单数量
        deals_mess = ''
        for match in matchs:
            temp = match
            deals_mess += str(match)+'\n'
            side = match['side']    # 方向
            deal_amt = float(match['cumfilledVol']) if side == 'BUY' \
                 else -float(match['cumfilledVol'])  # 成交金额
            match_amt += deal_amt
            # deal_size = float(match['origQty']) if side == 'BUY' \
            #       else -float(match['origQty'])  # 成交数量
            deal_size = float(match['amount']) if side == 'BUY' \
                  else -float(match['amount'])  # 成交数量
            match_size += deal_size
            
            # 调整对冲量
            match_size = match_size*getattr(self, 'follow_ratio', 1)

            # 下单
            try:
                await self.hedge(symbol, match_size)
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]  # 已完全对冲后,删除已对冲部分的订单
                self.hedge_clock = False
                self.log.info(f"对冲耗时:{time.time() * 1000 - self.t1}ms")
            except KeyboardInterrupt as e:   # 修改
                self.hedge_clock = False
                self.log.error(f"手动停止 {traceback.format_exc()}")
                raise e
            except:
                temp['side'] = 'BUY' if self.thisVol > 0 else 'SELL'
                temp_amt = abs(self.thisVol)*float(temp['lastfilledprice'])
                temp['amount'] = abs(self.thisVol)
                temp['cumfilledVol'] = temp_amt
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]
                self.deals_dict[symbol].append(temp)
                self.hedge_clock = False
                self.log.error((f'此条pendingTask处理失败! {traceback.format_exc()}'))
                tb.warning(f"演示策略{symbol}合约对冲策略hedge报错:hedge函数报错信息:{traceback.format_exc()}", 'risk')
                tb.sendmail(f'演示策略{symbol}合约对冲策略hedge报错', f"hedge函数报错信息:{traceback.format_exc()}")
    
    # 对冲
    async def hedge(self, symbol, vol):
        tempLog = ''
        hedgeMess = ''
        
        # 更新交易对精度
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            mess = f"{symbol}在ok没有获取到交易对信息,无法对冲,立即人工介入"
            self.log.warning(mess)
            tb.warning(f'26号演示ok对冲策略:合约对冲失败,无法获取交易对信息','risk')
            tb.sendmail('合约对冲失败,无法获取交易对信息', mess)
            self.deals_dict[symbol] = []
            return

        vol = round(vol, amount_precision)
        hedgeMess += f"需要外盘对冲 {vol}\n"
        hedge_vol = 0   # 已对冲数量
        while 1:
            thisVol = round(vol - hedge_vol, amount_precision) if amount_precision > 0 else int(
                vol - hedge_vol)
            self.thisVol = thisVol
            hedgeMess += f"下单量{thisVol} 总成交量{hedge_vol}\n"
            thisVol = round(thisVol, amount_precision)
            if thisVol == 0.0:
                self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
                break
            depth = await self.okx_rest.fetch_depth(symbol)
            bid = depth['bids'][0][0]
            ask = depth['asks'][0][0]

            if vol > 0:
                bidPrice = round(ask*(1+self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{bidPrice}  对冲数量:{thisVol}  方向:buy\n"
                self.log.info(f"最终下单参数:{hedgeMess}")
                # return
                t11 = time.time()*1000
                # result = await self.okx_rest.create_order(symbol=symbol, side='BUY', orderType=ocb.OrderType.LIMIT, 
                #                                         amount=abs(thisVol), price=bidPrice)
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='buy', amount=abs(thisVol), 
                                                          price=bidPrice, tdMode='cross')
                tempLog += f"buy下单延时:{round(time.time()*1000-t11, 2)}ms  "
            elif vol < 0:
                askPrice = round(bid*(1-self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{askPrice}  对冲数量:{thisVol}  方向:sell\n"
                self.log.info(f'最终下单参数:{hedgeMess}')
                # return
                t11 = time.time()*1000
                # result = await self.okx_rest.create_order(symbol=symbol, side='SELL', orderType=ocb.OrderType.LIMIT, 
                #                                         amount=abs(thisVol), price=askPrice)
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                          side='sell', amount=abs(thisVol), 
                                                          price=askPrice, tdMode='cross')
                tempLog += f"sell下单延时:{round(time.time()*1000-t11, 2)}ms  "
            {'symbol': 'ADA-USDT', 'result': True, 'id': '2269739899423547392', 'clientOrderId': '', 'timestamp': 1740145920218, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2269739899423547392', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1740145920218'}], 'inTime': '1740145920217640', 'msg': '', 'outTime': '1740145920219807'}}
            hedge_order_id = result['id']
            self.log.info(f"对冲下单 {result} 延时:{round(time.time()*1000-t11, 2)}ms hedge_order_id:{hedge_order_id}")
            
            t33 = time.time()*1000
            try:
                cancelRes = await self.okx_rest.cancel_order(symbol, order_id=hedge_order_id)
                self.log.info(f"撤单 {cancelRes}")
            except:
                self.log.error(f"撤单报错{traceback.format_exc()}")
            tempLog += f"撤单延时:{round(time.time()*1000-t33, 2)}ms  "
            
            t44 = time.time()*1000
            while 1:  # 等待ws回调函数到达
                # 查询接口
                res = await self.okx_rest.fetch_order_detail(symbol, order_id=hedge_order_id)
                self.log.info(f"对冲订单状态:{res}")
                if res['status'] in ['closed', 'canceled', 'rejected']:
                    hedge_vol += res['filled'] if res['side'] == 'buy' else -res['filled']
                    hedge_vol = round(hedge_vol*face_value, amount_precision)
                    break
                await asyncio.sleep(0.5)

            tempLog += f"验证订单状态耗时:{round(time.time()*1000-t44, 2)}ms  "
            t55 = time.time()*1000
            self.log.info(f"{symbol}已对冲量:{hedge_vol}, 需要对冲总量:{vol}")
            if abs(hedge_vol) >= abs(vol):
                self.log.info(f"{symbol}对冲完全成交")
                tempLog += f"循环后跳出前耗时:{round(time.time()*1000-t55, 2)}ms\n"
                break
            await asyncio.sleep(0.5)
        self.log.info(tempLog)        


def main(path='tag_user_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()
    while True:
        time.sleep(999999)



if __name__ == '__main__':
    main()
