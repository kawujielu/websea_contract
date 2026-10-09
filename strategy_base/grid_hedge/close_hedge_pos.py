'''
    监控指定标签+指定uid用户的成交
    若平仓则立即在外盘平仓
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
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss

# import contract as okx
from crypto_center.client.rest.okex.contract import OkexContract



class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()

        self._load_config()  # 读取配置
        self._initParams()    # 初始化参数

        # 订阅base数据
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())     # 必须写这个才能订阅
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self):
        """读取配置文件
        """
        # 配置账号
        self.ws_rest = ws_contract_rest('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')   # 压盘口策略
        self.task = OkexContract(apiKey='e1934f97-f831-418e-b154-1ec5a0415f9a',secret='A2D8A15D0DE4B59BD7B270A34BB1273C',passphrase='usRolUVNuyBbEF@7 ')

    def _initParams(self):
        """初始化参数
        """
        self.tag = 'Z'
        self.symbol = 'FLOKI-USDT'            # 对冲合约
        self.id = 478061                    # 标记用户id 
        self.uid = 29353521                 # 标记用户uid
        self.rc_task = rc.RestClient()
        self.symbols_markprice = {}         # 保存内盘标记价格
        self.contract_unit = {}             # 保存内盘合约单位
        self.symbols_bid_ask = {}           # 保存外盘wss推送来的一档价格
        self.deals_dict = {}                # 保存内盘成交数据
        self.symbol_precision = {}    # 保存交易对精度
        self.hedge_clock = False            # 对冲锁
        self.risk_clock = False             # 风控锁
        self.thisVol = 0                    # 本次下单量
        self.split = 0.002                  # 下单滑点
        self.send_tg_ts = 0                 # 发送tg时间戳
        self.exposeRiskAmt = getattr(self, 'exposeRiskAmt', 5000*1.1)   # 净敞口报警
    
    async def on_first(self):
        await self.hedge_contract_info()       # 更新币对信息
        await self.get_symbol_unit()        # 内盘合约单位
    
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.handle_deals, CronTrigger(second="*/2"))  # 每1s执行一次
        self.schedule.add_job(self.risk, CronTrigger(second="*/10"))  # 每1s执行一次

    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                data = await self.ws_rest.get_symbols(self.symbol, quan=True)
                self.log.info(f"合约单位:{data}")
                # 若走这个逻辑,表示交易对下架
                if not isinstance(data, list):
                    self.symbol_unit = data.contract_size
                    break
                else:
                    raise f'{self.symbol}交易对已下架,获取不到合约单位'
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        # 原始数据
        # 平仓ws成交推送数据:{'lastfilledVolume': '', 'orderType': 'market', 'leverage': 20, 'lastfilledSize': '', 'isolatedMargin': '0.6002', 'liquidationPrice': '1094.25', 'cumfilledSize': '', 'uid': '', 'positionAmt': '-0.02', 'markPrice': '600.25', 'price': '', 'tag': 'A', 'direction': 'SPACE', 'side': 'BUY', 'origQty': 0.02, 'positionSide': 'SHORT', 'updateTime': 1729147616045, 'userId': 60142803, 'market': 'BNB-USDT', 'entryPrice': '600.50', 'cumfilledVol': '', 'isAutoAddMargin': 'false', 'unRealizedProfit': '0.005', 'marginType': 'full', 'lastfilledprice': '', 'status': 'NEW'}, <class 'dict'>
        # 平仓ws成交推送数据:{'lastfilledVolume': 12.0, 'orderType': 'market', 'lastfilledSize': 0.02, 'side': 'BUY', 'origQty': 0.02, 'cumfilledSize': 0.02, 'userId': 60142803, 'market': 'BNB-USDT', 'uid': 0, 'cumfilledVol': '12.00', 'price': '', 'lastfilledprice': '600.41', 'tag': 'A', 'status': 'FILLED', 'direction': 'SPACE'}, <class 'dict'>
        if content['tag'] == self.tag and \
            content['market'] == self.symbol and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED'] and \
            content['userId'] in [self.uid]:
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
        res = await self.task.fetch_precision()
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
        try:
            text = 'okx持仓:\n'
            # 获取外盘仓位
            hedge_pos = await self.task.fetch_position()  # 仓位带正负
            # print(f"fetch_position数据:{hedge_pos}")
            for v in hedge_pos:
                if float(v['contracts']) != 0:
                    text += f"{v['symbol']}: {float(v['contracts'])}\n成本:{round(float(v['entryPrice']),4)} 当前价格:{round(float(v['markPrice']), 4)}\n杠杆:{v['leverage']} 爆仓价格:{v['info']['liqPx']}\n浮动盈亏:{round(float(v['unrealizedPnl']), 2)}\n"
            self.log.info(f"外盘仓位:{text}")
            # if time.time()-self.send_tg_ts > 600:
            #     await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q',chat_id=-1002413824899,content=text)
            #     self.send_tg_ts = int(time.time())
            
            data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', user_id=self.id)
            # print(f"标记用户{self.id}持仓:{data}")
            base_pos = 0
            # if data['data']['data'][0]['symbol'] == self.symbol:
            #     base_pos = int(data['data']['data'][0]['amount'])*self.symbol_unit
            for i in data['data']['data']:
                if i['symbol'] == self.symbol:
                    base_pos = int(i['amount'])*self.symbol_unit
            
            # 内盘所有持仓
            base_pos_dict = {}
            for i in data['data']['data']:
                if i['symbol'] in base_pos_dict:
                    base_pos_dict[i['symbol']] += int(i['amount'])
                else:
                    base_pos_dict[i['symbol']] = int(i['amount'])
            temp_mess = (f"标记用户{self.id}内盘全部持仓:\n")
            for k, v in base_pos_dict.items():
                temp_mess += (f"{k}: {v}\n")
            self.log.info(f"{temp_mess}")

            # 计算净敞口
            # 内外盘仓位差报警
            for v in hedge_pos:
                if v['symbol'] == self.symbol:
                    hedge_position = float(v['contracts'])
            self.log.info(f"内盘持仓:{base_pos} 外盘持仓:{hedge_position}")
            if abs(hedge_position) > abs(base_pos):
                text = f"外盘仓位:{hedge_position} 内盘仓位:{base_pos} 外盘仓位大于内盘仓位,立即查看!!!\n"
                tb.warning(f"单独I组用户{self.symbol}合约持仓异常:{text}",'risk')
                tb.sendmail(f'单独I组用户{self.symbol}合约持仓异常', text)
                # await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q',chat_id=-1002413824899,content=text)

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
        matchs = self.deals_dict.copy()
        for symbol, match in matchs.items():
            # 按交易对并行处理对冲
            if len(match) != 0:
                try:
                    await self.agg_deals(symbol, match)
                except:
                    pass
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
            deal_size = float(match['amount']) if side == 'BUY' \
                  else -float(match['amount'])  # 成交数量
            match_size += deal_size

        if match_size != 0:
            self.log.info(f"聚合的订单信息:{deals_mess}")
            
            # 调整对冲量
            match_size = match_size*getattr(self, 'follow_ratio', 1)

            # 下单
            try:
                await self.hedge(symbol, match_size)
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]  # 已完全对冲后,删除已对冲部分的订单
                self.log.info(f"对冲耗时:{time.time() * 1000 - self.t1}ms")
                self.hedge_clock = False
            except KeyboardInterrupt as e:   # 修改
                self.hedge_clock = False
                self.log.error(f"手动停止 {traceback.format_exc()}")
                raise e
            except:
                self.hedge_clock = False
                self.log.error((f'此条pendingTask处理失败! {traceback.format_exc()}'))
                # temp['side'] = 'BUY' if self.thisVol > 0 else 'SELL'
                # temp_amt = abs(self.thisVol)*float(temp['entryPrice'])
                # temp['amount'] = abs(self.thisVol)
                # temp['cumfilledVol'] = temp_amt
                self.deals_dict[symbol] = self.deals_dict[symbol][length:]
                # self.deals_dict[symbol].append(temp)
                tb.warning(f"单独I组用户{symbol}合约对冲策略hedge报错:hedge函数报错信息:{traceback.format_exc()}", 'risk')
                tb.sendmail(f'单独I组用户{symbol}合约对冲策略hedge报错', f"hedge函数报错信息:{traceback.format_exc()}")
    
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
            if vol == 0.0:
                self.log.info(f"{symbol}本次对冲量是{thisVol},不对冲")
                return
            depth = await self.task.fetch_bids_asks(symbol=symbol)
            bid = float(depth['bid'])
            ask = float(depth['ask'])
            
            if vol > 0:
                bidPrice = round(ask*(1+self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{bidPrice}  对冲数量:{thisVol}  方向:buy\n"
                self.log.info(f"最终下单参数:{hedgeMess}")
                # return
                t11 = time.time()*1000
                result = await self.task.create_order(symbol=symbol, side='buy', order_type='limit', 
                                                      amount=abs(thisVol), price=bidPrice,
                                                      tdMode='cross', reduceOnly=True)
                tempLog += f"buy下单延时:{round(time.time()*1000-t11, 2)}ms  "
            elif vol < 0:
                askPrice = round(bid*(1-self.split), price_precision)
                hedgeMess += f"交易对:{symbol} 对冲价格:{askPrice}  对冲数量:{thisVol}  方向:sell\n"
                self.log.info(f'最终下单参数:{hedgeMess}')
                # return
                t11 = time.time()*1000
                result = await self.task.create_order(symbol=symbol, side='sell', order_type='limit', 
                                                      amount=abs(thisVol), price=askPrice, 
                                                      tdMode='cross', reduceOnly=True)
                tempLog += f"sell下单延时:{round(time.time()*1000-t11, 2)}ms  "
            hedge_order_id = result['id']
            self.log.info(f"对冲下单 {result} 延时:{round(time.time()*1000-t11, 2)}ms hedge_order_id:{hedge_order_id}")

            t33 = time.time()*1000
            try:
                cancelRes = await self.task.cancel_order(symbol, order_id=hedge_order_id)
                self.log.info(f"撤单 {cancelRes}")
            except:
                self.log.error(f"撤单报错{traceback.format_exc()}")
            tempLog += f"撤单延时:{round(time.time()*1000-t33, 2)}ms  "
            
            t44 = time.time()*1000
            while 1:  # 等待ws回调函数到达
                # 查询接口
                res = await self.task.fetch_order_detail(symbol, order_id=hedge_order_id)
                self.log.info(f"对冲订单状态:{res}")
                if res['status'] in ['canceled', 'closed', 'rejected']:
                    hedge_vol += res['filled'] if res['side'] == 'buy' else -res['filled']
                    hedge_vol = round(hedge_vol*face_value, amount_precision)
                    break
                await asyncio.sleep(0.5)

            tempLog += f"验证订单状态耗时:{round(time.time()*1000-t44, 2)}ms  "
            t55 = time.time()*1000
            if abs(hedge_vol) >= abs(vol):
                print(f"{symbol}对冲完全成交")
                tempLog += f"循环后跳出前耗时:{round(time.time()*1000-t55, 2)}ms\n"
                break
            await asyncio.sleep(0.05)
        self.log.info(tempLog)


def main():
    task = Strategy().run()
    while True:
        time.sleep(999999)



if __name__ == '__main__':
    main()

