
import sys
import pytz
import traceback
import time
import json
import asyncio
import requests
import pandas as pd
from datetime import datetime
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from client.env_pro.rest.websea.contract import WebseaContract as old_contract
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from client.env_dev.wss.websea_contract import WebSeaContract as ws_contract_wss
# from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
# from client.env_pro.rest.websea import contract
from template.template_timer import TemplateTimer, CronTrigger
from utils.aio_redis import MyAioredis, MyAioredisFunctools


class adl(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self._load_config(config)   # 读取配置
        self._init_params()         # 初始化参数

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        # self.rest = Contract('271e2179a3ea3ba6cdbdd8e3dfft1638964','npgsvdyw184aycu2ufai', dev=True)
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.old_rest = old_contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')
        
        # self.rest1 = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口19
        # self.rest2 = contract.WebseaContract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口20
        # self.rest3 = contract.WebseaContract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位21
        # self.rest4 = contract.WebseaContract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量18
        # self.rest5 = contract.WebseaContract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略 22
        # self.rest6 = contract.WebseaContract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4')    # 压盘口策略
        # self.rest7 = contract.WebseaContract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3')    # 防守策略 23
        # self.rest8 = contract.WebseaContract('5accd1fb1d03e26f299a4297d9q52646135','nyuc69aljff4g297uj99') # MH做市策略
        # self.rest9 = contract.WebseaContract('57b49384ee4e0b236f0f38a45fj50733190','bu8qqmoyaq0o10xeepdd')
    
    def _init_params(self):
        # 测试数据
        # self.symbols_markprice = {'BTC-USDT': 110000, 'ETH-USDT': 4400, 'TRX-USDT':0.35, 'BCH-USDT':510, 'ADA-USDT':0.7, 'DOGE-USDT':0.5}   # 保存交易对标记价格
        self.symbols_markprice = {}
        self.filter_symbols = []      # 过滤交易对
        self.first = True
        self.last_hour = 0            # 上一时刻
        self.all_symbols = []         # 所有交易对
        self.adl_profit = 500         # adl阈值
        self.adl_percent = 80         # adl百分比
        self.filter_tags = ['E7','E8','E12']         # 过滤标签组
        self.mm_uids = [18] #,19,20,21,22,23,476515,465880]             # 做市用户id
        
    async def on_first(self):
        self.log.add(f"log/adl.log", rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(test=False)
        self.redis_conn = await self.redis_pool.open()
        # 订阅外盘一档数据
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        self.all_user_pos = await self.fetch_all_pos()
        await asyncio.sleep(1)
        await self.redis_conn.sub_channel(f"market.profit.hold")  # 普通用户持仓变化推送,用于adl策略
    
        # self.ws_wss = ws_contract_wss()
        # self.ws_wss.on_markprice = self.on_markprice
        # self.loop.create_task(self.ws_wss.only_subscribe())    # 必须写这个才能订阅
        # self.log.info(f"subscribe: 标记价格推送")
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/3"))
        self.schedule.add_job(self.check_pos, CronTrigger(hour="*"))
    
    # 定时用rest对齐wss推送的持仓信息
    async def check_pos(self):
        rest_user_pos = await self.fetch_all_pos()
        # 判断rest和wss推送数据是否有差别
        for k, v in self.symbol_pos.items():
            if k in rest_user_pos:
                # self.log.info(f"{k}rest仓位:{rest_user_pos[k]} wss仓位:{v}")
                if abs(rest_user_pos[k]) > abs(v):
                    text = f"{k}rest仓位:{rest_user_pos[k]} wss仓位:{v} rest仓位大于wss仓位\n"
                    self.send_msg(text)
                elif abs(rest_user_pos[k]) < abs(v):
                    text = f"{k}rest仓位:{rest_user_pos[k]} wss仓位:{v} rest仓位小于wss仓位\n"
                    self.send_msg(text)
        for k, v in rest_user_pos.items():
            if k not in self.symbol_pos:
                text = (f"{k}rest仓位:{v} wss仓位")
                self.log.info(text)
        # 以rest查询数据为准
        self.all_user_pos = rest_user_pos
    
    ''' ============================================================================'''
    ''' ==================================== wss ==================================='''
    ''' ============================================================================'''
    async def on_message(self, channel: str, item: dict):
        self.log.info(f"{channel}, {item}")
        # 判断变量类型是否是字典
        if channel == f"market.profit.hold" and isinstance(item, dict):
            {'userId': 19429, 'userUid': 34551221, 'symbol': 'DOGE-USDT', 'markPrice': '0.24391847083333332', 
             'avgPrice': '0.243226', 'openDirection': 2, 'amount': '0', 'tag': 'A'}
            uid = item['userUid']
            symbol = item['symbol']
            tag = item['tag'] if 'tag' in item else 'normal'
            margin_type = item['margin_type']   # TODO 字段需要研发添加
            side = 'LONG' if item['openDirection'] == 1 else 'SHORT'
            vol = float(item['amount']) if item['openDirection'] == 1 else -float(item['amount'])
            price = float(item['avgPrice'])
            if tag not in self.all_user_pos:
                self.all_user_pos[tag] = {uid: {symbol: {side: [price, vol, margin_type]}}}
            else:
                if uid not in self.all_user_pos[tag]:
                    self.all_user_pos[tag][uid] = {symbol: {side: [price, vol, margin_type]}}
                else:
                    if symbol not in self.all_user_pos[tag][uid]:
                        self.all_user_pos[tag][uid][symbol] = {side: [price, vol, margin_type]}
                    else:
                        self.all_user_pos[tag][uid][symbol][side] = [price, vol, margin_type]
    
    # 接收订阅的标记价格
    async def on_markprice(self, content):
        # self.log.info(f"websea标记价格:{content}")
        self.symbols_markprice[content.symbol] = content.markPrice
        
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        # 获取标记价格
        res = await self.old_rest.get_index()
        for s, v in res.items():
            self.symbols_markprice[s] = v.price
        
        if self.first:
            self.first = False
            res = await self.old_rest.get_symbols()    # 测试数据 quan=True
            # self.log.info(f"websea交易对:{res}")
            for s in res:
                depth = await self.old_rest.get_depth(s.symbol, 1) # 测试数据  quan=True
                if depth.bids == [] and depth.asks == []:
                    continue
                self.all_symbols.append(s.symbol)    # 交易对,交易对id
            self.log.info(f"初始交易对:{self.all_symbols}")
        
        # 波动最大的交易对
        hour = datetime.now(timezone).hour
        if hour != self.last_hour:
            self.last_hour = hour
            rate_list = []
            for s in self.all_symbols:
                try:
                    data = await self.old_rest.get_hr24(s)
                    rate = round((data.high/data.low-1)*100, 2)
                    rate_list.append([s, rate])
                except:
                    pass
                await asyncio.sleep(0.3)
            # rate_list根据[1]从大到小排序
            rate_list.sort(key=lambda x:x[1], reverse=True)
            self.log.info(f"波动最大的3个交易对:{rate_list[:3]}")
            for i in rate_list[:3]:
                self.filter_symbols.append(i[0])
        
        all_user_pos = self.all_user_pos
        # self.log.info(f"全部持仓:{all_user_pos}")
        for tag, uid_data in all_user_pos.items():
            for uid, symbol_data in uid_data.items():
                for symbol, side_data in symbol_data.items():
                    for side, price_vol in side_data.items():
                        if symbol in self.symbols_markprice:
                            mark_price = float(self.symbols_markprice[symbol])
                            price = price_vol[0]
                            vol = price_vol[1]
                            margin_type = price_vol[2]
                            if side == 'LONG':
                                profit = (mark_price - price) * vol
                            else:
                                profit = (price - mark_price) * vol
                            # 盈利超过阈值且不属于过滤标签组
                            if profit > self.adl_profit and tag not in self.filter_tags and symbol in self.filter_symbols:
                                await self.adl_order(uid, symbol, side, margin_type, profit)
                        else:
                            await self.ws_wss.sub_markprice(symbol)
                            self.log.info(f"{symbol}标记价格未更新")
        
    async def adl_order(self, uid, symbol, side, margin_type, profit):
        for mm_uid in self.mm_uids:
            # side方向就是用户持仓方向,用户多单需要被adl,则side是LONG,用户空单需要被adl,则side是SHORT
            self.log.info(f"adl订单信息:mm_uid={mm_uid},uid={uid},symbol={symbol},side={side},margin_type={margin_type},profit={profit},adl_percent={self.adl_percent}")
            continue
            # data = await self.rest1.adl_order(token=self.adl_token, market=symbol, \
            #                                   mmUserId=mm_uid, side=side, adlUserId=uid, \
            #                                   adlPercent=self.adl_percent,  marginType=margin_type)  #'isolated,'crossed')
            # self.log.info(f"adl下单回报:{data}")
    
    async def fetch_all_pos(self):
        rest_user_pos = {}   # rest_user_pos = {tag: {user_id: {symbol: amount}}}
        temp_hold_list = []
        last_page = 0
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_hold_list(page_size=100)
                    hold_list_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in temp_hold_list:
                            temp_hold_list.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, hold_list_page+1):
                    last_page = n
                    data = await self.rest.fetch_hold_list(page=n, page_size=100)
                    for i in data['data']:
                        if i not in temp_hold_list:
                            temp_hold_list.append(i)
                    await asyncio.sleep(0.5)
                for i in temp_hold_list:
                    tag = i['tag'] if i['tag'] != '' else 'normal'
                    margin_type = 'isolated' if i['isFull'] == 1 else 'crossed'  # 1逐仓 2全仓
                    side = 'LONG' if i['openDirection'] == 1 else 'SHORT'
                    price = float(i['avgPrice'])
                    vol = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
                    if tag not in rest_user_pos:
                        rest_user_pos[tag] = {i['user_id']: {i['symbol']: {side: [price, vol, margin_type]}}}
                    else:
                        if i['user_id'] not in rest_user_pos[tag]:
                            rest_user_pos[tag][i['user_id']] = {i['symbol']: {side: [price, vol, margin_type]}}
                        else:
                            if i['symbol'] not in rest_user_pos[tag][i['user_id']]:
                                rest_user_pos[tag][i['user_id']][i['symbol']] = {side: [price, vol, margin_type]}
                            else:
                                rest_user_pos[tag][i['user_id']][i['symbol']][side] = [price, vol, margin_type]
                break
            except:
                self.log.info(f"fetch_all_pos 异常:{traceback.format_exc()}")
                await asyncio.sleep(5)
        # {'data': [{'amount': '1366', 'avgPrice': '4370.027467057101024885', 'isFull': 2, 'is_full': 2, 'mark_price': '4288.350558136167', 'multiple': 50, 'openDirection': 1, 'open_time': 1756910192, 'profitLoss': '-1115.7065758599587799291', 'profit_loss': '-1115.7065758599587799291', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756910192, 'tradeNum': '13.66', 'user_id': '10466', 'user_name': '10466'}, {'amount': '2040', 'avgPrice': '4365.538102941176470579', 'isFull': 2, 'is_full': 2, 'mark_price': '4288.350558136167', 'multiple': 50, 'openDirection': 2, 'open_time': 1756910260, 'profitLoss': '1574.6259140221931998116', 'profit_loss': '1574.6259140221931998116', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756910260, 'tradeNum': '20.4', 'user_id': '10466', 'user_name': '10466'}, {'amount': '1162', 'avgPrice': '4377.965662729032209252', 'isFull': 1, 'is_full': 1, 'mark_price': '4288.350558136167', 'multiple': 5, 'openDirection': 2, 'open_time': 1756017649, 'profitLoss': '1041.32751536909373150824', 'profit_loss': '1041.32751536909373150824', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756017649, 'tradeNum': '11.62', 'user_id': '19371', 'user_name': '19371'}, {'amount': '1', 'avgPrice': '4401.83', 'isFull': 1, 'is_full': 1, 'mark_price': '4288.350558136167', 'multiple': 5, 'openDirection': 1, 'open_time': 1757309803, 'profitLoss': '-1.13479441863833', 'profit_loss': '-1.13479441863833', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1757309803, 'tradeNum': '0.01', 'user_id': '19371', 'user_name': '19371'}], 
        # 'pager': {'bengin_time': 1757395886, 'end_time': 1757395886, 'item_count': 9, 'page': '1', 'page_size': '100', 'total_page': 1}}
        self.log.info(f"rest获取全部用户持仓:{rest_user_pos}")
        return rest_user_pos


def main(path='adl_config.py'):
    config = __import__(path.split(".py")[0])
    adl(config).run()


if __name__ == '__main__':
    main()
