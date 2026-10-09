'''
    测试+监控, wss和rest仓位差在哪里
    版本信息:v1.0.0
    日期:2025-11-06
    作者:sky
'''

import os
import sys
import time
import datetime
import random
import traceback
import asyncio
import pytz
import pandas as pd
import numpy as np
import mysql.connector
from apscheduler.schedulers.asyncio import AsyncIOScheduler
scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")


sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from crypto_center.client.rest.binance import u_contract as bn_rest
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class fund(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()         # 读取配置
        self.symbol_pos = {}        # 交易对持仓
        self.user_symbol_pos = {}   # 按照用户+交易对记录持仓
        self.check_symbols = ['BTC-USDT','ETH-USDT','TREE-USDT','XPL-USDT','SXT-USDT','BCH-USDT','XMR-USDT','MON-USDT','VIRTUAL-USDT','RESOLV-USDT','SUI-USDT','CUDIS-USDT','XRP-USDT','AAVE-USDT','ADA-USDT','ENS-USDT','PUMP-USDT']

    def _load_config(self):
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        self.bn_rest = bn_rest.UBinanceContract('AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi','FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj')
    
    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        self.redis_pool = MyAioredis(test=False)
        self.redis_conn = await self.redis_pool.open()
        
        await self.fetch_all_pos(True)

        # 订阅外盘一档数据
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(1)
        await self.redis_conn.sub_channel(f"market.profit.hold")   # 普通用户持仓变化推送,用于adl策略

        self.log.info(f"策略初始化完成")
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.fetch_all_pos, CronTrigger(minute="*/20"))
        
    ''' ============================================================================'''
    ''' ================================== function ================================'''
    ''' ============================================================================'''
    # 查询内盘所有用户持仓
    async def fetch_all_pos(self, tag=False):
        rest_symbol_pos = {}
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
                    symbol = i['symbol']
                    if symbol not in self.check_symbols:
                        continue
                    pos = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
                    if symbol not in rest_symbol_pos:
                        rest_symbol_pos[symbol] = {'E': 0, 'filter_E': 0}
                    if 'E' in i['tag']:
                        rest_symbol_pos[symbol]['E'] += pos
                    else:
                        rest_symbol_pos[symbol]['filter_E'] += pos
                if tag:
                    user_symbol_pos = {}
                    for i in temp_hold_list:
                        {'amount': '200', 'avgPrice': '1.14075', 'isFull': 1, 'is_full': 1, 'mark_price': '1.1356112333333335', 
                         'multiple': 20, 'openDirection': 2, 'open_time': 1762404257, 'profitLoss': '1.0277533333333', 
                         'profit_loss': '1.0277533333333', 'symbol': 'ASTER-USDT', 'tag': '', 'time': 1762404257, 'tradeNum': '200', 
                         'user_id': '579178', 'user_name': '579178'}
                        symbol = i['symbol']
                        id = i['user_id']
                        if symbol not in self.check_symbols:
                            continue
                        tag = i['tag'] if i['tag'] != '' else 'normal'
                        side = 'buy' if i['openDirection'] == 1 else 'sell'
                        vol = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
                        if id not in user_symbol_pos:
                            user_symbol_pos[id] = {symbol: {side: [tag, vol]}}
                        else:
                            if symbol not in user_symbol_pos[id]:
                                user_symbol_pos[id][symbol] = {side: [tag, vol]}
                            else:
                                user_symbol_pos[id][symbol][side] = [tag, vol]
                    self.user_symbol_pos = user_symbol_pos
                    # print(f"用rest初始化的wss数据:{self.user_symbol_pos}")
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
            # {'data': [{'amount': '1366', 'avgPrice': '4370.027467057101024885', 'isFull': 2, 'is_full': 2, 'mark_price': '4288.350558136167', 'multiple': 50, 'openDirection': 1, 'open_time': 1756910192, 'profitLoss': '-1115.7065758599587799291', 'profit_loss': '-1115.7065758599587799291', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756910192, 'tradeNum': '13.66', 'user_id': '10466', 'user_name': '10466'}, {'amount': '2040', 'avgPrice': '4365.538102941176470579', 'isFull': 2, 'is_full': 2, 'mark_price': '4288.350558136167', 'multiple': 50, 'openDirection': 2, 'open_time': 1756910260, 'profitLoss': '1574.6259140221931998116', 'profit_loss': '1574.6259140221931998116', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756910260, 'tradeNum': '20.4', 'user_id': '10466', 'user_name': '10466'}, {'amount': '1162', 'avgPrice': '4377.965662729032209252', 'isFull': 1, 'is_full': 1, 'mark_price': '4288.350558136167', 'multiple': 5, 'openDirection': 2, 'open_time': 1756017649, 'profitLoss': '1041.32751536909373150824', 'profit_loss': '1041.32751536909373150824', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1756017649, 'tradeNum': '11.62', 'user_id': '19371', 'user_name': '19371'}, {'amount': '1', 'avgPrice': '4401.83', 'isFull': 1, 'is_full': 1, 'mark_price': '4288.350558136167', 'multiple': 5, 'openDirection': 1, 'open_time': 1757309803, 'profitLoss': '-1.13479441863833', 'profit_loss': '-1.13479441863833', 'symbol': 'ETH-USDT', 'tag': 'E', 'time': 1757309803, 'tradeNum': '0.01', 'user_id': '19371', 'user_name': '19371'}], 
            # 'pager': {'bengin_time': 1757395886, 'end_time': 1757395886, 'item_count': 9, 'page': '1', 'page_size': '100', 'total_page': 1}}
        self.log.info(f"rest查询到的持仓信息: {rest_symbol_pos}")
        wss_symbol_pos = await self.count_wss_pos()
        self.log.info(f"wss更新的持仓信息: {wss_symbol_pos}")

        
    ''' ============================================================================'''
    ''' ==================================== wss ==================================='''
    ''' ============================================================================'''
    async def on_message(self, channel: str, item: dict):
        self.log.info(f"{channel} {item}")
        # item是int类型,直接忽略
        if isinstance(item, int) or 'msg' in item:
            print(f"跳过的数据:{item}")
            return
        if channel == 'market.profit.hold' and isinstance(item, dict):
            # 没有tag字段表示没有标记组,有的话就是'tag': 'A'这样的字符串
            id = item['userId']
            symbol = item['symbol']
            if symbol not in self.check_symbols:
                return
            tag = item['tag'] if 'tag' in item else 'normal'
            side = 'buy' if item['openDirection'] == 1 else 'sell'
            vol = float(item['amount']) if item['openDirection'] == 1 else -float(item['amount'])  # 当前持仓,非成交数量
            
            if id not in self.user_symbol_pos:
                self.user_symbol_pos[id] = {symbol: {side: [tag, vol]}}
            else:
                if symbol not in self.user_symbol_pos[id]:
                    self.user_symbol_pos[id][symbol] = {side: [tag, vol]}
                else:
                    self.user_symbol_pos[id][symbol][side] = [tag, vol]
            
    async def count_wss_pos(self):
        temp_symbol_pos = {}
        for id, symbol_data in self.user_symbol_pos.items():
            for symbol, side_data in symbol_data.items():
                for side, tag_vol in side_data.items():
                    if symbol not in temp_symbol_pos:
                        temp_symbol_pos[symbol] = {'E': 0, 'filter_E': 0}
                    if 'E' in tag_vol[0]:
                        temp_symbol_pos[symbol]['E'] += tag_vol[1]
                    else:
                        temp_symbol_pos[symbol]['filter_E'] += tag_vol[1]
        return temp_symbol_pos


def main():
    fund().run()

if __name__ == '__main__':
    main()




