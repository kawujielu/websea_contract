'''
    定时获取内外盘资费
    版本信息:v1.0.0
    日期:2025-11-4
    作者:sky
    
    规则:
    1、定时获取内外盘资费
    2、存入mysql数据库
    3、用grafana展示内外盘资费
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
from crypto_center.client.rest.binance import u_contract as bn_rest
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class fund(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()         # 读取配置
        self.db = 1
        self.rc_task = rc.RestClient()

    def _load_config(self):
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        self.bn_rest = bn_rest.UBinanceContract('AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi','FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj')
                
    async def on_first(self):
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        # 查询初始数据
        # await self.del_mysql()
        # await self.select_table()
        # await self.create_table()
        await self.get_all_symbols()
        self.log.info(f"策略初始化完成")
        await self.main()
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="*/5"))         # 判断是否需要调整资费
        self.schedule.add_job(self.get_all_symbols, CronTrigger(hour="*"))  # 更新交易对
    
    ''' ============================================================================'''
    ''' =================================== mysql =================================='''
    ''' ============================================================================'''
    # 连接数据库
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="10.0.208.249",
            user="lh_sky",
            password="sky_123456",
            database="contract_dws",
            port=33306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
    async def del_mysql(self):
        try:
            await self.connect_db()
            self.cursor.execute("DROP TABLE check_fund_cost")
            self.db.commit()
        except:
            print("没有check_fund_cost表")
        self.db.close()
    
    async def select_table(self):
        await self.connect_db()
        try:
            self.cursor.execute(f"SELECT * FROM check_fund_cost")
            data = self.cursor.fetchall()
            for i in data[-10:]:
                print(i)
        except:
            print("没有check_fund_cost表")
        self.db.close()

    async def create_table(self):
        await self.connect_db()        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `check_fund_cost` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `datetime` VARCHAR(50) NOT NULL COMMENT '日期',
                `symbol` VARCHAR(50) NOT NULL COMMENT '交易对',
                `websea_fund_rate` NUMERIC(10, 8) COMMENT 'websea资金费率',
                `binance_fund_rate` NUMERIC(10, 8) COMMENT 'binance资金费率',
                `diff_fund_rate` NUMERIC(10, 8) COMMENT '资费差别'
            )
            """)
    
    # 保存到mysql数据库
    async def save_mysql(self, save_sql_data):
        # 删除2天前的数据
        try:
            await self.connect_db()
            self.cursor.execute("DELETE FROM check_fund_cost WHERE datetime < DATE_SUB(NOW(), INTERVAL 2 DAY)")
            self.db.commit()  # 提交事务
        except:
            mess = f"{datetime.datetime.now()} 保存mysql资金费率数据失败,报错:{traceback.format_exc()}"
            self.log.warning(mess)
            tb.warning(mess, 'risk')
        # 插入新数据
        try:
            insert_sql = "INSERT INTO check_fund_cost (datetime, symbol, websea_fund_rate, binance_fund_rate, diff_fund_rate) VALUES (%s, %s, %s, %s, %s)"
            self.log.info(f"保存数据:{insert_sql}\n{save_sql_data}")
            # 批量插入
            self.cursor.executemany(insert_sql, save_sql_data)
            self.db.commit()  # 提交事务
        except:
            mess = f"{datetime.datetime.now()} 保存mysql资金费率数据失败,报错:{traceback.format_exc()}"
            self.log.warning(mess)
            tb.warning(mess, 'risk')
        self.db.close()  # 关闭数据库连接
        
    ''' ============================================================================'''
    ''' ================================== function ================================'''
    ''' ============================================================================'''
    # 获取所有交易对
    async def get_all_symbols(self):
        res = await self.rest.fetch_symbol_info()
        all_symbols = list(res.keys())
        for s in all_symbols[-3:]:
            try:
                depth = await self.rest.fetch_depth(s)
            except:
                self.log.warning(f"获取{s}深度失败,报错:{traceback.format_exc()}")
                await asyncio.sleep(0.1)
                continue
            if depth['bids'] == [] and depth['asks'] == []:
                await asyncio.sleep(0.1)
                continue
            all_symbols.append(s)    # 交易对,交易对id
            await asyncio.sleep(0.1)
        self.all_symbols = all_symbols
    
    # 获取内外盘资费
    async def get_funding(self):
        # 外盘资费和内盘资费比较
        # 获取交易对资金费率
        symbols_fund = {}
        for s in self.all_symbols:
            while 1:
                try:
                    res = await self.rest.fetch_funding_rate(s)
                    symbols_fund[s] = round(float(res[s]['fundingRate'])/100, 6)
                    break
                except:
                    self.log.warning(f"获取{s}资金费率失败,报错:{traceback.format_exc()}")
                    await asyncio.sleep(0.2)
        # 查询binance资金费率
        bn_fund_all = {}
        while 1:
            try:
                data = await self.bn_rest.fetch_funding_rate()
                break
            except:
                self.log.warning(f"获取binance资金费率失败,报错:{traceback.format_exc()}")
                await asyncio.sleep(0.2)
        # self.log.info(f"binance资金费率:{data}")
        for k, v in data.items():
            bn_fund_all[k] = float(v['fundingRate'])
        both_fund = {}
        for s, v in symbols_fund.items():
            temp_bn_fund = round(bn_fund_all[s], 6) if s in bn_fund_all else None
            both_fund[s] = [v, temp_bn_fund]
        return both_fund
        
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 查询内外盘交易对资金费率
        both_fund = await self.get_funding()
        
        save_sql_data = []
        for s, v in both_fund.items():
            save_sql_data.append([
                datetime.datetime.now(), s, v[0], v[1], v[0]-v[1] if v[1] else None,
            ])
        await self.save_mysql(save_sql_data)
    


def main():
    fund().run()

if __name__ == '__main__':
    main()



