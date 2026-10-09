'''
    每日更新榜单
    版本信息:v1.0.0
    日期:2025-10-22
    作者:sky

    涨跌幅榜: 昨日涨跌幅排名前n的交易对
    新币榜: 上线一周内的交易对
    热门榜: n个交易日平均交易量最大的前n个交易对
'''
import os
import sys
import datetime
import asyncio
import traceback
import importlib
from pytz import timezone
from pathlib import Path

sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as old_WebseaContract
from crypto_center.client.rest.websea.contract_quan import WebseaContract     # 新接口做下单



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.redis_pool: Optional[MyAioredis] = None
        self.redis_conn: Optional[MyAioredisFunctools] = None
        self.db = 1
        self.rc_task = rc.RestClient()
        self._load_config(config)  # 读取配置
        self._initParams()         # 初始化参数
    
    def _load_config(self, config):
        """读取配置文件
        """
        self.init_config = config
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rest = WebseaContract(config['base_token'], config['base_secret'])
        self.old_rest = old_WebseaContract(config['base_token'], config['base_secret'])
        
    def _initParams(self):
        """初始化参数
        """
        self.select_days = 60       # 查询多少天的数据
        self.date_limit = 10        # 计算多少天平均交易量参数
        self.symbol_num = 10        # 筛选出多少个交易对
        self.all_symbols = []       # 记录所有内盘交易对
        self.symbol_unit = {}       # 记录所有内盘交易对单位
        self.symbol_pos_user = {}   # 交易人数
        self.symbol_pos_num = {}    # 交易人次
        pass
    
    async def on_first(self):
        self.log.info("=======first======")
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)

        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()
        # 订阅数据
        self.loop.create_task(self.redis_conn.subscribe_async(channel=[], callback=self.on_message))
        await asyncio.sleep(1)
        await self.redis_conn.sub_channel(f"market.profit.hold")          # 普通用户持仓变化推送,用于adl策略
        
        await self.get_symbol_unit()    # 获取内盘币对信息
        await self.get_all_symbols()    # 获取所有交易对
        self.log.info(f"初始化完成")
        # await self.main()
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(hour=f"8", minute="1", timezone=timezone("Asia/Shanghai")))   # 主策略
        # self.schedule.add_job(self.main, CronTrigger(minute="*/10"))          # 测试用的
        self.schedule.add_job(self.get_all_symbols, CronTrigger(hour="*"))    # 每小时更新一次内盘交易对
        self.schedule.add_job(self.get_symbol_unit, CronTrigger(hour="*"))    # 每小时更新一次交易对单位
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*"))    # 定时更新配置文件
    
    ''' ============================================================================'''
    ''' ==================================== wss ==================================='''
    ''' ============================================================================'''
    async def on_message(self, channel: str, item: dict):
        # print(channel, item)
        if channel == 'market.profit.hold' and isinstance(item, dict):
            {'userId': 19429, 'userUid': 34551221, 'symbol': 'DOGE-USDT', 'markPrice': '0.24391847083333332', 
             'avgPrice': '0.243226', 'openDirection': 2, 'amount': '0', 'tag': 'A'}
            {'userId': 584954, 'userUid': 93328205, 'symbol': 'ETH-USDT', 'markPrice': '3874.930372093', 
             'avgPrice': '3825.49', 'openDirection': 1, 'amount': '0', 'margin_type': 'crossed'}
            self.log.info(item)
            if 'msg' in item:
                return
            id = item['userId']
            symbol = item['symbol']
            tag = item['tag'] if 'tag' in item else 'normal'
            if 'E' not in tag:
                if symbol not in self.symbol_pos_user:
                    self.symbol_pos_user[symbol] = [id]
                else:
                    if id not in self.symbol_pos_user[symbol]:
                        self.symbol_pos_user[symbol].append(id)
                if symbol not in self.symbol_pos_num:
                    self.symbol_pos_num[symbol] = 1
                else:
                    self.symbol_pos_num[symbol] += 1
    
    ''' ==========================================================================='''
    ''' =================================== funcation ============================='''
    ''' ==========================================================================='''
    # 查询websea合约单位
    async def get_symbol_unit(self):
        while True:
            try:
                res = await self.rest.fetch_symbol_info()
                for s, d in res.items():
                    self.symbol_unit[s] = d['contractSize']
                self.log.info(f"初始化内盘合约单位:{self.symbol_unit}")
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            asyncio.sleep(0.5)
            
    async def get_all_symbols(self):
        res = await self.rest.fetch_symbol_info()
        self.all_symbols = list(res.keys())
        self.log.info(f"获取全部合约交易对:{self.all_symbols}")
    
    # 重新加载配置文件
    async def reload_config(self):
        try:
            importlib.reload(self.init_config)
            [setattr(self, k, v) for k, v in vars(self.init_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"reload_config报错{traceback.format_exc()}")
            except:
                pass
    
    async def check_today(self):
        today_tag = False
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        if today != self.today:
            self.today = today
            today_tag = True
        # 测试数据
        if datetime.datetime.now().hour == 3:
            today_tag = True
        return today_tag
    # 发送tg消息
    async def send_tg(self, id, text):
        text_temp = text.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=id, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=id, content=send_text)
    
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        temp_pos_user = {}
        for s, u in self.symbol_pos_user.items():
            temp_pos_user[s] = len(u)
        deal_user = sorted(temp_pos_user.items(), key=lambda x: x[1], reverse=True)[:self.symbol_num]
        deal_user = [i for i in deal_user]
        deal_num = sorted(self.symbol_pos_num.items(), key=lambda x: x[1], reverse=True)[:self.symbol_num]
        deal_num = [i for i in deal_num]
        self.symbol_pos_user = {}   # 归零重新统计
        self.symbol_pos_num = {}

        up_down_data = {}
        vol_data = {}
        new_data = {}
        all_symbols = self.all_symbols
        for s in all_symbols:
            res = await self.rest.fetch_kline(s, interval='1d', limit=self.select_days)
            try:
                symbol = res[0]['symbol']
                vol_data[symbol] = []
                range = round((res[1]['close']/res[1]['open']-1)*100, 2)
                up_down_data[symbol] = range
                new_data[symbol] = len(res)
                for i in res[:self.date_limit]:
                    price = (i['close']+i['open']+i['high']+i['low'])/4
                    amt = i['volume']*self.symbol_unit[symbol]*price
                    vol_data[symbol].append(amt)
            except:
                self.log.error(f"{s}数据报错:{traceback.format_exc()}\n{res}")
        up_list = sorted(up_down_data.items(), key=lambda x: x[1], reverse=True)[:self.symbol_num]
        down_list = sorted(up_down_data.items(), key=lambda x: x[1])[:self.symbol_num]
        new_list = sorted(new_data.items(), key=lambda x: x[1], reverse=False)[:self.symbol_num]
        up_list = [i[0] for i in up_list]
        down_list = [i[0] for i in down_list]
        new_list = [i[0] for i in new_list]
        vol_dict = {}
        for s, v in vol_data.items():
            vol_dict[s] = int(sum(v)/len(v))
        vol_list = sorted(vol_dict.items(), key=lambda x: x[1], reverse=True)[:self.symbol_num]
        vol_list = [i[0] for i in vol_list]
        mess = f"涨幅榜:{up_list}\n跌幅榜:{down_list}\n新币榜:{new_list}\n热门榜:按交易量排名:{vol_list}\n按交易人数排名:{deal_user}\n交易人次排名:{deal_num}"
        self.log.info(f"每日更新:{mess}")
        await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4899120322, content=mess)
        


def main(path='symbol_ranking_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()


if __name__ == '__main__':
    main()


