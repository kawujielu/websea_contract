'''
    监控websea合约盘口是否跟随,K线是否走平
    版本信息:v1.0.0
    日期:2025-10-20
    作者:sky

    wss订阅内外盘深度数据,K线数据

'''
import os
import sys
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from typing import Optional, Dict, List
from utils.aio_redis import MyAioredis, MyAioredisFunctools
import time
import traceback
import importlib
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
import objects.contract_request.websea as ocw
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
    
    async def on_first(self):
        self.log.info("=======first======")
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)

        self.redis_pool = MyAioredis(db=self.db)
        self.redis_conn = await self.redis_pool.open()

        await self.get_all_symbols()

        channel_depth = []
        for s in self.all_symbols:
            channel_depth.append(f"contract.kline.1m.{s}.websea")
            channel_depth.append(f"contract.depth.{s}.binance")
            channel_depth.append(f"contract.depth.{s}.websea")
        # channel_depth = [f"contract.depth.{self.symbol}.websea", f"contract.depth.{self.symbol}.binance", f"contract.kline.1m.{self.symbol}.websea"] # okex,binance
        self.loop.create_task(self.redis_conn.subscribe_async(channel=channel_depth, callback=self.on_message))
        
        # self.loop.create_task(self.redis_conn.subscribe_async(
        #     channel=[], callback=self.on_message))
        # await asyncio.sleep(1)
        # await self.redis_conn.sub_channel(f"contract.depth.{self.symbol}.websea")
        # await self.redis_conn.sub_channel(f"contract.depth.{self.symbol}.binance")
        # await self.redis_conn.sub_channel(f"contract.kline.{self.symbol}.websea")
        self.log.info(f"初始化完成")

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/10"))          # 主策略
        self.schedule.add_job(self.get_all_symbols, CronTrigger(hour="*"))    # 每小时更新一次内盘交易对
        self.schedule.add_job(self.check_wss, CronTrigger(second="*/10"))     # 检查wss链接
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*"))   # 定时更新配置文件

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
        self.binance_depth = {}     # 记录binance一档价格
        self.websea_depth = {}      # 记录websea一档价格
        self.check_depth_data = {}  # 记录用来检查盘口是否有变化的数据
        self.kline_dict = {}        # 记录内盘K线数据,用于判断价格是否走平
        self.all_symbols = []       # 记录所有内盘交易对
        self.bn_ws_ts = 0           # 记录binance wss最后一次时间戳
        self.websea_ws_ts = 0       # 记录websea wss最后一次时间戳
        self.needle_time_limit = 15 # 15s报警一次
        pass
    
    async def reload_config(self):
        try:
            importlib.reload(self.init_config)
            [setattr(self, k, v) for k, v in vars(self.init_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"reload_config报错{traceback.format_exc()}")
            except:
                pass
            
    ''' ==========================================================================='''
    ''' ===================================== wss ================================='''
    ''' ==========================================================================='''
    # websea深度
    async def on_message(self, items, content):
        # print(f"on_message:items:{items} content:{content}")
        # return
        try:
            if 'depth' in items and 'websea' in items:
                self.websea_ws_ts = int(content['timestamp']/1000)
                self.websea_depth[content['symbol']] = [content['bids'][0][0], content['asks'][0][0]]
            elif 'depth' in items and 'binance' in items:
                self.bn_ws_ts = int(content['timestamp']/1000)
                self.binance_depth[content['symbol']] = [content['bids'][0][0], content['asks'][0][0]]
            elif 'kline' in items and content['messageType'] == 'message':
                await self.check_kline(items, content)
        except:
            self.log.info(f"报错内容:{traceback.format_exc()} items:{items} content:{content}")
    
    ''' ==========================================================================='''
    ''' =================================== funcation ============================='''
    ''' ==========================================================================='''
    # 判断k线是否正常
    async def check_kline(self, items, content):
        symbol = items.split('1m.')[1].split('.websea')[0]
        ts = int(content['timestamp']/1000)
        kline_data = [content['open'], content['high'], content['low'], content['close']]
        # 判断是否插针
        await self.check_kline_needle(symbol, kline_data)
        if symbol not in  self.kline_dict:
            self.kline_dict[symbol] = {ts: kline_data}
        else:
            last_kline_data = list(self.kline_dict[symbol].values())[0]
            if last_kline_data == kline_data:
                diff_ts = ts-int(list(self.kline_dict[symbol].keys())[0])
                if diff_ts > self.time_limit:
                    self.log.info(f"盘面报警:{symbol}超过{diff_ts}ms K线数据不变")
                    # await self.send_tg(self.chat_id, f"盘面报警:合约{symbol}超过{int(diff_ts)}s K线数据不变。立即检查做市策略。推送的kline数据为:{kline_data}")
                # else:
                #     self.log.info(f"{symbol}数据有变化,时间差:{diff_ts}ms")
            else:
                self.kline_dict[symbol] = {ts: kline_data}
    
    # 判断K线是否有插针
    async def check_kline_needle(self, symbol, kline_data):
        diff_ratio = kline_data[1]/kline_data[2]-1
        if diff_ratio > self.needle_limit and time.time()-self.last_needle_ts > self.needle_time_limit:
            self.last_needle_ts = time.time()
            self.log.info(f"盘面报警:{symbol} K线有插针,差值比例:{diff_ratio}")
            await self.send_tg(self.chat_id, f"盘面报警:合约{symbol} K线有插针,波动比例:{round(diff_ratio*100, 2)}%  立即检查做市策略")
    
    async def get_all_symbols(self):
        res = await self.old_rest.get_symbols(quan=True)
        for s in res:
            self.all_symbols.append(s.symbol)
        self.log.info(f"获取全部合约交易对:{self.all_symbols}")
    
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
            
    # 检查wss推送是否正常
    async def check_wss(self):
        # 外盘数据
        if time.time() - self.bn_ws_ts > self.check_ws_ts_limit:
            mess = f"binance wss超过{self.check_ws_ts_limit}s未推送数据"
            tb.warning(mess, 'risk')
        # 内盘数据
        if time.time() - self.websea_ws_ts > self.check_ws_ts_limit:
            mess = f"websea wss超过{self.check_ws_ts_limit}s未推送数据"
            tb.warning(mess, 'risk')
    
    # 计算精度
    async def count_decimal(self, depth_data):
        bid = str(depth_data[0])
        ask = str(depth_data[1])
        bid_size = 0 if '.' not in bid else len(bid.split('.')[1])
        ask_size = 0 if '.' not in ask else len(ask.split('.')[1])
        return max(bid_size, ask_size)
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        all_symbols = self.all_symbols
        for s in all_symbols:
            # 判断内盘盘口是否没有变化
            try:
                websea_depth = self.websea_depth[s]
                binance_depth = self.binance_depth[s]
            except:
                # self.log.error(f"报错数据:websea:{depth_data} bn:{binance_data}")
                continue
            # 处理内外盘精度不一致
            websez_size = await self.count_decimal(websea_depth)
            binance_size = await self.count_decimal(binance_depth)
            size = min(websez_size, binance_size)
            websea_depth = [round(i, size) for i in websea_depth]
            binance_depth = [round(i, size) for i in binance_depth]
            
            now_ts = int(time.time())
            if s not in self.check_depth_data:
                self.check_depth_data[s] = {now_ts: [websea_depth, binance_depth]}
            check_ts = list(self.check_depth_data[s].keys())[0]   # 上次检查时间戳
            last_websea_depth = list(self.check_depth_data[s].values())[0][0]
            last_binance_depth = list(self.check_depth_data[s].values())[0][1]
            
            # 只有内盘不动外盘动的情况报警
            con1 = last_websea_depth == websea_depth and last_binance_depth != binance_depth
            bid_cross = websea_depth[0]/binance_depth[1] - 1 > self.risk_limit  # 判断盘口cross比例
            ask_cross = binance_depth[0]/websea_depth[1] -1 > self.risk_limit
            con2 = bid_cross or ask_cross
            if con1 and con2:
                diff_ts = int(now_ts-check_ts)   # 单位s
                # self.log.info(f"{s}内盘上次深度:{last_websea_depth} 本次深度:{websea_depth} 币安上次深度:{last_binance_depth} 币安本次深度:{binance_depth} 内盘深度不变持续时间:{diff_ts}s")
                if diff_ts > self.time_limit:
                    self.log.info(f"盘面报警:合约{s}超过{diff_ts}s 盘口数据不变")
                    # await self.send_tg(self.chat_id, f"盘面报警:合约{s}超过{diff_ts}s 盘口数据不变。立即检查做市策略\n当前内盘bbo:{websea_depth} 币安bbo:{binance_depth}")
            else:
                self.check_depth_data[s] = {now_ts: [websea_depth, binance_depth]}
        


def main(path='contract_kline_monitor_config.py'):
    config = __import__(path.split(".py")[0])
    strategy(config).run()


if __name__ == '__main__':
    main()



