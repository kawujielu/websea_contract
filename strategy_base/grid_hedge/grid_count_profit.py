
import sys
import time
import datetime
import traceback
import pytz
import asyncio
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest

from crypto_center.client.rest.okex import contract as okx_rest

strategy_name = "指定用户ok网格对冲策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self.save_log = tb.Log("/home/ubuntu/strategy_base/strategy/monitor/strategy_profit.log")
        self.log_path = "/home/ubuntu/strategy_base/strategy/grid_hedge/deal_log/535229_hedge_deals.log"
        self.keyword = "对冲 价"
        # self.date = '2025-06-15 16:00:00'
        # 创建东八区时区对象（UTC+8）
        # tz_utc8 = datetime.timezone(datetime.timedelta(hours=8))  # :ml-citation{ref="4" data="citationList"}
        # # 生成今日10点时间对象
        # today_10am = datetime.datetime.now(tz_utc8).replace(
        #     hour=10, minute=0, second=0, microsecond=0
        # )
        # self.date = today_10am.strftime("%Y-%m-%d %H:%M:%S")
        now = datetime.datetime.now()
        yesterday = now - datetime.timedelta(days=3)
        self.date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        self.log.info(f"策略启动时间：{self.date}")
        
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])

    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # 对冲
    async def hedge(self):
        buy_vol = 0
        sell_vol = 0
        buy_amt = 0
        sell_amt = 0
        try:
            with open(self.log_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if self.keyword in line:
                        print(line)
                        if "'code': '1'," in line:
                            continue
                        symbol = line.split(f"'symbol': '")[1].split("',")[0] # okx的交易对
                        id = eval(line.split("回报:")[1])['info']['data'][0]['ordId'] # okx的下单id
                        id = int(id)
                        print(symbol, id)
                        res = await self.okx_rest.fetch_order_detail(symbol, order_id=id)
                        
                        ts = res['timestamp']
                        timezone = timezone = pytz.timezone("Asia/Shanghai")
                        date = datetime.datetime.fromtimestamp(int(ts/1000), tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
                        print(date, self.date)
                        if date < self.date:
                            print(f"跳过")
                            continue
                        
                        if res['side'] == 'buy':
                            buy_vol += res['amount']
                            buy_amt += res['average']*res['amount']
                        else:
                            sell_vol += res['amount']
                            sell_amt += res['average']*res['amount']
                        await asyncio.sleep(0.5)
            profit = sell_amt - buy_amt
            log_mess = (f"{datetime.date.today()} 网格对冲策略 买入数量：{round(buy_vol,2)}, 买入金额：{round(buy_amt, 2)}, 卖出数量：{round(sell_vol, 2)}, 卖出金额：{round(sell_amt, 2)}, 盈亏：{round(profit, 2)}")
            self.save_log.write(log_mess)
            
        except FileNotFoundError:
            print(f"错误：日志文件 {self.log_path} 不存在")
        except PermissionError:
            print(f"错误：没有权限读取 {self.log_path}")
        except Exception as e:
            print(f"发生未知错误: {traceback.format_exc()}")
        
def main(path='grid_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()

