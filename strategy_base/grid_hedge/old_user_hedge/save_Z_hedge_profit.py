
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
from utils import Toolbox as tb

strategy_name = "指定用户ok网格对冲策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        #self.okx_rest = okx_rest.OkexContract('02a22951-cc40-4852-ae04-8da58c24df36','C88659593F833C4C316ACD21416DCAE3','Tbtb794972.')

    async def on_first(self):
        self.log.info("=======first=======")
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.hedge, CronTrigger(hour="*"))
        
    # 对冲
    async def hedge(self):
        mess = ""
        res = await self.okx_rest.fetch_balance()
        mess += (f"账户余额:{res}\n")
        
        sum_profit = 0
        hedge_pos = await self.okx_rest.fetch_position()
        mess += (f"持仓:\n")
        for i in hedge_pos:
            sum_profit += i['unrealizedPnl']
            mess += (f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
        mess += (f"总浮动盈亏:{sum_profit}\n")
        print(f"{datetime.datetime.now()}\n{mess}")

        if sum_profit > -2000:
            tb.warning(f"Z组长期持仓用户对冲仓位整体盈利:{sum_profit}U,考虑全部平仓", 'risk')

def main(path='grid_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()



