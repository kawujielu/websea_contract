
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from crypto_center.client.rest.okex import contract as okx_rest

strategy_name = "指定用户ok网格对冲策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config, config2):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config, config2)  # 读取配置
        self.rc_task = rc.RestClient()
                
    def _load_config(self, config, config2):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        [setattr(self, k, v) for k, v in vars(config2).items()]        # 批量生成所有参数
        config = config.config
        config2 = config2.config
        # 配置账号
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.ws_rest = Contract(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        self.okx_rest2 = okx_rest.OkexContract(config2['hedge_token'],config2['hedge_secret'],config2['passphrase'])
        self.rest.DEBUG = False
        self.ws_rest.DEBUG = False
        self.okx_rest.DEBUG = False
        self.okx_rest2.DEBUG = False
        
    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.hedge, CronTrigger(minute="*"))  # 每5min执行一次
        
    async def send_tg(self, mess):
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=send_text)

    # 对冲
    async def hedge(self):
        while 1:
        #    break
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.rest.fetch_hold_list(page_size=20, user_id='644676')
                print(data)
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                for n in range(2, hold_list_page+1):
                    data = await self.rest.fetch_hold_list(page=n, page_size=20, user_id='644676')
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(0.5)
                break
            except:
                print(traceback.format_exc())
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        # print(f"持仓列表:{hold_list}")
        mess = "smart_money策略:\n"
        for i in hold_list:
            if i['symbol'] in ['BTC-USDT','ETH-USDT']:
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                unit = 0.01 if i['symbol'] == 'ETH-USDT' else 0.001
                print(i['symbol'], float(i['amount'])*unit, side, '成本:', i['avgPrice'], '盈亏:', i['profit_loss'], '标记价格:',i['mark_price'])
                mess += f"644676内盘持仓:{i['symbol']} {float(i['amount'])*unit}个{i['symbol'].split('-')[0]} {side} 成本:{round(float(i['avgPrice']),2)} 盈亏:{round(float(i['profit_loss']),2)} 标记价格:{round(float(i['mark_price']),2)}\n"

        hedge_pos = await self.okx_rest.fetch_position()
        for i in hedge_pos:
            #if i['symbol'] in ['BTC-USDT','ETH-USDT']:
            liqPx = round(float(i['info']['liqPx']),2) if i['info']['liqPx'] else None
            print(f"对冲持仓:{i['symbol']} {i['contracts']}个{i['symbol'].split('-')[0]} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
            mess += f"对冲持仓:{i['symbol']} {i['contracts']}个{i['symbol'].split('-')[0]} 浮动盈亏:{round(float(i['unrealizedPnl']),2)} 成本:{round(float(i['entryPrice']),2)} 当前价格:{round(float(i['markPrice']),2)} 杠杆:{i['leverage']} 爆仓价:{liqPx}\n"
        mess += "\n全量用户持仓对冲策略:\n"
        hedge_pos = await self.okx_rest2.fetch_position()
        for i in hedge_pos:
            #if i['symbol'] in ['BTC-USDT','ETH-USDT']:
            liqPx = round(float(i['info']['liqPx']),2) if i['info']['liqPx'] else None
            print(f"对冲持仓:{i['symbol']} {i['contracts']}个{i['symbol'].split('-')[0]} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
            mess += f"对冲持仓:{i['symbol']} {i['contracts']}个{i['symbol'].split('-')[0]} 浮动盈亏:{round(float(i['unrealizedPnl']),2)} 成本:{round(float(i['entryPrice']),2)} 当前价格:{round(float(i['markPrice']),2)} 杠杆:{i['leverage']} 爆仓价:{liqPx}\n"
        
        if mess:
            await self.send_tg(mess)
        #else:
        #    await self.send_tg("644676用户全部平仓")



def main(path='smart_hedge_hedge_config.py', path2='pos_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    config2 = __import__(path2.split(".py")[0])
    Strategy(config, config2).run()



if __name__ == '__main__':
    main()



