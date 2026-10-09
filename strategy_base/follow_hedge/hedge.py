
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
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
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)
        self.rest2 = Contract('d6fc035b4e4319319743b70cbdk59445990','mwo2m8xyxappj07xr2d7')
        self.ws_rest = Contract(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])


    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # 对冲
    async def hedge(self):
        rest = await self.rest.fetch_balance_normal(1)
        print(rest)

        #res = await self.okx_rest.fetch_history_list(limit=100)
        #for i in res:
        #    print(i)
        #    date = datetime.datetime.fromtimestamp(int(i['timestamp']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        #    print(date, i['symbol'], i['filled'])
        #print(len(res))
        #exit()
        
        profit_sum = 0
        begin_date = "2025-07-25 00:00:00"
        end_date = "2025-08-25 00:00:00"
        # 修改日期为13位时间戳
        begin_ts = int(time.mktime(time.strptime(end_date, "%Y-%m-%d %H:%M:%S"))) * 1000
        end_ts = int(time.mktime(time.strptime(begin_date, "%Y-%m-%d %H:%M:%S"))) * 1000
        res = await self.okx_rest.fetch_position_history(limit=100, after=begin_ts, before=end_ts)
        profit_sum = 0
        #res = await self.okx_rest.fetch_position_history(limit=10)
        for i in res:
            #print(i)
            profit_sum += round(float(i['realizedPnl']), 2)
            profit_sum += round(float(i['fee']), 2)
            date = datetime.datetime.fromtimestamp(int(i['info']['cTime']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            open_date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            close_date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            #print(date, round(float(i['realizedPnl']), 2), round(float(i['fee']), 2))
            print(f"开仓:{open_date} 平仓:{close_date} {i['symbol']} 持仓:{i['openMaxPos']} 持仓方向:{i['direction']} 开仓:{round(float(i['openAvgPrice']), 2)} 平仓:{round(float(i['closeAvgPrice']), 2)} 盈亏:{round(float(i['realizedPnl']), 2)} 手续费:{round(float(i['fee']), 2)} 资金费率:{round(float(i['info']['fundingFee']), 2)} 杠杆:{i['leverage']}")
        print(profit_sum)

        res = await self.okx_rest.fetch_balance()
        print(f"对冲账户权益:{res}")
        exit()

        #res = await self.okx_rest.fetch_order_detail('ETH-USDT', order_id='2905639717718384640')
        #print(res)
        #exit()

        #res = await self.okx_rest.set_leverage(10, 'BTC-USDT', marginMode='cross')
        #print(res)
        #exit()

        cancel_orders = []
        res = await self.okx_rest.fetch_current_list(limit=100, state='live')
        print("当前委托:")
        for i in res:
            cancel_orders.append(i['id'])
        #    print(f"{i['symbol']} {i['price']} {i['amount']} {i['side']} {i['status']} {i['id']} {i['leverage']}")

        #res = await self.okx_rest.cancel_order_batch(symbol='ETH-USDT', order_id=cancel_orders)
        #print(f"撤单:{res}")

        # res = await self.okx_rest.set_position_mode(False)
        # print(res)
        # # return
        
        #for s in ['BTC-USDT','ETH-USDT','TRX-USDT','XRP-USDT','BCH-USDT','SOL-USDT','ICP-USDT','ADA-USDT','SIGN-USDT','DOT-USDT','SUI-USDT','ONDO-USDT','DOGE-USDT','WCT-USDT','AVAX-USDT','SHIB-USDT','PEPE-USDT','FLOKI-USDT','XLM-USDT']:
        #    res = await self.okx_rest.set_leverage(5, s, marginMode='cross')
        #    print(res)
        #exit()

        while 1:
        #    break
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.rest.fetch_hold_list(page_size=20, user_id='583268')
                print(data)
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                for n in range(2, hold_list_page+1):
                    data = await self.rest.fetch_hold_list(page=n, page_size=20, user_id='583268')
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(0.5)
                break
            except:
                print(traceback.format_exc())
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        print(f"持仓列表:{hold_list}")
        for i in hold_list:
            # print(i)
            print(i['symbol'], i['amount'], i['profit_loss'])
        
        symbol = 'ETH-USDT'
        price = '3250'
        side = 'sell'
        vol = 0.07
        #result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
        #                                    side=side, amount=vol, 
        #                                    price=price, tdMode='cross')
        #print(result)
            
        hedge_pos = await self.okx_rest.fetch_position()
        #print(f"外盘持仓:{hedge_pos}")
        for i in hedge_pos:
            print(f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
        
            


def main(path='follow_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()
