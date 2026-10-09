
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
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
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # 对冲
    async def hedge(self):

        res = await self.okx_rest.fetch_position_history(limit=20)
        #print(res)
        profit = 0
        for i in res:
            #print(i)
            profit += round(float(i['realizedPnl']), 2)
            date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{date} {i['symbol']} 持仓:{i['openMaxPos']} 持仓方向:{i['direction']} 开仓:{round(float(i['openAvgPrice']), 2)} 平仓:{round(float(i['closeAvgPrice']), 2)} 盈亏:{round(float(i['realizedPnl']), 2)} 手续费:{round(float(i['fee']), 2)} 资金费率:{round(float(i['info']['fundingFee']), 2)} 杠杆:{i['leverage']}")
        #print(profit)
        #exit()


        begin_time = '2025-06-26 00:00:00'
        end_time = '2025-06-25 23:59:59'
        res = await self.okx_rest.fetch_history_list('ETH-USDT', 100)
        buy_vol = 0
        sell_vol = 0
        buy_amt = 0
        sell_amt = 0
        for i in res:
           # i['timestamp']  # 是东八区时间
           date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000)
           date = date.astimezone(datetime.timezone(datetime.timedelta(hours=8)))
           date = date.strftime("%Y-%m-%d %H:%M:%S")
           if date >= begin_time and date <= end_time and i['filled'] != 0:
               print(date, i['timestamp'], i['id'], i['average'], i['filled'], i['side'])
               if i['side'] == 'buy':
                   buy_vol += i['filled']
                   buy_amt += i['average'] * i['filled']
               elif i['side'] =='sell':
                   sell_vol += i['filled']
                   sell_amt += i['average'] * i['filled']
        buy_ave_price = buy_amt/buy_vol if buy_vol != 0 else 0
        sell_ave_price = sell_amt/sell_vol if sell_vol != 0 else 0
        profit = sell_ave_price*min(buy_vol, sell_vol) - buy_ave_price*min(buy_vol, sell_vol)
        print(f"历史委托:{buy_vol} {sell_vol} {buy_amt} {sell_amt} {profit}")
        #exit()
        
        #res = await self.ws_rest.get_symbols(quan=True)
        #print(res)
        #exit()

        #data = await self.okx_rest.fetch_trade_fee()
        #print(data)

        #data = await self.okx_rest.fetch_trade('ETH-USDT')
        #print(data)
        #exit()
        
        #res = await self.okx_rest.fetch_order_detail('ETH-USDT', order_id='2712587261260324864')
        #print(res)
        #exit()

        res = await self.okx_rest.fetch_balance()
        print(f"账户余额:{res}")
        exit()

        symbols = []
        while 1:
        #    break
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id='478061')  # Z组478061
                hold_list_page = data['data']['pager']['total_page']
                temp_hold_list += data['data']['data']
                for n in range(2, hold_list_page+1):
                    data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20, user_id='478061')
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(0.5)
                break
            except:
                pass
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        #print(f"持仓列表:{hold_list}")
        sum_profit = 0
        for i in hold_list:
            #print(i)
            sum_profit += float(i['profit_loss'])
            side = 'buy' if i['openDirection'] == 1 else 'sell'
            print(i['symbol'], '持仓:', i['amount'], '方向:', side, '盈亏:', i['profit_loss'], i['zhqy'])
            symbols.append(i['symbol'])
        print(sum_profit)
        #exit()

        cancel_symbols = {}
        res = await self.okx_rest.fetch_current_list(limit=100, state='live')
        for s in res:
            if s['symbol'] in cancel_symbols:
                cancel_symbols[s['symbol']].append(s['id'])
            else:
                cancel_symbols[s['symbol']] = [s['id']]
        #for s, ids in cancel_symbols.items():
        #    res = await self.okx_rest.cancel_order_batch(s, order_id=ids)
        #    print(f"取消所有委托:{res}")
            
        symbol = 'APE-USDT'
        side = 'buy'
        vol = 1
        price = '0.578'
        #result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
        #                                         side=side, amount=vol, reduceOnly=True,
        #                                        price=price, tdMode='cross')
        #print(result)


        res = await self.okx_rest.fetch_current_list(symbol=symbol, limit=100, state='live')
        print("当前委托:")
        for i in res:
            print(f"{i['symbol']} {i['price']} {i['amount']} {i['side']} {i['status']} {i['id']} {i['leverage']}")
            if i['symbol'] == symbol:
                cancel_res = await self.okx_rest.cancel_order(i['symbol'], i['id'])
                print(cancel_res)
        
        #res = await self.okx_rest.cancel_order(symbol=symbol, order_id=)
        #print(f"撤单:{res}")
        #exit()
        
        symbols2 = []
        hedge_pos = await self.okx_rest.fetch_position()
        #print(f"持仓:{hedge_pos}")
        for i in hedge_pos:
            #print(i)
            print(f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
            symbols2.append(i['symbol'])

        print(sorted(symbols))
        print(sorted(symbols2))

def main(path='grid_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()
    # while True:
    #     time.sleep(999999)



if __name__ == '__main__':
    main()

