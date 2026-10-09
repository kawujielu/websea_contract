
import sys
import time
import datetime
import traceback
import asyncio
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
# from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
# from crypto_center.client.rest.websea.contract_quan import WebseaContract
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
        self.ws_rest = Contract(config['base_token'], config['base_secret'])
        self.token_rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        # self.new_rest = WebseaContract(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'], config['hedge_secret'], config['passphrase'])
        #self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        #self.okx_rest = okx_rest.OkexContract('47a613f4-06de-4bb9-aa7b-d1758d3f224e','6AFCB49A175F20474BE122717DFE52DB','Tbtb794972.')
        #self.okx_rest = okx_rest.OkexContract('02a22951-cc40-4852-ae04-8da58c24df36','C88659593F833C4C316ACD21416DCAE3','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b1c2b6e5-daf6-4ce4-8198-96aee61c3b17','E26B8728F3F85FED0451AB50F3CD1A25','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')
        # self.okx_rest = okx_rest.OkexContract('b29736e4-6a9b-4a29-ba07-048d75703924','9E0648BE0E41C21C857C93D1CF4F2588','Tbtb794972.')

    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
        
    # 对冲
    async def hedge(self):
        profit_sum = 0
        res = await self.okx_rest.fetch_position_history(symbol='ETH-USDT', limit=10)
        for i in res:
            #print(i)
            profit_sum += round(float(i['realizedPnl']), 2)
            date = datetime.datetime.fromtimestamp(int(i['info']['cTime']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
            open_date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
            date = datetime.datetime.fromtimestamp(i['timestamp'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
            close_date = (datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        #     #print(date, round(float(i['realizedPnl']), 2), round(float(i['fee']), 2))
            print(f"开仓:{open_date} 平仓:{close_date} {i['symbol']} 持仓:{i['openMaxPos']} 持仓方向:{i['direction']} 开仓:{round(float(i['openAvgPrice']), 2)} 平仓:{round(float(i['closeAvgPrice']), 2)} 盈亏:{round(float(i['realizedPnl']), 2)} 手续费:{round(float(i['fee']), 2)} 资金费率:{round(float(i['info']['fundingFee']), 2)} 杠杆:{i['leverage']}")
        print(profit_sum)
        # exit()

        res = await self.okx_rest.fetch_balance()
        print(f"账户余额:{res}")
        #exit()

        # res = await self.ws_rest.fetch_follow_sum(trader_id=485087, token='c1cf4185b2bed317aeb6e6674491fbef')
        # self.log.info(f"查询跟单比例:{res} {len(res)}")
        #exit()
        
        while 1:
            #break
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.token_rest.fetch_hold_list(page_size=20, user_id='606019')
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                for n in range(2, hold_list_page+1):
                    data = await self.token_rest.fetch_hold_list(page=n, page_size=20, user_id='606019')
                    temp_hold_list += data['data']
                    await asyncio.sleep(0.5)
                break
            except:
                pass
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        print(f"内盘持仓列表:")
        #if hold_list == []:
        #    tb.warning('迷人等待空仓了，启动策略', 'risk')
        for i in hold_list:
            #print(i)
            side = '多单' if i['openDirection'] == 1 else '空单'
            vol = float(i['amount']) if i['openDirection'] == 1 else -float(i['amount'])
            # print(vol, type(vol))
            # amt = abs(vol*0.01*float(i['avgPrice']))
            # level = round(amt/float(i['zhqy']), 1)
            print(f"{i['symbol']} {side} 持仓:{vol} 成本:{i['avgPrice']} 盈亏:{i['profit_loss']}")   # 爆仓价:{i['parity']}, 账户权益:{i['zhqy']}, 真实杠杆:{level}")


        symbol = 'ETH-USDT'
        side = 'sell'
        vol = 4.13
        price = '2700'
        #result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
        #                                        side=side, amount=vol, 
        #                                        price=price, tdMode='cross')
        #print(result)

        #res = await self.okx_rest.cancel_order(symbol=symbol, order_id=2829129190066642944)
        #print(f"撤单:{res}")
        #exit()

        cancel_orders = []
        res = await self.okx_rest.fetch_current_list(symbol='ETH-USDT', limit=100, state='live')
        print("当前委托:")
        for i in res:
            cancel_orders.append(i['id'])
            print(f"{i['symbol']} {i['price']} {i['amount']} {i['side']} {i['status']} {i['id']} {i['leverage']}")
        
        #res = await self.okx_rest.cancel_order_batch(symbol=symbol, order_id=cancel_orders)
        #print(f"撤单:{res}")
        
        hedge_pos = await self.okx_rest.fetch_position()
        print(f"外盘持仓:")  #{hedge_pos}")
        for i in hedge_pos:
        #     #print(i)
            print(f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n")
            #side = 'buy' if i['contracts'] > 0 else'sell'
            #vol = abs(i['contracts'])
            #price = i['markPrice']*(1+0.001) if side == 'buy' else i['entryPrice']*(1-0.001)
            # 平仓
            #result = await self.okx_rest.create_order(symbol=i['symbol'], order_type='limit', 
            #                                   side=side, amount=vol, 
            #                                   price=price, tdMode='cross')
            #print(result)


def main(path='major_player_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()





