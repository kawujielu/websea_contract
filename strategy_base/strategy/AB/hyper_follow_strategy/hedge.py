
import sys
import time
import datetime
import traceback
import mysql.connector
import asyncio
sys.path.append('../../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.websea.contract_quan import WebseaContract as new_ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
import objects.contract_request.websea as ocw

strategy_name = "copy OKX的跟单策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config()  # 读取配置
                
    def _load_config(self):
        """读取配置文件
        """
        # 配置全局变量
        #[setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        #config = config.config
        # 配置账号
        self.ws_rest = ws_contract_rest("23cb167617bb8398ccfcf74506t64669304","9mowefaye9nhyxf2w7zq")  #ws_contract_rest(config['base_token'], config['base_secret'])
        self.new_ws_rest = new_ws_contract_rest("23cb167617bb8398ccfcf74506t64669304","9mowefaye9nhyxf2w7zq")
        #self.new_ws_rest = new_ws_contract_rest(config['base_token'], config['base_secret'])
        #self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        self.ws_rest.DEBUG = False
        self.new_ws_rest.DEBUG = False


    async def on_first(self):
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        self.log.info("=======timer======")
    
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_db",
            port=3306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
    # 对冲
    async def hedge(self):
        await self.connect_db()
        #self.cursor.execute("TRUNCATE TABLE okx_teacher_deals")
        #self.db.commit()
        
        #self.cursor.execute(f"SELECT * FROM okx_teacher_deals")
        #data = self.cursor.fetchall()
        #for i in data[-20:]:
        #    print(i)
        
        balance = await self.ws_rest.get_walletList(is_full=2)
        self.log.info(f"账户权益:{balance.avail}") # 没有资金
        
        res = await self.new_ws_rest.fetch_history_list()
        print(res)
        
        self.symbol_size = {}
        res = await self.ws_rest.get_symbols(quan=True)
        for s in res:
            self.symbol_size[s.symbol] = s.contract_size

        self.all_pos = {}
        res = await self.ws_rest.get_position(is_full=1)
        pos_mess = f"{strategy_name}持仓:\n"
        for p in res:
            symbol = p.symbol
            pos = p.amount
            profit = p.profit
            open_price = p.open_price_avg
            leval = p.lever_rate
            liqu_price = p.liquidation_price
            side = 'buy' if p.type == 1 else 'sell'
            size = self.symbol_size.get(symbol, 1)
            pos *= size
            res = await self.ws_rest.get_index(symbol)
            mark_price = res.price
            self.all_pos[symbol] = {side: pos}
            pos_mess += f"{symbol} 持仓:{pos} 方向:{side} 开仓价:{open_price} 标记价:{mark_price} 盈亏:{profit} 杠杆:{leval} 爆仓价:{liqu_price}\n"
        self.log.info(pos_mess)
        #exit()

        symbol = 'ENS-USDT'
        od_type = ocw.OrderType.buy_market
        vol = 2
        price = 70
        precision = await self.ws_rest.get_precision(symbol, quan=True)
        print(precision)
        res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                        price=price, amount=abs(vol), \
                                                        precision=precision, contract_type='close')
        print(f"下单回报:{res}")
 

        # res = await self.ws_rest.order_detail('BM5555551754377691702WRR3VN')
        # print(f"查询订单:{res}")
        # res = await self.ws_rest.order_cancel('ADA-USDT')
        # print(f"撤单:{res}")

def main():
    Strategy().run()



if __name__ == '__main__':
    main()




