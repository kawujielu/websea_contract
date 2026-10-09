
import sys
import time
import datetime
import traceback
import mysql.connector
import asyncio
import pandas as pd
from collections import deque   # FIFO撮合
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.websea.contract_quan import WebseaContract as new_ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
import objects.contract_request.websea as ocw

strategy_name = "copy OKX跟单策略数据统计"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self.contract_unit = {}  # 合约单位
                
    def _load_config(self, config):
        """读取配置文件
        """
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest = ws_contract_rest('d6fc035b4e4319319743b70cbdk59445990','mwo2m8xyxappj07xr2d7')
        self.ws_rest1 = new_ws_contract_rest('d6fc035b4e4319319743b70cbdk59445990','mwo2m8xyxappj07xr2d7')
        self.ws_rest2 = new_ws_contract_rest('41d5ce0235d482aab2e60a779dl60001992','pigm0st0p01j6x9esb5o')
        self.ws_rest3 = new_ws_contract_rest('c05f95b1c43bde42556b6e34e1y67448667','fikpc1jlgqb05b3yq9r9')
        self.ws_rest4 = new_ws_contract_rest('7287274648396dc3f797c553ecd55748600','fwz3tuyik4nvs60v7i8v')
        self.ws_rest5 = new_ws_contract_rest('9294e3098ff151a5aa854926b6h57978648','ye31wgeixol2wumb6iuv')
        self.ws_rest6 = new_ws_contract_rest('19f991d6d7cdaac1083264c933j59094046','c4029mguoouis2ph0yox')
        self.ws_rest7 = new_ws_contract_rest('fcd8cd9ea07b0a7f0c85b12fa3c55191807','ocpvq2nh50mzr2krzm5i')
        self.ws_rest8 = new_ws_contract_rest('23cb167617bb8398ccfcf74506t64669304','9mowefaye9nhyxf2w7zq')
        self.ws_rest9 = new_ws_contract_rest('53dab96a47bd1021d9dcef3d9fk59654212','p56gp29wyz5dybrbu430')
        self.ws_rest10 = new_ws_contract_rest('4905669b7f070117807133b5e3j59096908','jyulvaphe2hnrsbvatj1')
        self.acct = {'James':self.ws_rest1, 
                     'one more': self.ws_rest2,
                     'eric_deal': self.ws_rest3,
                     '要想富满仓隔夜是条路': self.ws_rest4,
                     '404alive': self.ws_rest5,
                     '五条悟虚式【茈】': self.ws_rest6,
                     'labubugogogo': self.ws_rest7,
                     'zero量化': self.ws_rest8,
                     '酉时三刻': self.ws_rest9,
                     '致敬teacher郑': self.ws_rest10 }

    async def on_first(self):
        await self.get_symbol_unit()  # 获取合约单位
        await self.hedge()

    async def on_timer(self):
        self.log.info("=======timer======")
    
    async def get_symbol_unit(self):
        while True:
            try:
                data = await self.ws_rest.get_symbols()
                # 若走这个逻辑,表示交易对下架
                for i in data:
                    if not isinstance(i, list):
                        self.contract_unit[i.symbol] = i.contract_size
                print(self.contract_unit)
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            asyncio.sleep(0.5)
    
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
        mess = ""
        for name, task in self.acct.items():
            res = await task.fetch_history_list(limit=500)
            {'symbol': 'SUI-USDT', 'id': 'BM55750317557600849631BFVAK', 'clientOrderId': None, 
             'price': '', 'stopPrice': None, 'triggerPrice': None, 'amount': 509, 'amountIsNum': False, 
             'side': 'buy', 'type': 'market', 'status': 'closed', 'leverage': 5.0, 'timeInForce': None, 
             'postOnly': None, 'reduceOnly': True, 'marginMode': 'isolated', 'average': 3.517, 'filled': 509.0, 
             'cost': None, 'remaining': None, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': None, 
             'timestamp': 1755760084, 'cost_time': 0.04762233595829457, 'info': 
                 {'order_id': 'BM55750317557600849631BFVAK', 'ctime': 1755760084, 'symbol': 'SUI-USDT', 
                  'price': 'market', 'price_avg': '3.517', 'lever_rate': 5, 'amount': '509', 
                  'deal_amount': '509', 'type': 'buy-market', 'status': 3, 'contract_type': 'open', 
                  'trigger_price': '', 'stop_profit_price': None, 'stop_loss_price': None, 'profit': '0'}}
            deals = []
            for i in res:
                #if i['timestamp'] < 1756656000 or i['timestamp'] > 1757260800:
                #    continue
                symbol = i['symbol']
                side = i['side']
                price = i['average']
                vol = i['filled']*self.contract_unit[symbol]
                status = i['status']
                ts = i['timestamp']
                deals.append([symbol, side, price, vol, ts])
                
            df = pd.DataFrame(deals, columns=["symbol","side","price","qty","ts"])
            df["price"] = df["price"].astype(float)
            df["qty"] = df["qty"].astype(float)

            positions = {}  # 持仓
            trades = []     # 已平仓交易记录
            trade_num = len(deals)  # 交易笔数

            for _, row in df.iterrows():
                sym, side, price, qty, ts = row["symbol"], row["side"], row["price"], row["qty"], row["ts"]
                if sym not in positions:
                    positions[sym] = deque()

                if side == "buy":
                    # 买入 -> 平空 或 开多
                    remaining = qty
                    while remaining > 0 and positions[sym] and positions[sym][0][0] == "sell":
                        pos_side, pos_price, pos_qty = positions[sym][0]
                        trade_qty = min(remaining, pos_qty)
                        pnl = (pos_price - price) * trade_qty  # 空仓盈利 = 开仓价 - 平仓价
                        trades.append((sym, pnl, ts))
                        if pos_qty > trade_qty:
                            positions[sym][0] = (pos_side, pos_price, pos_qty - trade_qty)
                        else:
                            positions[sym].popleft()
                        remaining -= trade_qty
                    if remaining > 0:
                        positions[sym].append(("buy", price, remaining))

                else:  # sell
                    # 卖出 -> 平多 或 开空
                    remaining = qty
                    while remaining > 0 and positions[sym] and positions[sym][0][0] == "buy":
                        pos_side, pos_price, pos_qty = positions[sym][0]
                        trade_qty = min(remaining, pos_qty)
                        pnl = (price - pos_price) * trade_qty  # 多仓盈利 = 平仓价 - 开仓价
                        trades.append((sym, pnl, ts))
                        if pos_qty > trade_qty:
                            positions[sym][0] = (pos_side, pos_price, pos_qty - trade_qty)
                        else:
                            positions[sym].popleft()
                        remaining -= trade_qty
                    if remaining > 0:
                        positions[sym].append(("sell", price, remaining))

            # 结果统计
            trades_df = pd.DataFrame(trades, columns=["symbol","pnl","ts"])
            total_trades = len(trades_df)
            wins = (trades_df["pnl"] > 0).sum()
            losses = (trades_df["pnl"] < 0).sum()
            win_rate = wins / total_trades if total_trades > 0 else 0
            profit = trades_df[trades_df["pnl"]>0]["pnl"].sum()
            loss = trades_df[trades_df["pnl"]<0]["pnl"].sum()
            pl_ratio = profit / abs(loss) if loss < 0 else float("inf")
            cumulative_pnl = trades_df["pnl"].sum()
            last_week_pnl = trades_df[trades_df["ts"]>int(time.time()-7*24*60*60)]["pnl"].sum()
            
            mess += (f"{name} 账户:\n交易笔数:{trade_num}\n胜率:{round(win_rate*100, 2)}%\n盈亏比:{round(pl_ratio, 2)}"+
                  f"\n上周盈亏:{round(last_week_pnl, 2)}\n累计盈亏:{round(cumulative_pnl, 2)}\n\n")     #分品种累计收益:\n{trades_df.groupby('symbol').sum()}\n\n")  已平仓笔数:{total_trades}\n
        print(mess)


def main(path='ok_follow_deal_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()






