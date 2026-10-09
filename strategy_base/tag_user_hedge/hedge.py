
import sys
import time
import datetime
import pytz
import asyncio
import traceback
import numpy as np
sys.path.append('/home/ubuntu/crypto_center/client/rest/okex')
import contract as okx


class Strategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()  # 读取配置
        self.symbol_precision = {}

    def _load_config(self):
        """读取配置文件
        """
        # self.task = okx.OkexContract(apiKey='4416630e-2b3f-4016-b634-bc16d231224c',secret='026A1C823ED985A3E24FFCF70C9EE14B',passphrase='265199_JJsy')
        self.task = okx.OkexContract(apiKey='e1934f97-f831-418e-b154-1ec5a0415f9a',secret='A2D8A15D0DE4B59BD7B270A34BB1273C',passphrase='usRolUVNuyBbEF@7 ')

    async def main(self):
        # depth = await self.task.fetch_depth('ADA-USDT')
        # print(f"ok深度:{depth}")

        # res = await self.task.fetch_precision()
        # # print(f"{res}")
        # for i in res:
        #     price_precision = int(-np.log10(i['price']))    # 价格精度
        #     amount_precision = int(-np.log10(i['amount']))  # 数量精度
        #     face_value = i['faceValue']  # 合约面试,1张=多少币
        #     self.symbol_precision[i['info']['instFamily']] = (price_precision, amount_precision, face_value)
        # print(f"交易对精度+面值:{self.symbol_precision}")

        #res = await self.task.fetch_precision('BCH-USDT')
        #print(f"{res}")
        #for s, v in res.items():
        #    if s == 'BCH-USDT':
        #        price_precision = int(-np.log10(v['price']))    # 价格精度
        #        amount_precision = int(-np.log10(v['amount']))  # 数量精度
        #        face_value = v['faceValue']  # 合约面试,1张=多少币
        #print(f"交易对精度:{price_precision}, {amount_precision} 合约面值:{face_value}")  # 精度不对

        # res = await self.task.fetch_balance()
        # # print(f"账户权益:{res}")
        # balance = res['USDT']['total']
        # avail = res['USDT']['free']
        # print(f"账户权益:{balance} 可用:{avail}")

        # res = await self.task.fetch_position()
        # #print(f"账户持仓:{res}")
        # text = ''
        # for i in res:
        #     text += f"{i['symbol']}持仓:{i['contracts']} 浮动盈亏:{i['unrealizedPnl']} 成本:{i['entryPrice']} 当前价格:{i['markPrice']} 杠杆:{i['leverage']} 爆仓价:{i['info']['liqPx']}\n"
        # print(text)

        # list_res = await self.task.fetch_current_list(limit=10)
        # print(f"当前委托:{list_res}")
        # for i in list_res:
        #     print(i['symbol'], i['id'], i['price'], i['side'], i['amount'])
        # [{'symbol': 'ADA-USDT', 'id': '2262864873063915520', 'clientOrderId': '', 'price': 0.7, 'stopPrice': None, 'triggerPrice': None, 'amount': 0.1, 'amountIsNum': True, 'side': 'buy', 'type': 'limit', 'status': 'open', 'leverage': 3.0, 'timeInForce': None, 'postOnly': None, 'reduceOnly': False, 'marginMode': 'isolated', 'average': None, 'filled': 0.0, 'cost': None, 'remaining': 0.1, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': {'feeCurrency': 'USDT', 'cost': 0.0}, 'timestamp': 1739941028466, 'info': {'accFillSz': '0', 'algoClOrdId': '', 'algoId': '', 'attachAlgoClOrdId': '', 'attachAlgoOrds': [], 'avgPx': '', 'cTime': '1739941028466', 'cancelSource': '', 'cancelSourceReason': '', 'category': 'normal', 'ccy': '', 'clOrdId': '', 'fee': '0', 'feeCcy': 'USDT', 'fillPx': '', 'fillSz': '0', 'fillTime': '', 'instId': 'ADA-USDT-SWAP', 'instType': 'SWAP', 'isTpLimit': 'false', 'lever': '3', 'linkedAlgoOrd': {'algoId': ''}, 'ordId': '2262864873063915520', 'ordType': 'limit', 'pnl': '0', 'posSide': 'net', 'px': '0.7', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'quickMgnType': '', 'rebate': '0', 'rebateCcy': 'USDT', 'reduceOnly': 'false', 'side': 'buy', 'slOrdPx': '', 'slTriggerPx': '', 'slTriggerPxType': '', 'source': '', 'state': 'live', 'stpId': '', 'stpMode': 'cancel_maker', 'sz': '0.1', 'tag': '', 'tdMode': 'isolated', 'tgtCcy': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'tradeId': '', 'uTime': '1739941028466'}}]

        # 报错
        # res = await self.task.fetch_leverage(symbol='ADA-USDT', marginMode='cross')
        # print(f"交易对杠杆:{res}")
        
        # exit()
        amount = 0.2
        price = 2695
        side = 'buy'
        symbol = 'ETH-USDT'
        #res = await self.task.create_order(symbol=symbol, order_type='limit', side=side, amount=amount, price=price, tdMode='cross')
        #print(f"下单回报:{res}")

        #res = await self.task.cancel_order(symbol='BCH-USDT', order_id='2289237874461892608')
        #print(f"撤单回报:{res}")

        #res = await self.task.cancel_order_batch(symbol='ETH-USDT', order_id=[order["id"] for order in list_res])
        #print(f"撤单:{res}")

        # res = await self.task.fetch_trade(symbol='LTC-USDT', limit=10)
        # # print(f"历史成交:{res}")
        # for i in res:
        #     date = datetime.datetime.fromtimestamp(int(int(i['info']['ts'])/1000))
        #     print(date, i['info'])

        # 查询单独订单
        text = ''
        buy_vol = 0
        buy_amt = 0
        sell_vol = 0
        sell_amt = 0
        fee = 0
        for i in [2584462282497712128,2584462382959681536,2584474564426457088,2584495501620273152,2584502447052152832,2584502849436901376,2584503252224303104]:
            res = await self.task.fetch_order_detail(symbol='BCH-USDT', order_id=i)
            fee += res['fee']['cost']
            ts = res['timestamp']
            utc_time = datetime.datetime.utcfromtimestamp(ts / 1000)
            cst_tz = pytz.timezone('Asia/Shanghai')
            cst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(cst_tz)
            date = cst_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
            if res['side'] == 'buy':
                buy_vol += res['amount']
                buy_amt += res['average']*res['amount']
            else:
                sell_vol += res['amount']
                sell_amt += res['average']*res['amount']
            text += (f"{date} {res['symbol']} price:{round(res['average'],2)} side:{res['side']} amount:{res['amount']} fee:{res['fee']['cost']}\n")
        print(text)
        print(buy_vol, buy_amt, sell_vol, sell_amt, fee)
        print(f"总盈亏:{sell_amt-buy_amt+fee}")

async def main():
    task = Strategy()
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())


