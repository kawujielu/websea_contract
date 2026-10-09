
import sys
import time
import datetime
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
        self.task = okx.OkexContract(apiKey='edb91757-df52-4bd1-a04a-576b7aad0b84',secret='7E785C4C118E33832852DB85C5A55C4E',passphrase='265199_JJsy')

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

        # res = await self.task.fetch_precision('ADA-USDT')
        # # print(f"{res}")
        # for i in res:
        #     if i['symbol'] == 'ADA-USDT':
        #         price_precision = int(-np.log10(i['price']))    # 价格精度
        #         amount_precision = int(-np.log10(i['amount']))  # 数量精度
        #         face_value = i['faceValue']  # 合约面试,1张=多少币
        # print(f"交易对精度:{price_precision}, {amount_precision} 合约面值:{face_value}")  # 精度不对

        # res = await self.task.fetch_balance()
        # print(f"账户权益:{res}")
        # balance = res['USDT']['total']
        # avail = res['USDT']['free']
        # print(f"账户权益:{balance} 可用:{avail}")

        res = await self.task.fetch_position()
        print(f"账户持仓:{res}")
        return
        # [{'symbol': 'ADA-USDT', 'id': '2262921827685703680', 'contracts': -0.1, 'contractSize': None, 
        # 'side': 'net', 'entryPrice': 0.736, 'markPrice': 0.7423, 'notional': None, 'leverage': 3.0, 
        # 'unrealizedPnl': -0.0629999999999997, 'realizedPnl': -0.00368, 'marginMode': 'isolated', 
        # 'marginRatio': 46.00245055586562, 'initialMargin': 2.453333333333333, 'maintenanceMargin': None, 
        # 'stopLossPrice': None, 'takeProfitPrice': None, 'timestamp': 1739942725846, 
        # 'info': {'adl': '1', 'availPos': '', 'avgPx': '0.736', 'baseBal': '', 'baseBorrowed': '', 
        # 'baseInterest': '', 'bePx': '0.735264367816092', 'bizRefId': '', 'bizRefType': '', 
        # 'cTime': '1739942725846', 'ccy': 'USDT', 'clSpotInUseAmt': '', 'closeOrderAlgo': [], 
        # 'deltaBS': '', 'deltaPA': '', 'fee': '-0.00368', 'fundingFee': '0', 'gammaBS': '', 
        # 'gammaPA': '', 'idxPx': '0.7426000000000000', 'imr': '', 'instId': 'ADA-USDT-SWAP', 
        # 'instType': 'SWAP', 'interest': '', 'last': '0.7423', 'lever': '3', 'liab': '', 'liabCcy': '', 
        # 'liqPenalty': '0', 'liqPx': '0.9744117510758028', 'margin': '2.4533333333333333', 
        # 'markPx': '0.7423', 'maxSpotInUseAmt': '', 'mgnMode': 'isolated', 'mgnRatio': '46.00245055586562', 
        # 'mmr': '0.0482495', 'notionalUsd': '7.4192885', 'optVal': '', 'pendingCloseOrdLiabVal': '', 
        # 'pnl': '0', 'pos': '-0.1', 'posCcy': '', 'posId': '2262921827685703680', 'posSide': 'net', 
        # 'quoteBal': '', 'quoteBorrowed': '', 'quoteInterest': '', 'realizedPnl': '-0.00368', 
        # 'spotInUseAmt': '', 'spotInUseCcy': '', 'thetaBS': '', 'thetaPA': '', 'tradeId': '194756885', 
        # 'uTime': '1739942725846', 'upl': '-0.0629999999999997', 'uplLastPx': '-0.0629999999999997', 
        # 'uplRatio': '-0.0256793478260868', 'uplRatioLastPx': '-0.0256793478260868', 'usdPx': '0.9995', 
        # 'vegaBS': '', 'vegaPA': ''}}]

        list_res = await self.task.fetch_current_list(limit=10)
        print(f"当前委托:{list_res}")
        for i in list_res:
            print(i['symbol'], i['id'], i['price'], i['side'], i['amount'])
        # [{'symbol': 'ADA-USDT', 'id': '2262864873063915520', 'clientOrderId': '', 'price': 0.7, 'stopPrice': None, 'triggerPrice': None, 'amount': 0.1, 'amountIsNum': True, 'side': 'buy', 'type': 'limit', 'status': 'open', 'leverage': 3.0, 'timeInForce': None, 'postOnly': None, 'reduceOnly': False, 'marginMode': 'isolated', 'average': None, 'filled': 0.0, 'cost': None, 'remaining': 0.1, 'takeProfitPrice': None, 'stopLossPrice': None, 'fee': {'feeCurrency': 'USDT', 'cost': 0.0}, 'timestamp': 1739941028466, 'info': {'accFillSz': '0', 'algoClOrdId': '', 'algoId': '', 'attachAlgoClOrdId': '', 'attachAlgoOrds': [], 'avgPx': '', 'cTime': '1739941028466', 'cancelSource': '', 'cancelSourceReason': '', 'category': 'normal', 'ccy': '', 'clOrdId': '', 'fee': '0', 'feeCcy': 'USDT', 'fillPx': '', 'fillSz': '0', 'fillTime': '', 'instId': 'ADA-USDT-SWAP', 'instType': 'SWAP', 'isTpLimit': 'false', 'lever': '3', 'linkedAlgoOrd': {'algoId': ''}, 'ordId': '2262864873063915520', 'ordType': 'limit', 'pnl': '0', 'posSide': 'net', 'px': '0.7', 'pxType': '', 'pxUsd': '', 'pxVol': '', 'quickMgnType': '', 'rebate': '0', 'rebateCcy': 'USDT', 'reduceOnly': 'false', 'side': 'buy', 'slOrdPx': '', 'slTriggerPx': '', 'slTriggerPxType': '', 'source': '', 'state': 'live', 'stpId': '', 'stpMode': 'cancel_maker', 'sz': '0.1', 'tag': '', 'tdMode': 'isolated', 'tgtCcy': '', 'tpOrdPx': '', 'tpTriggerPx': '', 'tpTriggerPxType': '', 'tradeId': '', 'uTime': '1739941028466'}}]

        # 报错
        # res = await self.task.fetch_leverage(symbol='ADA-USDT', marginMode='cross')
        # print(f"交易对杠杆:{res}")

        amount = 0.2
        price = 1.1
        side = 'sell'
        #res = await self.task.create_order(symbol='ADA-USDT', order_type='limit', side=side, amount=amount, price=price, tdMode='cross')
        #print(f"下单回报:{res}")

        #res = await self.task.cancel_order(symbol='BCH-USDT', order_id='2289237874461892608')
        #print(f"撤单回报:{res}")

        #res = await self.task.cancel_order_batch(symbol='ADA-USDT', order_id=[order["id"] for order in list_res])
        #print(f"撤单:{res}")

        # res = await self.task.fetch_trade(symbol='LTC-USDT', limit=10)
        # # print(f"历史成交:{res}")
        # for i in res:
        #     date = datetime.datetime.fromtimestamp(int(int(i['info']['ts'])/1000))
        #     print(date, i['info'])

        #res = await self.task.fetch_order_detail(symbol='ADA-USDT', order_id=res['id'])
        #print(f"订单详情:{res}")


async def main():
    task = Strategy()
    try:
        await task.main()  # 主循环
    except:
        print(f'策略报错!\n{traceback.format_exc()}')

asyncio.run(main())


