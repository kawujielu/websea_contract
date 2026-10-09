
import sys
import time
import traceback
sys.path.append('/usr/local/server')
from wbfAPI.exchange import binanceUsdtSwap as bn
#sys.path.append('/home/ubuntu/crypto_center/client/rest/okex')
#import contract as okx

class Strategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        self._load_config()  # 读取配置
        self.num = 0

    def _load_config(self):
        """读取配置文件
        """
        # 2个binance账户都没钱了
        self.binance_rest = bn.AccountRest('AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi','FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj')
        #self.binance_rest = bn.AccountRest('1oZivN3rwdcPSIGMJiwVNKTABw1tLmLK6GnSlhK40WIYLmqRk27vOaV0pIa2nICn','7EZlBSu2l7vdyAldI06xf33VQJrNSk0hRyJ714tJE4CGFkWxLmhMhfu47AqeXTGX')

    def main(self):
        #print(self.binance_rest.getSymbolConfig('bch/usdt'))    # 交易对设置

        #print(self.binance_rest.getFee('bch/usdt'))   
        cost = 299.42
        profit = 0
        num = 0
        #res = self.binance_rest.getDeals('bch/usdt', count=1000)
        #for i in res['data']:
        #    if i['side'] == 'sell' and i['ts'] > 1741143600000:
        #        num += 1
        #        print(i)
        #        print((i['price']-cost)*i['vol']-float(i['fee']))
        #        profit += (i['price']-cost)*i['vol']-float(i['fee'])
        #print(f"{num}笔交易,盈利:{profit}")
        
        # print(self.binance_rest.updateLeverage('bch/usdt'))   # 设置杠杆

        # data = self.binance_rest.getHisTrans(False)  # 查询持仓模式
        # print(data)
        # exit()

        #for i in range(5):
        #    data = self.binance_rest.makeOrder('bch/usdt', vol=5, price=300, orderType='sell-limit') #, positionSide='LONG')
        #    print(data)
        #    time.sleep(2)
        #data = self.binance_rest.cancelAll('bch/usdt')
        #print(data)

        data = self.binance_rest.getBalance()
        print(f"合约账户权益:{data}")
        # 多单开了40个之后
        # {'symbol': 'usdt', 'balance': 199.99307, 'available': 199.31147, 'frozen': 0.6815999999999747, 'initMargin': None}
        # {'symbol': 'usdt', 'balance': 199.986126, 'available': 198.61362776, 'frozen': 1.3724982399999988, 'initMargin': None}

        data = self.binance_rest.getPosition('all')
        print(f"合约持仓:{data}")
        # exit()
        # {'symbol': 'ada/usdt', 'pos': 40.0, 'posSide': 1, 'openPrice': 0.3465, 'openAmt': 13.86, 
        #  'holdPrice': None, 'liquidationPrice': 0.0, 'unrealProfitLoss': 0.024, 'closeProfitLoss': None, 
        #  'lever': 20.0, 'maxNotionalValue': 1000000}
        #exit()
        #data = self.binance_rest.makeOrder('bch/usdt', vol=22, price=270, orderType='sell-limit', offset='close') #, positionSide='LONG')
        #print(data)
        exit()
        
        while self.num < 33:
            depth = self.binance_rest.getDepth('bch/usdt')
            bid_one = depth['data'][1][0][0]
            ask_one = depth['data'][2][0][0]
            print(bid_one)
            if bid_one > 270:
                data = self.binance_rest.makeOrder('bch/usdt', vol=2, price=270, orderType='sell-limit', offset='close') #, positionSide='LONG')
                print(data)
                self.num += 1
            time.sleep(1)
        
        # {'symbol': 'ada/usdt', 'pos': 40.0, 'posSide': 1, 'openPrice': 0.3465, 'openAmt': 13.86, 
        #  'holdPrice': None, 'liquidationPrice': 416.69021, 'unrealProfitLoss': 0.0349984, 'closeProfitLoss': None, 
        #  'lever': 20.0, 'maxNotionalValue': 1000000}, 
        # {'symbol': 'ada/usdt', 'pos': -40.0, 'posSide': -1, 'openPrice': 0.3472, 'openAmt': -13.888, 
        #  'holdPrice': None, 'liquidationPrice': 416.69021, 'unrealProfitLoss': -0.0069984, 'closeProfitLoss': None, 
        #  'lever': 20.0, 'maxNotionalValue': 1000000}
        # exit()

        # data = self.binance_rest.getSymbolConfig()
        # print(f"合约交易对配置:{data}")

        # 双向下单规则
        # 开多规则为:side=buy, positionSide=LONG;
        # 开空规则为:side=sell,positionSide=SHORT;
        # 平多规则为:side=sell,positionSide=LONG; 
        # 平空规则为:side=buy, positionSide=SHORT;
        # for i in range(148):
        #     data = self.binance_rest.makeOrder('bch/usdt', vol=1, price=315, orderType='buy-limit', positionSide='LONG')
        #     print(data)
        #     time.sleep(0.1)

        # {'orderId': 44821317381, 'symbol': 'ADAUSDT', 'status': 'NEW', 'clientOrderId': 'dlxBMLRCzJrfda4hJOcVR5', 
        #  'price': '0.34700', 'avgPrice': '0.00', 'origQty': '40', 'executedQty': '0', 'cumQty': '0', 
        #  'cumQuote': '0.00000', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': False, 
        #  'closePosition': False, 'side': 'BUY', 'positionSide': 'LONG', 'stopPrice': '0.00000', 
        #  'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'priceMatch': 'NONE', 
        #  'selfTradePreventionMode': 'NONE', 'goodTillDate': 0, 'updateTime': 1728837010851}


if __name__ == '__main__':
    task = Strategy()
    task.main()
