import sys
import asyncio
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
from utils import restclient as rc
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.rest.binance import u_contract as bn_rest
from template.template_timer import TemplateTimer, CronTrigger
# from typing import Optional, Dict, List
# from utils.aio_redis import MyAioredis, MyAioredisFunctools
from crypto_center.client.rest.okex import contract as okx_rest



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        # self.redis_pool: Optional[MyAioredis] = None
        # self.redis_conn: Optional[MyAioredisFunctools] = None
        self.rc_task = rc.RestClient()
        self.rest = ws_contract_rest('84ad0e771f0f5f2df264e56d0ecddef9', '9cvay7ieczqp01lwy0zr')
        self.bn_rest = bn_rest.UBinanceContract('ZlbbTVrOSfCUBtj6b6eNTCiTNjsojoxMRaDGIEZjORCRTm2dSax7sdwi9PgyMvGu','M62FUBt8Prey2Q2B0YszyB2Ms9nXb5hd5o01sF8LEablSG1qtVev7bjJXkbT0fSu')
        self.ok_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        self.symbols = []
        self.raito_limit = 0.01
        # self.db = 1
        
    
    async def on_first(self):
        self.log.info("=======first======")
        data = await self.rest.get_symbols(quan=True)
        # self.log.info(f"合约单位:{data}")
        for i in data:
            self.symbols.append(i.symbol)
        print(f"订阅长度:{len(self.symbols)}")
    
    async def on_timer(self):
        # self.log.info("=======timer======")
        self.schedule.add_job(self.check_price, CronTrigger(minute="*/5"))  # 每1s执行一次
        # self.schedule.add_job(self.check_price, CronTrigger(second="*/5"))
    
    async def check_price(self):
        for s in self.symbols:
            depth = await self.rest.get_depth(s, limit=5, quan=True)
            if depth is None or depth.bids == [] or depth.asks == []:
                continue
            try:
                bn_depth = await self.bn_rest.get_depth(s, 10)
                # {'code': -1121, 'msg': 'Invalid symbol.'}
                if 'msg' in bn_depth and bn_depth['msg'] == 'Invalid symbol.':
                    self.log.info(f"币安没有{s}交易对")
                    bn_depth = 0
            except:
                self.log.info(f"币安没有{s}交易对")
                bn_depth = 0
            try:
                ok_depth = await self.ok_rest.fetch_depth(s, 2)
                if 'result' in ok_depth:
                    ok_depth = 0
            except:
                self.log.info(f"OK没有{s}交易对")
                ok_depth = 0
            if bn_depth == 0 and ok_depth == 0:
                self.log.info(f"币安和OK都没有{s}交易对")
                continue
            elif bn_depth == 0:
                bid_diff_bn, ask_diff_bn = 0, 0
                bid_diff_ok = abs(depth.bids[0].price/float(ok_depth['bids'][0][0])-1)
                ask_diff_ok = abs(depth.asks[0].price/float(ok_depth['asks'][0][0])-1)
            elif ok_depth == 0:
                bid_diff_bn = abs(depth.bids[0].price/float(bn_depth['bids'][0][0])-1)
                ask_diff_bn = abs(depth.asks[0].price/float(bn_depth['asks'][0][0])-1)
                bid_diff_ok, ask_diff_ok = 0, 0
            else:
                bid_diff_bn = abs(depth.bids[0].price/float(bn_depth['bids'][0][0])-1)
                bid_diff_ok = abs(depth.bids[0].price/float(ok_depth['bids'][0][0])-1)
                ask_diff_bn = abs(depth.asks[0].price/float(bn_depth['asks'][0][0])-1)
                ask_diff_ok = abs(depth.asks[0].price/float(ok_depth['asks'][0][0])-1)
            self.log.info(f"全部价差:{bid_diff_bn}, {bid_diff_ok}, {ask_diff_bn}, {ask_diff_ok}")
            con1 = bid_diff_bn > self.raito_limit and bid_diff_ok > self.raito_limit
            con2 = ask_diff_bn > self.raito_limit and ask_diff_ok > self.raito_limit
            if con1 or con2:
                mess = f"websea合约{s} 单边价差达到{round(max(bid_diff_bn, bid_diff_ok, ask_diff_bn, ask_diff_ok)*100, 4)}%, 立即检查做市策略"
                self.log.info(f"报警:{mess}")
                # await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1001898787256, content=mess)   # 价差群编号
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1001973998142, content=mess)     # 紧急报警群编号
            await asyncio.sleep(0.2)


def main():
    strategy().run()


if __name__ == '__main__':
    main()

