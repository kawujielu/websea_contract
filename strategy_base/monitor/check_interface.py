'''
    合约+现货所有接口测试
'''
import sys
import asyncio
from pathlib import Path
sys.path.append(Path(__file__).resolve().parent.parent.parent.as_posix())
import pytz
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_quan import WebseaContract     # 新接口做下单
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
from client.env_pro.rest.binance.u_contract import UBinanceContract as bn_rest
from client.env_pro.wss.binance.u_contract import UBinanceContract as bn_wss

timezone = pytz.timezone("Asia/Shanghai")


class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()  # 读取配置
        self._initParams()         # 初始化参数
    
    def _load_config(self):
        """读取配置文件
        """
        # self.bn_rest = bn_rest(config['hedge_token'], config['hedge_secret'])
        self.rest = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')
        self.rest1 = WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')

    def _initParams(self):
        """初始化参数
        """
        pass
    
    async def on_first(self):
        self.log.info("=======first======")
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
    
    async def main(self):
        symbol = "BTC-USDT"
        res = await self.rest1.fetch_depth(symbol)
        print(f"fetch_depth接口:{res}")
        await asyncio.sleep(1)
        res = await self.rest1.fetch_kline(symbol, "1m", limit=5)
        print(f"fetch_kline接口:{res}")
        await asyncio.sleep(1)
        res = await self.rest1.fetch_markprice(symbol)
        print(f"fetch_markprice接口:{res}")
        await asyncio.sleep(1)
        res = await self.rest1.fetch_position()
        print(f"pos接口:{res}")
        await asyncio.sleep(1)
        ret = await self.rest1.fetch_precision(symbol)
        print(f"precision接口:{res}")


def main():
    strategy().run()


if __name__ == '__main__':
    main()


