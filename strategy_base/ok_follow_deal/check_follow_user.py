"""
检查 OKX 带单员是否设置了仓位保密。

遍历预设的带单员列表，调用 OKX 接口查询其持仓；
若返回 symbol 为空，视为已保密，否则视为开放。
最终打印开放交易员与保密交易员名单，供跟单策略筛选使用。
"""
import sys
import time
import datetime
import traceback
import mysql.connector
import asyncio
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
#from crypto_center.client.rest.websea.contract_quan import WebseaContract as ws_contract_rest

from crypto_center.client.rest.okex import contract as okx_rest
import objects.contract_request.websea as ocw

strategy_name = "copy OKX的跟单策略"


class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        """初始化策略：创建事件循环并加载配置。"""
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
                
    def _load_config(self, config):
        """读取配置文件，初始化 Websea / OKX REST 客户端。"""
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        #self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        # self.ws_rest = ws_contract_rest('c05f95b1c43bde42556b6e34e1y67448667','fikpc1jlgqb05b3yq9r9')
        #self.ws_rest = ws_contract_rest('7287274648396dc3f797c553ecd55748600','fwz3tuyik4nvs60v7i8v')
        #self.ws_rest = ws_contract_rest('9294e3098ff151a5aa854926b6h57978648','ye31wgeixol2wumb6iuv')
        #self.ws_rest = ws_contract_rest('19f991d6d7cdaac1083264c933j59094046','c4029mguoouis2ph0yox')
        #self.ws_rest = ws_contract_rest('fcd8cd9ea07b0a7f0c85b12fa3c55191807','ocpvq2nh50mzr2krzm5i')
        #self.ws_rest = ws_contract_rest('23cb167617bb8398ccfcf74506t64669304','9mowefaye9nhyxf2w7zq')
        #self.ws_rest = ws_contract_rest('53dab96a47bd1021d9dcef3d9fk59654212','p56gp29wyz5dybrbu430')
        self.ws_rest = ws_contract_rest('4905669b7f070117807133b5e3j59096908','jyulvaphe2hnrsbvatj1')
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        self.ws_rest.DEBUG = False
        self.okx_rest.DEBUG = False

    async def on_first(self):
        """策略首次启动入口，执行带单员保密状态检查。"""
        await self.hedge()        # 内盘合约单位

    async def on_timer(self):
        """定时器回调（当前仅打日志，无业务逻辑）。"""
        self.log.info("=======timer======")
    
    async def connect_db(self):
        """连接合约 MySQL 数据库并创建 cursor。"""
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_db",
            port=3306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
    async def hedge(self):
        """
        检查带单员是否设置仓位保密。

        对预设 traders 列表逐个查询 OKX 持仓：
        - symbol 为空 → 保密，归入 pop_trades
        - 否则 → 开放，归入 new_traders
        最后打印开放/保密名单及数量。
        """
        # 验证带单员是否设置保密
        traders = [['904F7F81492EB183', 'luoxiaohei'], ['185F7C91DCE1B23A', 'CrazyBuIl'], ['5F9177A0DD1D162E', 'Flaky-AirGap-Pea'], ['2D213ABBFCFB9FF1', 'Griffith-King'], ['559F336686BED416', 'Mumu(<d83c><df40>,<d83c><df40>)'], ['FF48C5939FE6119F', 'speculation emperor'], ['A0CFA7455CF0B4EE', 'danaofu'], ['D87783DB77C30A45', 'Amazing-Fund-Aronia'], ['E722637B1609A4F3', 'Temptat'], ['28E7D9C5B7040D7C', '川普炒币'], ['F4AB78B3D3BDAC61', 'www***'], ['B6B66F0F8BE47217', 'moderation'], ['59B1FE41104E97E6', 'Godori'], ['9B03EB5EDF68F727', '币圈十五年'], ['B156E249E8F39FBD', 'Dragon_King'], ['DF5B4593C1E7A0CB', 'caesar1'], ['958BC6B1C2D699C2', '交易员刘一手'], ['96F4C7FA4260C891', 'A MD, PhD Trader '], ['6801DAE15A7740BF', 'Langshen'], ['2BE980C9BEA40361', 'ChineseDing'], ['6A58A37D13955D25', 'Fortune Freedom 888'], ['87FAA12C94CF9E11', 'Rubis'], ['DF4FD1CAF2E9E23A', 'Jack Musk '], ['8F5A3438131106A2', 'Shoot out'], ['3C0A650E43C9F05F', 'Effy-zhuang'], ['C84F6F6717746BD4', 'BestMax'], ['CB2096451224C6E7', '稳住别慌，还能来'], ['C17F6C19D48D4E24', 'Unstoppable Us'], ['6660A04A8B3182D7', 'Air force-one'], ['37C31C5A06944914', 'Mad-Trilemma-Car'], ['C6C5130B473B00C2', 'OKZero'], ['B47938B726372A7D', '神仙也该死'], ['72EA983CBF575401', '134***3536'], ['BFF0B05FDCC878AA', 'X:shubuda_BTC'], ['2C6B8A645872B919', 'Knifepoint dancer'], ['B70B9F8B82B179E1', 'ahxin'], ['EBDC652139829D19', '千千万万场雨'], ['D99506B381B10AE4', 'haikuxing'], ['0A8E45C8FA13527D', 'JYJSDeng'], ['6393B350B3F00D7A', 'Callmelaoxu'], ['D8F249DC07B94985', 'Turn-Over'], ['1F1AC261729ED153', '青山顶呱呱'], ['A24E06C746B022B4', 'RealCryptoFox'], ['00CF26477A4A5B40', 'zputishuxia'], ['9C2CB3B2306B28EA', 'Hungry-Premium-Grass']]
        pop_trades = []
        new_traders = []
        for id in traders:
            while True:
                try:
                    res = await self.okx_rest.fetch_trader_pos(id[0])
                    # self.log.info(f"{id[0]} {id[1]}用户持仓:{res}")
                    if isinstance(res, list):
                        break
                    # self.log.info(f"查询fetch_trader_pos报错:{res}")
                except:
                    self.log.warning(f"获取{id[0]}持仓失败, {traceback.print_exc()},重试中...")
                await asyncio.sleep(2)
            for i in res:
                if i['symbol'] == '':
                    if id not in pop_trades:
                        pop_trades.append(id)
                    continue
            if id not in pop_trades:
                new_traders.append(id)
            await asyncio.sleep(0.5)
        
        print(f"开放交易员:{new_traders} {len(new_traders)}")
        print(f"保密交易员:{pop_trades}")

def main(path='ok_follow_deal_config.py'):
    """加载配置并启动策略。"""
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()

