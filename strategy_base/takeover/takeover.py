
'''
    需求:用户触发穿仓后,平掉用户的仓位
    大概率,如果用户穿仓,做市商的头寸是赚钱的,也就是做市商头寸方向与穿仓用户头寸方向相反
    策略逻辑:
    有用户穿仓
        1、若mm仓位盈利,在市场上跟mm成交
        2、若mm仓位不盈利,通过adl跟其他盈利用户成交
    细节:
        1、穿仓在市场下单,用限价单一笔开仓,若超过单笔最大下单量,就拆分下单。限价单不撤单,mm策略补单时会吃掉,保证mm利润最大化
        2、无论穿仓用户持仓与mm是否一致,只要mm盈利就可以跟mm成交。成交后若触发adl,后续逻辑由adl策略处理
    问题:
        1、穿仓用户的信息有单独的接口吗?
        2、如何将用户持仓平掉?用adl接口?还是有其他接口可以处理用户持仓?

    需求更新:
    目的是穿仓+扎针
    对于我们不盈利的仓位,根本不管。不是完全不做处理,可以加方向判断。
    在下跌的时候,人工爆仓更安全合理
    若用户穿仓,mm可以盈利,就处理。否则不处理

    当前逻辑:
    只对维持保证金超过95%的A标签用户做takeover操作
    其余用户全部跳过

'''

import sys
import pytz
import traceback
import time
import datetime
import json
import requests
import asyncio
from loguru import logger
from datetime import datetime
timezone = pytz.timezone("Asia/Shanghai")
sys.path.append('../..')
from client.env_pro.rest.websea import contract


class takeOverStrategy():

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        self._load_config(config)   # 读取配置

    def _load_config(self, config):
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 交易所实例初始化
        self.token = config['tokon1']
        self.uid = config['uid1']
        self.rest18 = contract.WebseaContract(config['tokon1'],config['secret1'])
        self.rest19 = contract.WebseaContract(config['tokon2'],config['secret2'])
        self.rest20 = contract.WebseaContract(config['tokon3'],config['secret3'])
        self.rest21 = contract.WebseaContract(config['tokon4'],config['secret4'])
        self.rest22 = contract.WebseaContract(config['tokon5'],config['secret5'])

    async def init(self):
        # 获取交易对列表
        self.symbols = []
        data = await self.rest18.get_symbols()
        for s in data:
            self.symbols.append(s.symbol)

    ''' ==========================================================================='''
    '''==================================== message ==============================='''
    ''' ==========================================================================='''
    # 记录日志
    def writeLog(self, mess):
        logger.info(mess)

    async def warning(self, content, contractSymbol='', method='normal'):
        larkDic = {
            'normal': 'https://open.feishu.cn/open-apis/bot/v2/hook/bad3eefd-381c-409e-927d-4900ff58850a',
        }
        if method in larkDic:
            url = larkDic.get(method)
            date = datetime.fromtimestamp(int(time.time()), tz=timezone).strftime("%Y-%m-%d %H:%M:%S")
            content = f"==={date} 报警 {contractSymbol}===\n{content}\n"
            headers = {"Content-Type": "application/json ;charset=utf-8 "}
            msg = {"msg_type": "text",
                   "content": {"text": content}
                  }  # 飞书
            try:
                requests.post(url, headers=headers, data=json.dumps(msg))
            except:
                print(f'飞书报错 {traceback.format_exc()}')
            return

    ''' ==========================================================================='''
    ''' =============================== tool function ============================='''
    ''' ==========================================================================='''
    
    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    # @profile(precision=4, stream=open("memory.log", "w+"))    # 查看每行代码的内存消耗
    async def main(self):
        filter_list = []
        temp_filter_list = []
        data = await self.rest18.tag_list(token=self.takeover_token, tag = 'A', page_size=1000)  # , tag='B'
        tag_list_page = data['total_page']
        for id in data['tag_list']:
            temp_filter_list.append(id.user_id)
        for n in range(2, tag_list_page+1):
            data = await self.rest18.tag_list(token=self.takeover_token, page=n, page_size=1000)  #, tag='B'
            for id in data['tag_list']:
                temp_filter_list.append(id.user_id)
        for i in temp_filter_list:
            if int(i) not in filter_list:
                filter_list.append(int(i))
        
        for s in self.symbols:
            try:
                data = await self.rest18.adl_positionRiskRank(token=self.takeover_token, market=s, risk_filter='loss')
            except:
                mess = f"get_loss_user函数调用adl_positionRiskRank报错:{traceback.format_exc()}"
                print(mess)
                await self.warning(mess, f'takeover策略获取持仓队列报错')
                continue
            # 数据有疑问,同一个交易对,杠杆低的,反而<当前杠杆倍数允许的名义价值上限>更低。
            for user in data:
                id = user.UserID                    # id
                if id in filter_list:               # 白名单过滤
                    symbol = user.market                # 交易对
                    un_profit = user.unRealizedProfit   # 未实现盈亏
                    side = user.positionSide            # 持仓方向
                    mmr = user.maintMarginRatio         # 维持保证金比率
                    mmr = float(mmr.split('%')[0])
                    
                    # 必须同时满足浮亏一定金额+保证金率超过20%才考虑takeover
                    print(f"未实现盈亏:{un_profit} mmr:{mmr} 原始数据:{user}")
                    con = un_profit < 0 and mmr > self.risk_ratio
                    if con:
                        takeover_user = (symbol, side, id)
                        print(f"做市账户{self.uid},进行takeover操作,用户{id},symbol:{symbol},un_profit:{un_profit},mmr:{mmr},side:{side}")
                        await self.make_order(takeover_user, 100)
                    elif un_profit < -1500:
                        mess = f"亏损用户详细信息:{(id, symbol, side, un_profit, mmr)} 原始信息:{user}"
                        await self.warning(mess, '有大额亏损用户,注意takeover')
                        print(mess)
            time.sleep(1)
    
    async def make_order(self, takeover_user, adl_percent):
        symbol, side, id = takeover_user
        mess= (f"======下单参数======token:{self.takeover_token} symbol:{symbol} adlUserId:{id} mmUserId:{self.uid} side:{side} adlPercent:100")
        print(mess)
        try:
            await self.warning(mess, f'takeover策略执行减仓')
        except:
            print(f"发送飞书报错:{traceback.format_exc()}")
        # return
        try:
            for task in [(self.rest18, 18), (self.rest19, 19), (self.rest20, 20), 
                             (self.rest21, 21), (self.rest22, 22)]:
                rest, mm_uid = task
                print(f"adl下单参数:{mm_uid} {symbol} {id} {side} {adl_percent}")
                #data = await rest.adl_order(token=self.takeover_token, market=symbol, \
                #                            mmUserId=mm_uid, side=side, \
                #                            adlUserId=id, adlPercent=adl_percent, marginType='crossed')
                #print(f"adl下单回报:{data}")
                #if data in [True, '成功成交']:
                #    break
        except:
            mess = f"takeover策略减仓报错:{traceback.format_exc()}"
            print(mess)
            await self.warning(mess, f'takeover策略减仓报错')


async def main(path='takeover_config.py'):
    config = __import__(path.split(".py")[0])
    task = takeOverStrategy(config)
    await task.init()
    while 1:
        try:
            await task.main()  # 主循环
        except:
            try:
                task.writeLog(f'策略报错!\n{traceback.format_exc()}')
            except:
                pass
        time.sleep(120)  # TODO 根据adl接口数据更新频率决定sleep间隔

asyncio.run(main())

