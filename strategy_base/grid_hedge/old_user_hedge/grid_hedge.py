'''
    策略逻辑：
    根据标记用户或指定用户的持仓成本
    在成本之下做对冲
    根据算法:
    mmr越高,越早对冲;mmr越高,越晚对冲;

    2025-04-21 update:
    1、将rest查询全量用户持仓,修改为wss推送
    2、定时用rest检查本地维护的用户持仓是否正确
'''

import sys
import time
import traceback
import asyncio
import importlib
import numpy as np
sys.path.append('../..')
from utils import Toolbox as tb
from utils import restclient as rc
from template.template_timer import TemplateTimer, CronTrigger
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from client.env_pro.wss.websea.contract import WebSeaContract as ws_contract_wss
import objects.contract_request.binance as ocb

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
        self._initParams()    # 初始化参数
        
        # 订阅base数据
        self.ws_wss = ws_contract_wss()
        self.ws_wss.on_adl = self.on_adl
        self.loop.create_task(self.ws_wss.only_subscribe())
        self.loop.create_task(self.ws_wss.sub_adl(subtag=self.tag))
                
    def _load_config(self, config):
        """读取配置文件
        """
        self.init_config = config
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.rc_task = rc.RestClient()
        self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])

    def _initParams(self):
        """初始化参数
        """
        self.symbol_size = {}       # 内盘合约单位
        self.symbol_precision = {}  # 交易对精度+面值
        self.hedge_pos = {}         # 已对冲信息
        self.base_pos = {}          # 内盘持仓
        self.last_base_pos = {}     # 上一次内盘持仓
        self.hedge_pos_risk = {}    # 对冲持仓
        self.hold_list = []         # 标签用户持仓数据
        self.diff_num = 0           # 仓位不一致的次数
        self.send_tg_ts = 0         # 发送tg时间戳
    
    async def on_first(self):
        self.log.add(f"log/grid_hedge_log.log", rotation="100 MB", retention=10)
        await self.get_symbol_unit()        # 内盘合约单位
        await self.hedge_contract_info()    # 更新币对信息
        await self.cancel_orders()          # 取消所有委托
        await self.risk(True)                   # 判断是否对冲了计划内的仓位

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(second="*/10"))  # 每分钟执行一次="*"
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))
        self.schedule.add_job(self.reload_config, CronTrigger(minute="*"))  # 每分钟的0s执行
    
    async def reload_config(self):
        try:
            importlib.reload(self.init_config)
            [setattr(self, k, v) for k, v in vars(self.init_config).items()]
        except:
            try:  # 异常处理
                self.log.warning(f"reload_config报错{traceback.format_exc()}")
            except:
                pass
    
    ''' ==========================================================================='''
    ''' ==================================== wss =================================='''
    ''' ==========================================================================='''
    
    async def on_ticker_order(self, content):
        # print(f"外盘一档数据:{content}")
        symbol = content['symbol']
        bid = content['bid_price']
        ask = content['ask_price']
        bid_vol = content['bid_qty']
        ask_vol = content['ask_qty']
        self.symbols_bid_ask[symbol] = [[bid, bid_vol], [ask, ask_vol]]
        # print(f"外盘一档价格:{self.symbols_bid_ask}")

    # 内盘wss成交推送
    async def on_adl(self, content):
        self.log.info(f"ws成交推送数据:{content}")
        if content['tag'] == self.tag and \
            content['status'] in ['PARTIALLY_FILLED', 'FILLED']:
            await self.update_tag_user_pos(dict(content))

    # 将wss推送数据转换成hold_list查询到的用户持仓数据格式
    async def update_tag_user_pos(self, content):
        # 推送数据
        {'lastfilledVolume': 2.97, 'orderType': 'limit', 'leverage': 49, 'lastfilledSize': 0.01, 
         'isolatedMargin': '0.0606', 'liquidationPrice': '435085.92', 'cumfilledSize': 0.01, 'uid': 100022, 
         'positionAmt': '-0.01', 'markPrice': '296.80', 'price': '296.96', 'tag': 'K', 'direction': 'SPACE', 
         'amount': 0.01, 'side': 'SELL', 'origQty': 0.01, 'positionSide': 'SHORT', 
         'updateTime': 1740552793258, 'userId': 76332266, 'market': 'BCH-USDT', 'entryPrice': '297.01', 
         'cumfilledVol': '2.97', 'isAutoAddMargin': 'false', 'unRealizedProfit': '0.0021', 
         'marginType': 'full', 'lastfilledprice': '297.01', 'status': 'FILLED'}
        # 保存的数据格式
        {'amount': '4', 'avgPrice': '2526.42', 'freeze_amount': '0', 'in_position': '1.22', 'isFull': 1, 
         'is_full': 1, 'mark_price': '2526.27', 'multiple': 100, 'openDirection': 1, 'open_time': 1747726735, 
         'parity': '1707.34', 'profitLoss': '-0.0060', 'profit_loss': '-0.0060', 'risk_ratio': '1.22', 
         'symbol': 'ETH-USDT', 'tag': '', 'time': 1747726735, 'user_id': '485111', 'user_name': '485111', 
         'zhqy': '33.0301'}
        
        symbol = content['market']
        side = 1 if content['positionAmt'][0] == '+' else -1
        is_full = 2 if content['marginType'] == 'full' else 1
        # None表示推送数据中没有相关内容
        temp_pos = {'amount': str(content['amount']/self.symbol_size[symbol]), 'avgPrice': content['entryPrice'], 'freeze_amount': None, 
                    'in_position': None, 'isFull': is_full, 'is_full': is_full, 'mark_price': content['markPrice'],
                    'multiple': content['leverage'], 'openDirection': side, 'open_time': content['updateTime'], 
                    'parity': content['liquidationPrice'], 'profitLoss': content['unRealizedProfit'], 
                    'profit_loss': content['unRealizedProfit'], 'risk_ratio': None, 'symbol': content['market'], 
                    'tag': content['tag'], 'time': content['updateTime'], 'user_id': str(content['uid']), 
                    'user_name': str(content['uid']), 'zhqy': None}
        update_tag = False
        for i in self.hold_list:
            if temp_pos['symbol'] == i['symbol'] and \
                temp_pos['user_id'] == i['user_id'] and \
                temp_pos['isFull'] == i['isFull']:
                update_tag = True
                index = self.hold_list.index(i)
                break
        if update_tag:
            self.hold_list[index] = temp_pos
            self.log.info(f"更新持仓:{temp_pos}")
        else:
            self.hold_list.append(temp_pos)
            self.log.info(f"新增持仓:{temp_pos}")
    
    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                # 获取所有交易对
                res = await self.ws_rest.get_symbols(quan=True)
                for s in res:
                    self.symbol_size[s.symbol] = s.contract_size
                self.log.info(f"内盘合约单位:{self.symbol_size}")
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    # rest获取对冲端交易对详情
    async def hedge_contract_info(self):
        res = await self.okx_rest.fetch_precision()
        for s, v in res.items():
            price_precision = int(-np.log10(v['price']))    # 价格精度
            amount_precision = int(-np.log10(v['amount']))  # 数量精度
            face_value = v['faceValue']  # 合约面试,1张=多少币
            hedge_vol_limit = v['limit']['amount']['min']
            self.symbol_precision[s] = (price_precision, amount_precision, face_value, hedge_vol_limit)
        self.log.info(f"交易对精度+面值:{self.symbol_precision}")
    
    async def cancel_orders(self, symbol=None, ids=None):
        cancel_symbols = {}
        # 开盘撤单
        if symbol is None:
            res = await self.okx_rest.fetch_current_list(limit=100, state='live')
            for s in res:
                if s['symbol'] in cancel_symbols:
                    cancel_symbols[s['symbol']].append(s['id'])
                else:
                    cancel_symbols[s['symbol']] = [s['id']]
            for s, ids in cancel_symbols.items():
                res = await self.okx_rest.cancel_order_batch(s, order_id=ids[:20])
                self.log.info(f"取消所有委托:{res}")
        # 用户平仓后,撤单委托
        else:
            res = await self.okx_rest.cancel_order_batch(symbol, order_id=ids)
            self.log.info(f"取消指定委托:{res}")
    
    # 对冲仓位平仓
    async def hedge_pos_close(self, symbol):
        hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
        for v in hedge_pos:
            if v['symbol'] == symbol and float(v['contracts']) != 0:
                side = 'buy' if float(v['contracts']) < 0 else'sell'
                depth = await self.okx_rest.fetch_depth(symbol=symbol, size=1)
                price = depth['asks'][0][0]*(1+self.price_slip) if side == 'buy' else depth['bids'][0][0]*(1-self.price_slip)
                vol = abs(float(v['contracts']))
                result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                    side=side, amount=abs(vol), 
                                                    price=price, tdMode='cross')
                self.log.info(f"平仓结果:{result}")

    async def update_pos_data(self, hold_list):
        while True:
            try:
                # 内盘持仓
                # hold_list = await self.get_hold_list()  # 获取当前所有用户持仓
                self.base_pos, _ = await self.get_hedge_dict(hold_list)   # 按筛选条件获取持仓数据
                # 外盘委托
                res = await self.okx_rest.fetch_current_list(limit=100, state='live')
                for i in res:
                    # self.log.info(f"外盘委托挂单:{i}")
                    side = 1 if i['side'] == 'buy' else -1
                    if i['symbol'] not in self.hedge_pos:
                        self.hedge_pos[i['symbol']] = i['amount']*side/self.hedge_pos_ratio
                    else:
                        self.hedge_pos[i['symbol']] += i['amount']*side/self.hedge_pos_ratio
                    # print(f"{i['symbol']} {i['price']} {i['amount']} {i['side']} {i['status']} {i['id']} {i['leverage']}")
                # 外盘持仓
                hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
                for v in hedge_pos:
                    if float(v['contracts']) != 0:
                        if v['symbol'] not in self.hedge_pos:
                            self.hedge_pos[v['symbol']] = float(v['contracts'])/self.hedge_pos_ratio    # contracts表示币的数量,net持仓模式下,自带正负号
                        else:
                            self.hedge_pos[v['symbol']] += float(v['contracts'])/self.hedge_pos_ratio
                # 按照字典的value大小排序
                self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
                self.hedge_pos = dict(sorted(self.hedge_pos.items(), key=lambda item: item[1], reverse=True))
                self.log.info(f"\n初始内盘持仓:{self.base_pos}\n初始外盘持仓:{self.hedge_pos}")
                break
            except:
                self.log.warning(f"update_pos_data函数报错:{traceback.format_exc()}")
            await asyncio.sleep(3)
        
    def send_msg(self, text):
        try:
            tb.warning(f"{strategy_name}{self.symbol}合约OK持仓异常:{text}",'risk')
            tb.sendmail(f'{self.symbol}合约持仓异常', text)
            self.log.info(text)
        except:
            self.log.warning(f"send_msg函数报错:{traceback.format_exc()}")
    
    ''' ==========================================================================='''
    ''' ===================================== risk ================================'''
    ''' ==========================================================================='''

    async def risk(self, first_tag=False):
        self.log.info(f"风控线程")
        try:
            # 内盘持仓
            hold_list = await self.get_hold_list()  # 获取当前所有用户持仓
            if first_tag:
                print("第一次执行")
                await self.update_pos_data(hold_list)
            self.hold_list = hold_list
            self.base_pos, _ = await self.get_hedge_dict(hold_list)   # 按筛选条件获取持仓数据
            # 外盘委托
            hedge_pos_risk = {}
            res = await self.okx_rest.fetch_current_list(limit=100, state='live')
            for i in res:
                # self.log.info(f"外盘委托挂单:{i}")
                side = 1 if i['side'] == 'buy' else -1
                if i['symbol'] not in hedge_pos_risk:
                    hedge_pos_risk[i['symbol']] = i['amount']*side/self.hedge_pos_ratio
                else:
                    hedge_pos_risk[i['symbol']] += i['amount']*side/self.hedge_pos_ratio
                # print(f"{i['symbol']} {i['price']} {i['amount']} {i['side']} {i['status']} {i['id']} {i['leverage']}")
            # 外盘持仓
            text = f"{strategy_name} {self.id}用户持仓:\n"
            sum_profit = 0
            hedge_pos = await self.okx_rest.fetch_position()  # 仓位带正负
            for v in hedge_pos:
                if float(v['contracts']) != 0:
                    sum_profit += float(v['unrealizedPnl'])
                    text += f"{v['symbol']} 持仓:{v['contracts']} 浮动盈亏:{v['unrealizedPnl']} 成本:{v['entryPrice']} 当前价格:{v['markPrice']}\n"
                    if v['symbol'] not in hedge_pos_risk:
                        hedge_pos_risk[v['symbol']] = float(v['contracts'])/self.hedge_pos_ratio    # contracts表示币的数量,net持仓模式下,自带正负号
                    else:
                        hedge_pos_risk[v['symbol']] += float(v['contracts'])/self.hedge_pos_ratio
            # 按照字典的value大小排序
            self.base_pos = dict(sorted(self.base_pos.items(), key=lambda item: item[1], reverse=True))
            hedge_pos_risk = dict(sorted(hedge_pos_risk.items(), key=lambda item: item[1], reverse=True))
            self.log.info(f"\n内盘持仓:{self.base_pos}\n外盘持仓:{hedge_pos_risk}")
            text += f"总浮动盈亏:{sum_profit}\n"
            self.log.info(text)
            #if time.time()-self.send_tg_ts > 1800:
            #    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=text)
            #    self.send_tg_ts = int(time.time())
        except:
            self.log.warning(f"update_pos_data函数报错:{traceback.format_exc()}")
            
        # 计算净敞口
        # 内外盘仓位差报警
        if self.base_pos != self.last_base_pos:
            self.diff_num += 1
            if self.diff_num > 1:
                await self.check_pos(hedge_pos_risk)
                self.diff_num = 0
        else:
            self.diff_num = 0
        
        # if abs(hedge_position) > abs(base_pos):
        #     text = f"外盘仓位:{hedge_position} 内盘仓位:{base_pos} 外盘仓位大于内盘仓位,立即查看!!!\n"
        #     tb.warning(f"{self.symbol}合约持仓异常:{text}",'risk')
        #     tb.sendmail(f'{self.symbol}合约持仓异常', text)
            # await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q',chat_id=-1002413824899,content=text)
            
    async def check_pos (self, hedge_pos_risk):
        for k, v in self.base_pos.items():
            if k in hedge_pos_risk:
                self.log.info(f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v}")
                # 按对冲比例还原,若超过则报警
                hedge_pos_amount = abs(hedge_pos_risk[k])/self.hedge_pos_ratio
                if hedge_pos_amount > abs(v):
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位大于内盘仓位,立即查看!!!\n"
                    self.send_msg(text)                    
                # TODO 这里逻辑不太对,理论上应该完全相等,不相等的话,相差的是小于100u的金额
                elif hedge_pos_amount < abs(v)*0.8:
                    text = f"{k}外盘仓位:{hedge_pos_risk[k]} 内盘仓位:{v} 外盘仓位小于内盘仓位,立即查看!!!\n"
                    self.send_msg(text)
        for k, v in hedge_pos_risk.items():
            if k not in self.base_pos:
                text = (f"{k}外盘仓位:{v} 内盘仓位:0 立即平仓")
                self.send_msg(text)
        self.hedge_pos_risk = hedge_pos_risk
        self.last_base_pos = self.base_pos

    ''' ==========================================================================='''
    ''' ===================================== main ================================'''
    ''' ==========================================================================='''

    async def get_hold_list(self):
        while 1:
            hold_list = []
            temp_hold_list = []
            try:
                data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page_size=20, user_id=self.id)
                hold_list_page = data['data']['pager']['total_page']
                temp_hold_list += data['data']['data']
                for n in range(2, hold_list_page+1):
                    data = await self.ws_rest.hold_list(token='c1cf4185b2bed317aeb6e6674491fbef', page=n, page_size=20, user_id=self.id)
                    temp_hold_list += data['data']['data']
                    await asyncio.sleep(2)
                break
            except:
                pass
            await asyncio.sleep(5)
        for i in temp_hold_list:
            if i not in hold_list:
                hold_list.append(i)
        # self.log.info(f"当前所有用户持仓:{temp_hold_list}")
        # self.log.info(f"当前所有用户持仓222:{hold_list}")
        return hold_list
    
    async def get_hedge_dict(self, hold_list):
        temp_hedge_dict = {}
        temp_open_parity_price = {}   # 保存交易对开仓均价和爆仓价
        for i in hold_list:
            id = int(i['user_id'])
            tag = i['tag']
            symbol = i['symbol']
            '''
            若只想要一个条件约束,其他条件要写[]
            '''
            con1 = id == self.id and symbol not in self.symbol
            con2 = tag == self.tag and symbol not in self.symbol
            con3 = symbol in self.symbol and id != self.id and tag != self.tag
            con4 = id == self.id and symbol in self.symbol
            con5 = tag == self.tag and symbol in self.symbol
            if con1 or con2 or con3 or con4 or con5:
                # self.log.info(f"用户持仓情况:{i}")
                symbol = i['symbol']
                side = 'buy' if i['openDirection'] == 1 else 'sell'
                side_type = 1 if i['openDirection'] == 1 else -1
                open_price = float(i['avgPrice'])   # 开仓均价
                amount = float(i['amount'])*self.symbol_size[symbol]*side_type   # 持仓数量
                try:
                    parity = float(i['parity'])         # 强平价
                except:
                    parity = 0
                # 用来记录开仓均价和爆仓价
                if symbol in temp_open_parity_price:
                    if side in temp_open_parity_price[symbol]:
                        sum_amount = temp_open_parity_price[symbol][side][0]
                        ave_price = temp_open_parity_price[symbol][side][1]
                        open_price = (open_price*amount+sum_amount*ave_price)/(amount+sum_amount)
                        parity = max(parity, temp_open_parity_price[symbol][side][2]) if side == 'buy' \
                            else min(parity, temp_open_parity_price[symbol][side][2])
                        temp_open_parity_price[symbol][side][0] = sum_amount+amount
                        temp_open_parity_price[symbol][side][1] = open_price
                        temp_open_parity_price[symbol][side][2] = parity
                    else:
                        temp_open_parity_price[symbol][side] = [amount, open_price, parity]
                else:
                    temp_open_parity_price[symbol] = {side: [amount, open_price, parity]}
                # 统计当前内盘标记用户持仓
                if symbol not in temp_hedge_dict:
                    temp_hedge_dict[symbol] = amount
                else:
                    temp_hedge_dict[symbol] += amount
        mess = (f"内盘标记用户当前持仓:\n")
        for s, v in temp_hedge_dict.items():
            mess += (f"{s}持仓:{v}\n")
        self.log.info(mess)
        self.base_pos = temp_hedge_dict
        return temp_hedge_dict, temp_open_parity_price
    
    async def get_hedge_info(self, temp_hedge_dict):
        hedge_info = {'open':{}, 'close':{}}
        # 开仓、加减仓情况
        for s, v in temp_hedge_dict.items():
            last_pos = self.hedge_pos.get(s, 0)
            if v == last_pos:
                #self.log.info(f"{s}持仓已对冲过")
                pass
            elif last_pos == 0:
                self.log.info(f"{s}新增持仓:{v}")
                hedge_info['open'][s] = v
            elif v > last_pos and last_pos > 0:
                self.log.info(f"{s}多单持仓增加:{v-last_pos}")
                hedge_info['open'][s] = (v-last_pos)
            elif v < last_pos and last_pos > 0:
                self.log.info(f"{s}多单持仓减少:{last_pos-v}")
                hedge_info['close'][s] = -(last_pos-v)
            elif v > last_pos and last_pos < 0:
                self.log.info(f"{s}空单持仓减少:{v-last_pos}")
                hedge_info['close'][s] = (v-last_pos)
            elif v < last_pos and last_pos < 0:
                self.log.info(f"{s}空单持仓增加:{last_pos-v}")
                hedge_info['open'][s] = -(last_pos-v)
        # 平仓情况
        for s, v in self.hedge_pos.items():
            now_pos = temp_hedge_dict.get(s, 0)
            if now_pos == 0:
                self.log.info(f"{s}持仓已清空")
                hedge_info['close'][s] = -v
        mess = f"需要对冲的持仓:\n"
        for k, v in hedge_info['open'].items():
            mess += (f"开仓信息:{k}对冲:{v}\n")
        for k, v in hedge_info['close'].items():
            mess += (f"平仓信息:{k}对冲:{v}\n")
        self.log.info(mess)
        self.hedge_pos = temp_hedge_dict   # 判断完哪些仓位需要对冲后,更新持仓信息
        return hedge_info
    
    async def cancel_close_orders(self, temp_hedge_dict):
        # TODO 若挂单太多怎么办?
        res = await self.okx_rest.fetch_current_list(limit=100, state='live')
        current_symbols = {}
        for i in res:
            if i['symbol'] not in current_symbols:
                current_symbols[i['symbol']] = [i['id']]
            else:
                current_symbols[i['symbol']].append(i['id'])
        now_pos_symbols = list(temp_hedge_dict.keys())
        for s, v in current_symbols.items():
            if s not in now_pos_symbols:
                self.log.info(f"{s}持仓已清空,撤销挂单;外盘持仓")
                await self.cancel_orders(symbol=s, ids=v)
                await self.hedge_pos_close(s)
    
    async def main(self):
        # 获取当前所有用户持仓
        hold_list = self.hold_list   # await self.get_hold_list()
        # 按筛选条件获取持仓数据
        temp_hedge_dict, temp_open_parity_price = await self.get_hedge_dict(hold_list)
        # 判断持仓是否有变化,以及需要对冲和撤单的数据
        hedge_info = await self.get_hedge_info(temp_hedge_dict)
        # 撤销已平仓交易对的挂单
        # await self.cancel_close_orders(temp_hedge_dict)

        # 平仓对冲
        for k, v in hedge_info.get('close', {}).items():
            symbol = k
            side = 'buy' if v > 0 else'sell'
            vol = abs(v)
            depth = await self.okx_rest.fetch_depth(symbol=symbol, size=1)
            price = depth['asks'][0][0]*(1+self.price_slip) if side == 'buy' else depth['bids'][0][0]*(1-self.price_slip)
            self.log.info(f"平仓对冲价格:{price}\n对冲量:{vol}")
            # 去对冲
            await self.hedge(symbol, [price], [vol], side, '平仓')
            
        # 开仓对冲
        for k, v in hedge_info.get('open', {}).items():
            symbol = k
            side = 'buy' if v > 0 else 'sell'
            vol = v*self.hedge_pos_ratio
            # if abs(vol)*temp_open_parity_price[symbol][side][1] < self.hedge_amt_limit:
            #     self.log.info(f"{k}对冲数量:{vol} 小于对冲限制:{self.hedge_amt_limit} 暂时不对冲")
            #     continue
            
            open_price = temp_open_parity_price[symbol][side][1]
            parity = temp_open_parity_price[symbol][side][2]
            self.log.info(f"{k}对冲方向:{side} 开仓均价:{open_price} 强平价:{parity}")

            # 价格波动多少百分比会爆仓
            liquidity_ratio = 1-parity/open_price if side == 'buy' else parity/open_price-1
            hedge_price_interval = self.price_ratio if liquidity_ratio >= self.price_ratio \
                                    else round(liquidity_ratio, 3)
            hedge_step_percent = hedge_price_interval*self.hedge_price_ratio/self.hedge_step  # 每档对冲价格比例
            self.log.info(f"价格波动多少百分比会爆仓:{liquidity_ratio} 最大对冲价格比例:{hedge_price_interval} 每档对冲价格比例:{hedge_step_percent}")    

            # 根据斐波那契额数列对冲
            def generate_fibonacci(n):
                fib = [0, 1]
                for i in range(2, n):
                    fib.append(fib[-1] + fib[-2])
                return fib[:n]
            fbnq = generate_fibonacci(self.hedge_step+1)[1:]   # 生成step位斐波那契数列
            fenshu = []
            fbnq_sum = sum(fbnq)
            for i in fbnq:
                fenshu.append(round(i/fbnq_sum, 4))
            self.log.info(f"斐波那契额数列:{fbnq}  权重:{fenshu}")
            
            hedge_price_list = []
            hedge_vol_list = []
            for n in range(len(fenshu)):
                hedge_price = open_price*(1-hedge_step_percent*(n+1)) if side == 'buy' \
                        else open_price*(1+hedge_step_percent*(n+1))
                hedge_vol = vol*fenshu[n]
                hedge_price_list.append(hedge_price)
                hedge_vol_list.append(hedge_vol)
            self.log.info(f"对冲价格:{hedge_price_list}\n对冲量:{hedge_vol_list}")

            # 去对冲
            await self.hedge(symbol, hedge_price_list, hedge_vol_list, side)
                
        
    # 对冲
    async def hedge(self, symbol, price_list, vol_list, side, tag='开仓'):        
        # 更新交易对精度
        try:
            price_precision, amount_precision, face_value, hedge_vol_limit = self.symbol_precision[symbol]
        except:
            mess = f"{strategy_name} {symbol}在ok没有获取到交易对信息,无法对冲,立即人工介入"
            self.log.warning(mess)
            tb.warning(mess,'risk')
            tb.sendmail('ok对冲失败,无法获取交易对信息', mess)
            return
        # 处理可能多对冲的情况
        hedge_vol = sum([round(v, amount_precision) for v in vol_list]) # hedge_vol是带正负号的
        real_hedge_vol = self.hedge_pos_risk.get(symbol, 0)     # real_hedge_vol是带正负号的
        sum_vol = hedge_vol + real_hedge_vol    # 已对冲仓位+本次对冲仓位
        base_vol = self.base_pos.get(symbol, 0) # 内盘持仓
        diff_vol = sum_vol - base_vol           # 内外盘持仓差  TODO 这里需要优化,内外盘持仓方向可能不同
        if diff_vol > 0:
            if vol_list[-1] > diff_vol:
                vol_list[-1] -= diff_vol
            else:
               self.log.info(f"已对冲仓位:{real_hedge_vol} 本次对冲仓位:{hedge_vol} 内盘持仓:{base_vol} 本次对冲仓位过大,已跳过,立即查看原因!!!")
               return
        
        hedge_id_list = []
        t11 = time.time()*1000
        hedgeMess = f"交易对:{symbol} 方向:{side} 类型:{tag}\n"
        for i in range(len(price_list)):
            price = round(price_list[i], price_precision)
            if 'e' in str(price):
                price = f'{price:.10f}'
            # vol = round(vol_list[i], amount_precision) # 因为四舍五入有可能多对冲
            vol = vol_list[i]
            if vol < hedge_vol_limit or vol < face_value*hedge_vol_limit:   # 小于一张合约面值也会下单失败
                self.log.info(f"{symbol}对冲数量:{vol} 小于对冲限制:{hedge_vol_limit} 或者小于合约面值:{face_value*hedge_vol_limit} 跳过对冲")
                continue
            hedgeMess += f"对冲价量:{price}  {vol}\n"
            self.log.info(f"123123123:{hedgeMess}")
            # continue
            while True:
                try:
                    result = await self.okx_rest.create_order(symbol=symbol, order_type='limit',
                                                        side=side, amount=abs(vol), 
                                                        price=price, tdMode='cross')
                    hedge_id_list.append(result['id'])
                    break
                except:
                    self.log.error(f"{symbol}对冲side:{side} amount:{abs(vol)} price:{price} 下单失败:{traceback.format_exc()}")
                    if result['info']['data'][0]['sCode'] == '51006':
                        await self.chase_order(symbol, abs(vol), side)
                        break
                await asyncio.sleep(1)
        # {'symbol': 'ADA-USDT', 'result': True, 'id': '2269739899423547392', 'clientOrderId': '', 'timestamp': 1740145920218, 'info': {'code': '0', 'data': [{'clOrdId': '', 'ordId': '2269739899423547392', 'sCode': '0', 'sMsg': 'Order placed', 'tag': '', 'ts': '1740145920218'}], 'inTime': '1740145920217640', 'msg': '', 'outTime': '1740145920219807'}}
        self.log.info(f"{hedgeMess}对冲延时:{round(time.time()*1000-t11, 2)}ms\n本次对冲order_id:{hedge_id_list}")

    async def chase_order(self, symbol, vol, side):
        self.log.info(f"{symbol}对冲失败,追单下单")
        try:
            depth = await self.okx_rest.fetch_depth(symbol=symbol, size=1)
            price = depth['asks'][0][0]*(1+self.price_slip) if side == 'buy' else depth['bids'][0][0]*(1-self.price_slip)
            # self.log.info(f"{symbol}追单price:{price} vol:{vol} side:{side}")
            # return
            result = await self.okx_rest.create_order(symbol=symbol, order_type='limit', 
                                                        side=side, amount=abs(vol), 
                                                        price=price, tdMode='cross')
            self.log.info(f"{symbol}追单price:{price} vol:{vol} side:{side} 结果:{result}")
        except:
            self.log.error(f"{symbol}追单下单失败:{traceback.format_exc()}")

def main(path='grid_hedge_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()
    while True:
        time.sleep(999999)



if __name__ == '__main__':
    main()


