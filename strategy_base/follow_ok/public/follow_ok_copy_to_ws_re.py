'''
    跟单逻辑相关链接:
    平仓: https://www.okx.com/zh-hans/help/closing-lead-trades-with-limit-order-and-custom-amount
    跟随限制: https://www.okx.com/zh-hans/help/what-limits-are-there-for-perpetual-copy-trading
'''
import sys
import os
import time
import datetime
import traceback
import asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # follow_ok/
sys.path.append('../../..')
from template.template_timer import TemplateTimer, CronTrigger
# from crypto_center.client.rest.websea.contract_quan import WebseaContract as ws_contract_rest
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
import objects.contract_request.websea as ocw
from utils import Toolbox as tb
from utils import restclient as rc


# 限频规则是5次/2s IP级别

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self.teacher_deals = {}    # 保存所有的交易记录
        self.precision_dict = {}   # 保存内盘合约信息
        self.last_send_tg_time = 0 # 上次发送tg的时间
        self.symbol_size = {}      # 保存内盘合约单位
        self.out_symbol_size = {}  # 保存外盘合约单位
        self.all_pos = {}          # 保存所有交易对的持仓情况
        self.last_traders = []     # 上次获取的带单老师信息
        self.first_run = True      # 第一次运行
        self.pos_err_cnt = 0       # 查询持仓连续失败次数
        self.invest_err_cnt = 0    # 查询本金连续失败次数
        self.last_pos_dict = {}    # 上次带单员持仓 {instId: 币数量}
        self.websea_pos = {}       # 内盘持仓 {instId: 币数量}
        
    def _load_config(self, config):
        """读取配置文件
        """
        self.rc_task = rc.RestClient()
        # 配置全局变量
        [setattr(self, k, v) for k, v in vars(config).items()]        # 批量生成所有参数
        config = config.config
        # 配置账号
        self.ws_rest = ws_contract_rest(config['base_token'], config['base_secret'])
        self.okx_rest = okx_rest.OkexContract(config['hedge_token'],config['hedge_secret'],config['passphrase'])
        self.ws_rest.DEBUG = False
        self.okx_rest.DEBUG = False
        
    async def on_first(self):
        self.log.add(f"log/okx_follow.log", rotation="100 MB", retention=10)
        await self.get_symbol_unit()        # 获取合约单位
        await self.get_out_symbol_unit()    # 获取外盘合约单位
        # await self.pos_monitor()            # 获取交易记录
        await self.risk()
        
    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.pos_monitor, CronTrigger(second="*/10"))
        self.schedule.add_job(self.risk, CronTrigger(minute="*"))
    
    async def get_symbol_unit(self):
        # 查询内盘合约单位
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
    
    async def get_out_symbol_unit(self):
        # 查询外盘合约单位
        while True:
            try:
                res = await self.okx_rest.fetch_precision()
                # print(res, type(res))
                for s, data in res.items():
                    self.out_symbol_size[s] = float(data["faceValue"])
                self.log.info(f"外盘合约单位:{self.out_symbol_size}")
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)
    
    async def get_precision(self, symbol):
        """更新 币对信息"""
        precision = await self.ws_rest.get_precision(symbol, quan=True)
        self.precision_dict[symbol] = precision
        self.log.info(f"{symbol}币对信息:{precision}")
        return precision
    
    async def send_tg(self, content):
        """发送 Telegram 消息"""
        try:
            await self.rc_task.tg_warning(token=self.tel_token, chat_id=self.chat_id, content=content)
        except:
            self.log.warning(f"send_tg失败:{traceback.format_exc()}")

    # 发送报警
    async def send_warning(self, title, warning_mess):
        try:
            self.log.warning(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            self.log.warning(f"send_warning发送报警信息失败:{traceback.format_exc()}")
    
    async def first_close_pos(self, symbol):
        close_vol = self.websea_pos.get(symbol, 0)/self.symbol_size[symbol]
        if close_vol == 0:
            print(f"初始平仓时,{symbol}内盘无持仓")
            return
        od_type = ocw.OrderType.buy_market if close_vol < 0 else ocw.OrderType.sell_market
        for i in range(5):
            try:
                depth = await self.ws_rest.get_depth(symbol, quan=True)
                # self.log.info(f"获取{symbol}深度数据:{depth}")
                price = depth.asks[0].price*(1+self.slip) if close_vol < 0 else \
                        depth.bids[0].price*(1-self.slip)
                precision = await self.get_precision(symbol)
                break
            except:
                last_err = traceback.format_exc()
                self.log.warning(f"get_depth报错{symbol}交易对:{last_err}")
                await asyncio.sleep(0.2)
        await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                        price=price, amount=abs(close_vol), \
                                        precision=precision, contract_type='close')
        print(f"初始平仓,{symbol}内盘仓位全平")

    async def risk(self):
        '''
            查持仓,查余额
        '''
        balance = await self.ws_rest.get_walletList(is_full=2)
        self.balance = balance.avail
        self.log.info(f"带单账户余额:{balance.avail}")
        res = await self.ws_rest.get_position(is_full=1)
        pos_mess = f"{self.strategy_name}持仓:\n"
        for p in res:
            symbol = p.symbol
            pos = p.amount
            profit = p.profit
            open_price = p.open_price_avg
            leval = p.lever_rate
            liqu_price = p.liquidation_price
            side = 'buy' if p.type == 1 else 'sell'
            size = self.symbol_size.get(symbol, 1)
            index_res = await self.ws_rest.get_index(symbol)
            mark_price = index_res.price
            self.all_pos[symbol] = {side: pos}
            pos_mess += f"{symbol} 持仓:{pos*size} 方向:{side} 开仓价:{open_price} 标记价:{mark_price} 盈亏:{profit} 杠杆:{leval} 爆仓价:{liqu_price}\n"
        self.log.info(pos_mess)
        # 内盘持仓
        for k, v in self.all_pos.items():
            for side, pos in v.items():
                pos = pos if side == 'buy' else -pos
                if k not in self.last_pos_dict:
                    self.websea_pos[k] = pos*size
                else:
                    self.websea_pos[k] += pos*size
        self.log.info(f"内盘当前持仓:{self.websea_pos}")
    
    # 查询带单员是否有成交
    async def pos_monitor(self):
        # 查询带单员当前持仓情况
        try:
            from okx_public_current_subpositions import get_public_current_subpositions
            rows = get_public_current_subpositions(self.lead_id).get("data") or []
            self.pos_err_cnt = 0
            mess = f"带单员当前持仓共{len(rows)}笔:\n"
            for i in rows:
                openTime = datetime.datetime.fromtimestamp(int(i['openTime'])/1000).strftime("%Y-%m-%d %H:%M:%S")
                mess += f"{i['instId']} 持仓:{i['subPos']} 方向:{i['posSide']} 开仓价:{round(float(i['openAvgPx']), 4)} 标记价:{round(float(i['markPx']), 2)} 盈亏:{round(float(i['upl']), 2)} 杠杆:{i['lever']} 开仓时间:{openTime}\n"
            self.log.info(mess)

        except:
            self.pos_err_cnt += 1
            self.log.warning(f"查询当前持仓失败({self.pos_err_cnt}):{traceback.format_exc()}")
            if self.pos_err_cnt >= 20:
                await self.send_tg(f"{self.strategy_name} 查询带单员持仓连续失败{self.pos_err_cnt}次, lead_id={self.lead_id}")
                self.pos_err_cnt = 0
            return
        
        # 判断带单员是否设置隐私
        privacy = 0
        for i in rows:
            if i['instId'] == "" and i['markPx'] == "":
                privacy += 1
                print(f"{self.lead_id}最新持仓:{i}")
        if privacy >= 1:
            await self.send_tg(f"{self.strategy_name} {self.lead_id}带单员设置了隐私,立即切换带单员, 当前持仓:{len(rows)}笔, 设置了隐私的持仓:{privacy}笔")
            # TODO 把现有仓位全平
            return

        # 统计持仓情况
        [{'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'lever': '20', 'margin': '4999.9806805', 'markPx': '1913.86', 'mgnMode': 'cross', 'openAvgPx': '1919.71', 'openTime': '1785953870185', 'posSide': 'net', 'subPos': '-520.91', 'subPosId': '3806799641624260608', 'uniqueCode': 'B15E19173830B674', 'upl': '304.73235', 'uplRatio': '0.060946705491976'}, 
        {'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'lever': '20', 'margin': '4999.909389', 'markPx': '1913.86', 'mgnMode': 'cross', 'openAvgPx': '1919.13', 'openTime': '1785953855036', 'posSide': 'net', 'subPos': '-521.06', 'subPosId': '3806799133308170240', 'uniqueCode': 'B15E19173830B674', 'upl': '274.59862', 'uplRatio': '0.05492071928426'}, 
        {'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'lever': '20', 'margin': '500.0613955', 'markPx': '1913.86', 'mgnMode': 'cross', 'openAvgPx': '1919.99', 'openTime': '1785953842665', 'posSide': 'net', 'subPos': '-52.09', 'subPosId': '3806798718206291968', 'uniqueCode': 'B15E19173830B674', 'upl': '31.93117', 'uplRatio': '0.063854499242184'}, 
        {'ccy': 'USDT', 'instId': 'ETH-USDT-SWAP', 'instType': 'SWAP', 'lever': '20', 'margin': '14999.97598', 'markPx': '1913.86', 'mgnMode': 'cross', 'openAvgPx': '1888.69', 'openTime': '1785948486862', 'posSide': 'net', 'subPos': '-1588.4', 'subPosId': '3806619007278723072', 'uniqueCode': 'B15E19173830B674', 'upl': '-3998.0028', 'uplRatio': '-0.266533946809694'}]

        # 测试数据
        # self.last_pos_dict = {'ETH-USDT-SWAP': 0}
        # self.last_pos_dict = {'ETH-USDT-SWAP': -200}
        # 测试数据
        
        pos_dict = {}
        level_dict = {}
        for i in rows:
            price = float(i['openAvgPx'])   # 开仓均价
            level = int(i['lever'])         # 杠杆
            this_pos = float(i['subPos'])   # 张数
            size = self.out_symbol_size.get(i['instId'].replace('-SWAP', ''), 1)
            if i['posSide'] == 'net':
                this_pos *= size
            else:
                side = 1 if i['posSide'] == 'long' else -1
                this_pos = this_pos*size*side
            if i['instId'].replace('-SWAP', '') not in pos_dict:
                pos_dict[i['instId']] = this_pos
                level_dict[i['instId']] = level
            else:
                pos_dict[i['instId']] += this_pos   # 币的数量
        
        # 测试数据
        # pos_dict = {}
        # 测试数据

        self.log.info(f"带单员最新持仓:{pos_dict}")
        if pos_dict == self.last_pos_dict:
            self.log.info(f"带单员持仓未变化,不进行操作")
            return
        
        # 查询带单账户的本金
        try:
            from okx_public_stats import get_public_stats
            invest_amt = float((get_public_stats(self.lead_id).get("data") or [{}])[0].get("investAmt") or 0)
            self.invest_err_cnt = 0
            self.log.info(f"带单员本金 investAmt={invest_amt}")
        except:
            self.invest_err_cnt += 1
            self.log.warning(f"查询带单员本金失败({self.invest_err_cnt}):{traceback.format_exc()}")
            if self.invest_err_cnt >= 20:
                await self.send_tg(f"{self.strategy_name} 查询带单员本金连续失败{self.invest_err_cnt}次, lead_id={self.lead_id}")
                self.invest_err_cnt = 0
            return
        
        # 计算本次下单金额（相对上次持仓的增减，币数量；>0开多/加仓，<0开空/加空）
        pos_diff, is_reduce = {}, {}
        for sym in set(pos_dict) | set(self.last_pos_dict):
            last, now = self.last_pos_dict.get(sym, 0), pos_dict.get(sym, 0)
            d = now - last
            if abs(d) <= 1e-12:
                continue
            pos_diff[sym] = d
            # 同向且绝对值变小 → 减仓（含全平）
            is_reduce[sym.replace('-SWAP', '')] = bool(last and last * now >= 0 and abs(now) < abs(last))
        self.log.info(f"持仓差异 pos_diff={pos_diff} is_reduce={is_reduce} last={self.last_pos_dict} now={pos_dict}")

        # 计算下单比例
        for k, vol in pos_diff.items():
            symbol = k.replace('-SWAP', '')
            # 不在带单范围不跟单
            if self.ws_level.get(symbol, 0) == 0:
                print(f"{symbol}不在带单范围,不跟单")
                continue
            side = 'sell' if vol > 0 else 'buy'
            # 测试数据
            # vol = 0.01 if vol > 0 else -0.01
            # 测试数据
            hedge_vol = 0
            if pos_dict.get(k, 0) == 0:
                # 全平
                res = await self.ws_rest.get_position(is_full=1)
                for p in res:
                    print(f"内盘持仓:{p} {p.symbol}")
                    if p.symbol == symbol:
                        hedge_vol = p.amount  #*self.symbol_size[symbol]
                        print(f"{symbol}内盘全平:{hedge_vol}")
                if hedge_vol == 0:
                    print(f"{symbol}内盘无持仓")
                    return
            else:
                index_price = await self.ws_rest.get_index(symbol)
                price = float(index_price.price)
                level = level_dict[k]
                amt = abs(vol)*price
                ratio = amt/(invest_amt*level)
                print(111111111, vol, price, level, amt, invest_amt, invest_amt*level, ratio)
                # 首次判断
                if self.first_run:
                    ws_level = self.ws_level[symbol]
                    ws_ratio = abs((self.websea_pos.get(symbol, 0)*price)/(self.balance*ws_level))
                    print(f"首次判断{symbol}，内外盘仓位比例: ws_ratio:{ws_ratio} ratio:{ratio}")
                    # 内盘均无持仓
                    if ws_ratio == 0 and ratio == ws_ratio:
                        print(f"首次判断，内外盘仓位均为0")
                        continue
                    # 外盘无持仓，内盘有持仓
                    if ratio == 0 and ratio != ws_ratio:
                        print(f"首次判断，外盘无持仓，内盘有持仓。内盘仓位全平")
                        await self.first_close_pos(symbol)
                        continue
                    # 外盘有持仓，内盘无持仓
                    if ratio != 0 and ws_ratio == 0:
                        print(f"首次判断，外盘有持仓，内盘无持仓。直接跟单外盘仓位")
                    # 内外盘均有持仓
                    if ws_ratio != 0 and ratio != 0:
                        print(f"首次判断，内外盘均有持仓，仓位差: ratio:{ratio} ws_ratio:{ws_ratio} {abs(ratio/ws_ratio-1)}")
                        if abs(ratio/ws_ratio-1) < 0.01:
                            self.last_pos_dict = pos_dict
                            print(f"首次判断，内外盘仓位差不足1%，不进行跟单")
                            continue
                        if ws_ratio > ratio:
                            self.last_pos_dict = pos_dict
                            print(f"首次判断，内盘持仓大于外盘，不对冲")
                            continue
                # websea跟单数量计算
                ws_level = self.ws_level[symbol]
                hedge_vol = int(self.balance*ws_level*ratio/price/self.symbol_size[symbol])  # 对冲数量
                print(222222222, self.balance, ws_level, ratio, self.balance*ws_level*ratio, price, hedge_vol)
            self.log.info(f"下单信息: {symbol} {side} {hedge_vol} 是否减仓:{is_reduce}")
            await self.to_deal((symbol, side, hedge_vol, is_reduce))
        self.first_run = False
        self.last_pos_dict = dict(pos_dict)
    
    async def to_deal(self, new_deals):
        symbol, side, vol, is_reduce = new_deals
        od_type = ocw.OrderType.buy_market if side == 'buy' else ocw.OrderType.sell_market
        last_err = ""
        for i in range(5):
            try:
                depth = await self.ws_rest.get_depth(symbol, quan=True)
                # self.log.info(f"获取{symbol}深度数据:{depth}")
                price = depth.asks[0].price*(1+self.slip) if side == 'buy' else \
                        depth.bids[0].price*(1-self.slip)
                precision = await self.get_precision(symbol)
                break
            except:
                last_err = traceback.format_exc()
                self.log.warning(f"get_depth报错{symbol}交易对:{last_err}")
                await asyncio.sleep(0.2)
        else:
            await self.send_tg(
                f"{self.strategy_name} 获取深度/精度失败 lead_id={self.lead_id} 接口=get_depth/get_precision "
                f"symbol={symbol} 报错:{last_err}"
            )
            return
        
        trigger_mess = f"{self.strategy_name} 触发跟单{self.lead_id} {symbol} 方向:{side} 数量:{abs(vol)} 价格:{price}"
        await self.send_tg(trigger_mess)
        try:
            t1 = time.time()*1000
            if not is_reduce[symbol]:
                res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                    price=price, amount=abs(vol), \
                                                    precision=precision)
            else:
                res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                    price=price, amount=abs(vol), \
                                                    precision=precision, contract_type='close')
            deal_mess = f"{self.strategy_name} 下单完成 跟单{self.lead_id} {symbol} 价格:{price} 数量:{abs(vol)} 方向:{side} 延时:{round(time.time()*1000-t1, 2)}ms 回报:{res}"
            self.log.info(deal_mess)
            await self.send_tg(deal_mess)
        except:
            warning_mess = f"{self.strategy_name} 下单失败 跟单{self.lead_id}下单 {symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
            await self.send_tg(warning_mess)
            self.log.warning(f"{warning_mess}")


def main(path='re_ok_follow_deal_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()


