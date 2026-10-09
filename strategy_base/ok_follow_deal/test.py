'''
    await self.ws_rest.get_walletList(is_full=2)  # 全仓有5000u
    只能下市价单
    res = await self.ws_rest.order_create(symbol='ADA-USDT', od_type=ocw.OrderType.buy_market, \
                                          price=0.1, amount=1, \
                                          precision=precision)
    
'''
import sys
import time
import datetime
import traceback
import asyncio
import mysql.connector
sys.path.append('../..')
from template.template_timer import TemplateTimer, CronTrigger
# from crypto_center.client.rest.websea.contract_quan import WebseaContract as ws_contract_rest
from client.env_pro.rest.websea.contract import WebseaContract as ws_contract_rest
from crypto_center.client.rest.okex import contract as okx_rest
import objects.contract_request.websea as ocw
from utils import Toolbox as tb
from utils import restclient as rc

strategy_name = "555555用户copy OKX带单策略"

# 限频规则是5次/2s IP级别

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self, config):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self._load_config(config)  # 读取配置
        self._setLocalDict()       # 配置本地数据
        
        self.teacher_deals = {}    # 保存所有的交易记录
        self.precision_dict = {}   # 保存内盘合约信息
        self.last_send_tg_time = 0 # 上次发送tg的时间
        self.symbol_size = {}      # 保存内盘合约单位
        self.all_pos = {}          # 保存所有交易对的持仓情况
        self.profit_ratio = 0.01   # 盈利比例
        self.last_traders = []     # 上次获取的带单老师信息
        self.first_run = True      # 第一次运行
        
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
        self.okx_rest.DEBUG = False

    def _setLocalDict(self):
        self._local_user = tb.LocalDict('local_follow_user.log')
        self.traders = self._local_user.load().get('users', {})    # 保存所有成交聚合数据
        self.target_profit_deal = self._local_user.load().get('target_profit', {})

    async def on_first(self):
        self.log.add(f"log/okx_follow.log", rotation="100 MB", retention=10)
        # await self.create_table()       # 创建表
        # await self.update_teacher_pos() # 更新老师持仓信息
        await self.get_teacher()        # 获取带单老师信息
        # await self.get_symbol_unit()    # 获取合约单位
        # await self.fetch_deals()        # 获取交易记录
        # await self.risk()               # 风控检查
        
        # res = await self.ws_rest.order_detail('BM5555551754377691702WRR3VN')
        # print(f"查询订单:{res}")
        # res = await self.ws_rest.order_cancel('ADA-USDT')
        # print(f"撤单:{res}")
        # precision = await self.get_precision('BTC-USDT')
        # res = await self.ws_rest.order_create(symbol='BTC-USDT', od_type=ocw.OrderType.buy_market, \
        #                                               price=120000, amount=1, \
        #                                               precision=precision, contract_type='close')
        # print(f"下单:{res}")
        # balance = await self.ws_rest.get_walletList(is_full=2)
        # print(f"余额:{balance}")
        # res = await self.ws_rest.get_position(is_full=2)
        # print(f"持仓:{res}")
        
    
    async def on_timer(self):
        self.log.info("=======timer======")
        # self.schedule.add_job(self.fetch_deals, CronTrigger(minute="*"))
        # self.schedule.add_job(self.risk, CronTrigger(minute="*"))
    
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_db",
            port=3306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
    async def create_table(self):
        await self.connect_db()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS `okx_teacher_deals` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `teacher_id` VARCHAR(36) NOT NULL,
                `teacher_nick` VARCHAR(36) NOT NULL,
                `symbol` VARCHAR(36) NOT NULL,
                `side` VARCHAR(36) NOT NULL,
                `vol` DECIMAL(20,12) NOT NULL DEFAULT 0,
                `pos` DECIMAL(20,12) NOT NULL DEFAULT 0,
                `open_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
    
    async def update_teacher_pos(self):
        self.cursor.execute(f"SELECT * FROM okx_teacher_deals")
        data = self.cursor.fetchall()
        for i in data:
            id, symbol, pos = i[1], i[3], i[6]
            if pos != 0:
                if id in self.teacher_deals:
                    self.teacher_deals[id][symbol] = pos
                else:
                    self.teacher_deals[id] = {symbol: pos}
        self.db.close()   # 关闭数据库连接
        self.log.info(f"根据mysql初始化的当前持仓:{self.teacher_deals}")
    
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
    
    async def get_precision(self, symbol):
        """更新 币对信息"""
        precision = await self.ws_rest.get_precision(symbol, quan=True)
        self.precision_dict[symbol] = precision
        self.log.info(f"{symbol}币对信息:{precision}")
        return precision
    
    async def get_teacher(self):
        # if self.traders == {}:
        if 1:
            traders = []
            for n in range(1,10):
                res = await self.okx_rest.fetch_traders(minLeadDays='1', page=n, limit=20)
                for i in res:
                    traderId = i['traderId']    # 带单员唯一编号
                    winRatio = i['winRatio']    # 胜率
                    leadDays = i['leadDays']    # 带单天数
                    leadAum = int(float(i['leadAum']))      # 带单规模
                    pnl = int(float(i['info']['pnl']))      # 近90天盈亏
                    pnlRatio = round(float(i['info']['pnlRatio'])*100, 2)   # 近90天收益率
                    copyTraderNum = int(i['copyTraderNum'])      # 跟单人数
                    profit_pnl_ratio_list = []
                    loss_pnl_ratio_list = []
                    for j in i['info']['pnlRatios']:
                        if float(j['pnlRatio']) > 0:
                            profit_pnl_ratio_list.append(float(j['pnlRatio']))
                        else:
                            loss_pnl_ratio_list.append(float(j['pnlRatio']))
                    ave_profit_pnl_ratio = round(sum(profit_pnl_ratio_list)/len(profit_pnl_ratio_list), 2) if profit_pnl_ratio_list else 0
                    ave_loss_pnl_ratio = round(sum(loss_pnl_ratio_list)/len(loss_pnl_ratio_list), 2) if loss_pnl_ratio_list else 0
                    # 盈亏比 = 平均每次盈利 / 平均每次亏损
                    if ave_loss_pnl_ratio != 0:
                        profit_loss_ratio = round(ave_profit_pnl_ratio / ave_loss_pnl_ratio, 2) if loss_pnl_ratio_list else 0
                    else:
                        profit_loss_ratio = None
                    # if copyTraderNum > 30 and pnlRatio > 10:
                    if winRatio > 0.7 and leadAum > 1000:
                        # if profit_loss_ratio is None:
                        #     print(i)
                        self.log.info(f"带单员:{traderId} {i['info']['nickName']} 胜率:{winRatio} 盈亏比:{profit_loss_ratio} 带单天数:{leadDays} 带单规模:{leadAum} 近7天盈亏:{pnl} 近7天收益率:{pnlRatio}% 跟单人数:{copyTraderNum}\n")
                        traders.append([traderId, i['info']['nickName']])
            self.log.info(f"跟单用户列表:{traders} {len(traders)}")

            for n in range(1,10):
                res = await self.okx_rest.fetch_traders(minLeadDays='2', page=n, limit=20)
                for i in res:
                    traderId = i['traderId']    # 带单员唯一编号
                    winRatio = i['winRatio']    # 胜率
                    leadDays = i['leadDays']    # 带单天数
                    leadAum = int(float(i['leadAum']))      # 带单规模
                    pnl = int(float(i['info']['pnl']))      # 近90天盈亏
                    pnlRatio = round(float(i['info']['pnlRatio'])*100, 2)   # 近90天收益率
                    copyTraderNum = int(i['copyTraderNum'])      # 跟单人数
                    profit_pnl_ratio_list = []
                    loss_pnl_ratio_list = []
                    for j in i['info']['pnlRatios']:
                        if float(j['pnlRatio']) > 0:
                            profit_pnl_ratio_list.append(float(j['pnlRatio']))
                        else:
                            loss_pnl_ratio_list.append(float(j['pnlRatio']))
                    ave_profit_pnl_ratio = round(sum(profit_pnl_ratio_list)/len(profit_pnl_ratio_list), 2) if profit_pnl_ratio_list else 0
                    ave_loss_pnl_ratio = round(sum(loss_pnl_ratio_list)/len(loss_pnl_ratio_list), 2) if loss_pnl_ratio_list else 0
                    # 盈亏比 = 平均每次盈利 / 平均每次亏损
                    if ave_loss_pnl_ratio != 0:
                        profit_loss_ratio = round(ave_profit_pnl_ratio / ave_loss_pnl_ratio, 2) if loss_pnl_ratio_list else 0
                    else:
                        profit_loss_ratio = None

                    # if copyTraderNum > 30 and pnlRatio > 10:
                    if winRatio > 0.7 and leadAum > 1000:
                        # if profit_loss_ratio is None:
                        #     print(i)
                        self.log.info(f"带单员:{traderId} {i['info']['nickName']} 胜率:{winRatio} 盈亏比:{profit_loss_ratio} 带单天数:{leadDays} 带单规模:{leadAum} 近30天盈亏:{pnl} 近30天收益率:{pnlRatio}% 跟单人数:{copyTraderNum}\n")
                        traders.append([traderId, i['info']['nickName']])
            self.log.info(f"跟单用户列表:{traders} {len(traders)}")
            self.traders = traders
    
    # 发送报警
    async def send_warning(self, title, warning_mess):
        try:
            self.log.warning(warning_mess)
            tb.warning(warning_mess, 'risk')
            # tb.sendmail(title, warning_mess)
        except:
            self.log.warning(f"send_warning发送报警信息失败:{traceback.format_exc()}")
    
    async def risk(self):
        '''
            查持仓,查余额
        '''
        balance = await self.ws_rest.get_walletList(is_full=2)
        self.log.info(f"带单账户余额:{balance.avail}")
        res = await self.ws_rest.get_position(is_full=1)
        pos_mess = f"{strategy_name}持仓:\n"
        for p in res:
            symbol = p.symbol
            pos = p.amount
            profit = p.profit
            open_price = p.open_price_avg
            leval = p.lever_rate
            liqu_price = p.liquidation_price
            side = 'buy' if p.type == 1 else 'sell'
            size = self.symbol_size.get(symbol, 1)
            res = await self.ws_rest.get_index(symbol)
            mark_price = res.price
            self.all_pos[symbol] = {side: pos}
            pos_mess += f"{symbol} 持仓:{pos*size} 方向:{side} 开仓价:{open_price} 标记价:{mark_price} 盈亏:{profit} 杠杆:{leval} 爆仓价:{liqu_price}\n"
            # 平仓
            if side == 'buy' and mark_price/open_price-1 > self.profit_ratio:
                self.log.info(f"buy止盈订单:{symbol} 持仓:{pos*size} 方向:{side} 开仓价:{open_price} 标记价:{mark_price} 盈亏:{profit}")
                await self.target_profit(symbol, pos, mark_price, side)
            elif side =='sell' and open_price/mark_price-1 > self.profit_ratio:
                self.log.info(f"sell止盈订单:{symbol} 持仓:{pos*size} 方向:{side} 开仓价:{open_price} 标记价:{mark_price} 盈亏:{profit}")
                await self.target_profit(symbol, pos, mark_price, side)
        self.log.info(pos_mess)
        # if time.time() - self.last_send_tg_time > 60*10:
        #     await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893618309, content=pos_mess)
        #     self.last_send_tg_time = time.time()
    
    # 止盈平仓
    async def target_profit(self, symbol, vol, price, side):
        self.target_profit_deal[self.traders[0][1]] = (symbol, vol, price, side)
        try:
            precision = await self.get_precision(symbol)
            od_type = ocw.OrderType.sell_market if side == 'buy' else ocw.OrderType.buy_market
            t1 = time.time()*1000
            res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                    price=price, amount=abs(vol), \
                                                    precision=precision, contract_type='close')
            deal_mess = f"{strategy_name}止盈下单 {symbol} 对冲价格:{price} 对冲数量:{abs(vol)} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{res}"
            self.log.info(f"下单回报:{deal_mess}")
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=self.tel_id, content=deal_mess)
        except:
            title = f"{strategy_name} 下单失败"
            warning_mess = f"{strategy_name} 止盈下单 {symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
            await self.send_warning(title, warning_mess)
            self.log.warning(f"{warning_mess}")
            
    # 查询带单员是否有成交
    async def fetch_deals(self):
        diff_pos_mess = ""
        save_mysql_deals = []
        pop_trades = []
        if self.first_run:
            self.first_run = False
            loc_traders = self.traders
        else:
            loc_traders = [self.traders[0]]
        for id in loc_traders:
            pos_dict = {}
            temp_symbol_pos = {}    # 保存交易对持仓
            mess = f"带单员:{id[0]} {id[1]} 持仓信息:\n"
            while True:
                try:
                    res = await self.okx_rest.fetch_trader_pos(id[0])
                    self.log.info(f"{id[0]} {id[1]}用户持仓:{res}")
                    if isinstance(res, list):
                        break
                    self.log.info(f"查询fetch_trader_pos报错:{res}")
                except:
                    self.log.warning(f"获取{id[0]}持仓失败, {traceback.print_exc()},重试中...")
                await asyncio.sleep(5)
            for i in res:
                pos = 0
                # 交易员设置保密状态后,会更新本地交易员信息,再选择第二个交易员去跟单。原来仓位需要人工查看是否调整
                if i['symbol'] == '':
                    self.log.info(f"带单员:{id[0]} {id[1]} 设置了保密状态,不可查看交易信息")
                    if id not in pop_trades:
                        pop_trades.append(id)
                    continue
                
                symbol = i['symbol'].split('-SWAP')[0]   # 交易对
                if i['posSide'] == 'net':       # 单向持仓
                    pos = i['posContracts']     # 仓位,带方向
                elif i['posSide'] == 'long':    # 双向持仓
                    pos += i['posContracts']     # 仓位,带方向
                elif i['posSide'] =='short':
                    pos += i['posContracts']*-1  # 仓位,带方向
                if symbol not in temp_symbol_pos:
                    temp_symbol_pos[symbol] = pos
                else:
                    temp_symbol_pos[symbol] += pos
                leverage = i['leverage']      # 杠杆
                openTs = i['openTs']          # 开仓时间
                date = datetime.datetime.fromtimestamp(openTs/1000)  #.strftime('%Y-%m-%d %H:%M:%S')
                date = date + datetime.timedelta(hours=8)
                temp_pos = [temp_symbol_pos[symbol], i['posSide'], leverage, date]
                pos_dict[symbol] = temp_pos
            self.log.info(f"{mess}{pos_dict}")
            
            # 判断是否有交易
            symbols1 = list(self.teacher_deals.get(id[0], {}).keys())
            symbols2 = list(pos_dict.keys())
            symbols = list(set(symbols1 + symbols2))     # 合并两个list并去重
            
            self.log.info(f"290数据验证:{id} {self.teacher_deals}")
            teacher_deals = self.teacher_deals.get(id[0], {})   # 之前的持仓
            self.log.info(f"{id[0]}用户 teacher_deals:{teacher_deals}\npos_dict:{pos_dict}")
            for s in symbols:
                symbol = s.split('-SWAP')[0]
                last_pos = float(teacher_deals.get(s, 0))
                new_pos = float(pos_dict.get(s, [0,0,0,0])[0])
                open_date = pos_dict.get(s, [0,0,0,0])[3]
                if float(last_pos) != float(new_pos):
                    self.log.info(f"验证111:{id[0]}用户 {s} last_pos:{last_pos} new_pos:{new_pos}")
                    diff_pos = new_pos - last_pos     # 持仓带方向
                    try:
                        min_vol = self.precision_dict[symbol].minQuantity
                    except:
                        try:
                            precision = await self.get_precision(symbol)
                            min_vol = precision.minQuantity
                        except:
                            continue
                    if abs(diff_pos) < min_vol:
                        continue
                    side = 'buy' if diff_pos > 0 else 'sell'
                    date = datetime.datetime.now() + datetime.timedelta(hours=8)
                    diff_pos_mess += f"{id[0]} {id[1]}用户 {symbol} {side} 本次交易:{diff_pos} 当前持仓:{new_pos} {date}\n"
                    save_mysql_deals.append([(id[0], id[1], symbol, side, diff_pos, new_pos, open_date, date)])
                    self.log.info(f"验证save_mysql_deals新增:{[(id[0], symbol, side, diff_pos, new_pos, open_date, date)]}")
                    # 验证获取到最新持仓信息和产生交易的时间相差多少
                    try:
                        self.log.info(f"数据验证:{date} {type(date)} {open_date} {type(open_date)}")
                        if open_date:
                            diff_time = date-open_date
                            self.log.info(f"成交时间差diff_time:{diff_time}")
                    except:
                        self.log.warning(f"date:{date} open_date:{open_date} 报错:{traceback.format_exc()}")
            if diff_pos_mess:
                self.log.info(f"有新增交易:{diff_pos_mess}")
            
            # 更新self.teacher_deals全局变量
            self.log.info(f"self.teacher_deals开始:{self.teacher_deals}")
            for s in symbols:
                if s in pos_dict:
                    for s, v in pos_dict.items():
                        if id[0] not in self.teacher_deals:
                            self.teacher_deals[id[0]] = {s: float(v[0])}
                        else:
                            self.teacher_deals[id[0]][s] = float(v[0])
                else:
                    del self.teacher_deals[id[0]][s]
            self.log.info(f"self.teacher_deals结束:{self.teacher_deals}")
        # 本地化用户信息
        # 需要从本地化数据中删除,并统计还剩余几个用户是开放状态,只剩2个用户时,需要提醒重新筛选用户
        if pop_trades:
            try:
                tb.warning(f"okx {pop_trades}带单员修改为保密状态,及时处理websea当前持仓", 'risk')
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=self.tel_id, content=f"okx {pop_trades}带单员修改为保密状态,及时处理websea当前持仓")
                # tb.sendmail(title, warning_mess)
            except:
                self.log.warning(f"发送报警信息失败:{traceback.format_exc()}")
            for trader in pop_trades:
                self.traders.remove(trader)
            self.traders = self._local_user.save({'users':self.traders})['users']
        if len(self.traders) <= 2:
            title = f"{strategy_name} 跟单用户数量少于2"
            warning_mess = f"{strategy_name} 跟单用户数量少于2,请重新筛选用户"
            self.send_warning(title, warning_mess)
        
        if save_mysql_deals:
            await self.connect_db()
            for data in save_mysql_deals:
                insert_sql = f"INSERT INTO okx_teacher_deals (teacher_id, teacher_nick, symbol, side, vol, pos, open_ts, ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.executemany(insert_sql, data)
            self.log.info(f"更新数据:{save_mysql_deals} {len(save_mysql_deals)}条数据")
            self.db.commit()  # 提交事务
            self.db.close()   # 关闭数据库连接
            # 去下单
            await self.to_deal(save_mysql_deals)
        else:
            self.log.info("无新增交易记录")
    
    async def to_deal(self, save_mysql_deals):
        for data in save_mysql_deals:
            teacher_id, teacher_nick, symbol, side, vol, pos, open_ts, ts = data[0]
            # 先判断是否已经止盈
            if self.target_profit_deal.get(teacher_nick, []):
                target_symbol, target_vol, target_price, target_side = self.target_profit_deal.get(teacher_nick, [])
                if symbol == target_symbol and side == target_side:
                    mess = f"{teacher_nick}用户最新成交数据{symbol} vol:{vol} price:{price} side:{side},已触发止盈平仓"
                    self.log.info(mess)
                    await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=self.tel_id, content=mess)
                    continue
                
            od_type = ocw.OrderType.buy_market if side == 'buy' else ocw.OrderType.sell_market
            try:
                # depth = await self.ws_rest.fetch_depth(symbol)    # 新接口写法
                depth = await self.ws_rest.get_depth(symbol, quan=True)
                price = depth.asks[0].price*(1+self.slip) if side == 'buy' else \
                        depth.bids[0].price*(1-self.slip)
                # price = depth['asks'][0][0]*(1+self.slip) if side == 'buy' else \
                #         depth['bids'][0][0]*(1-self.slip) # 新接口写法
                precision = await self.get_precision(symbol)
            except:
                self.log.warning(f"get_depth报错{symbol}交易对:{traceback.format_exc()}")
                continue
            
            # 根据账户资金调整下单金额
            # vol = vol*self.follow_ratio   按照固定比例跟单
            balance = await self.ws_rest.get_walletList(is_full=2)
            fee_balance = balance.avail*self.each_ratio
            size = self.symbol_size.get(symbol, 1)
            self.log.info(f"399数据验证:{fee_balance} price:{price} size:{size}")
            vol = int(fee_balance/price/size)    # 下单数量是张！！！
            
            # 判断是开仓还是平仓
            open_or_close = 'open'
            res_side = 'buy' if side == 'sell' else 'sell'
            if abs(vol) < self.all_pos.get(symbol, {}).get(res_side, 0):
                open_or_close = 'close'
            
            self.log.info(f"跟单{teacher_id}下单 数据来源:{(teacher_id, symbol, side, vol, pos, open_ts, ts)}\n{symbol} 对冲价格:{price} 对冲数量:{abs(vol)} 方向:{side}")
            try:
                t1 = time.time()*1000
                if open_or_close == 'open':
                    res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                        price=price, amount=abs(vol), \
                                                        precision=precision)
                else:
                    res = await self.ws_rest.order_create(symbol=symbol, od_type=od_type, \
                                                        price=price, amount=abs(vol), \
                                                        precision=precision, contract_type='close')
                deal_mess = f"跟单{teacher_id}下单 {symbol} 对冲价格:{price} 对冲数量:{abs(vol)} 方向:{side} 下单延时:{round(time.time()*1000-t1, 2)}ms 回报:{res}"
                self.log.info(f"下单回报:{deal_mess}")
            except:
                title = f"{strategy_name} 下单失败"
                warning_mess = f"{strategy_name} 跟单{teacher_id}下单 {symbol} 对冲价格:{price} 对冲数量:{vol} 方向:{side} 报错:{traceback.format_exc()} 检查策略、账户、持仓"
                await self.send_warning(title, warning_mess)
                self.log.warning(f"{warning_mess}")


def main(path='ok_follow_deal_config.py'):
    config = __import__(path.split(".py")[0])
    Strategy(config).run()



if __name__ == '__main__':
    main()






