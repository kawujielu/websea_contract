'''
    用户交易行为分析:
    1、每次启动获取全量数据
    2、可以按照用户id查询分析
'''

'''
    监控websea合约资金费率实际收取金额
    版本信息:v1.0.0
    日期:2025-10-20
    作者:sky

    每日早8点统计全市场资费收取情况
    每8小时推送最近8小时的资费收取情况
'''
import os
import sys
import datetime
import traceback
import asyncio
import time
import datetime
import numpy as np
import pandas as pd
import asyncio
import motor.motor_asyncio

sys.path.append("../..")
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract

mm_id = ['18','19','20','21','22','476515','465880']
buy_sell_side = {'1':'开多', '2':'开空', '3':'平多', '4':'平空'}


class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.filter_users = []
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
    
    async def on_first(self):
        self.log.info("=======first======")
        script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        log_file = f"log/{script_name}.log"
        self.log.add(log_file, rotation="100 MB", retention=10)
        await self.update_tag_user()  # 查询模拟金用户
        self.log.info(f"初始化完成")
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
    
    # 更新模拟金用户
    async def update_tag_user(self):
        temp_tag_list = []
        last_page = 0
        while 1:
            for i in range(20):
                num = i+1
                try:
                    if last_page == 0:
                        data = await self.rest.fetch_tag_list(tag=f'E{num}', page=1, page_size=1000)
                        tag_list_page = data['pager']['total_page']
                        for i in data['data']:
                            if i not in temp_tag_list:
                                temp_tag_list.append(i)
                    begin = 2 if last_page == 0 else last_page
                    for n in range(begin, tag_list_page+1):
                        last_page = n
                        data = await self.rest.fetch_tag_list(tag=f'E{num}', page=n, page_size=1000)
                        for i in data['data']:
                            if i not in temp_tag_list:
                                temp_tag_list.append(i)
                        await asyncio.sleep(0.5)
                    continue
                except:
                    self.log.info(traceback.format_exc())
                    await asyncio.sleep(5)
            break
                
        tag_users = {}
        for data in temp_tag_list:
            if data['tag'] in tag_users and data['user_id'] not in tag_users[data['tag']]:
                tag_users[data['tag']].append(data['user_id'])
            else:
                tag_users[data['tag']] = [data['user_id']]
        self.log.info(f"E组用户:{tag_users}")
        for tag, users in tag_users.items():
            self.filter_users += users
        self.log.info(f"E组用户:{self.filter_users}")

    async def main(self):
        # AsyncIOMotorClient 代表一个mongod进程，或者它们的一个集群。显式创建这些客户端对象之一，将其连接到正在运行的 mongod，并在应用程序的整个生命周期中使用它。
        client = motor.motor_asyncio.AsyncIOMotorClient('mongodb://future-ro:qVy8HB31R5Wc7fHU@10.60.99.85:27020/')
        # AsyncIOMotorDatabase：每个 mongod 都有一组数据库（磁盘上不同的数据文件集）。可以从客户端获取对数据库的引用。
        db = client.exchange
        
        info = await db.list_collection_names()
        print(f"集合列表:{info}")
        
        # AsyncIOMotorCollection：一个数据库有一组集合，其中包含文档；从数据库中获得对集合的引用。
        collection = db.real_contract_deal

        usr_ids = ['462703','543796','605186','608922','583268','602886','607514','597971','584067','605434']
        begin_date = "2025-11-01"   # 查询此时间后的数据
        end_date = "2025-12-01"


        profit_list = []
        for usr_id in usr_ids:
            t1 = time.time()
            # 查找takerUser或makerUser至少一个字段包含468958值。
            if usr_id in self.filter_users:
                self.log.info(f"用户{usr_id}是模拟金用户")
                continue
            query = {
                "$or": [
                    {"takerUser": {"$regex": usr_id}},
                    {"makerUser": {"$regex": usr_id}}
                ]
                #"symbol": "CFX-USDT",
            }

            # 查询某2个字段的值不同时在指定列表中的数据
            # query = {
            #     "$or": [
            #         {"takerUser": {"$nin": mm_id}},
            #         {"makerUser": {"$nin": mm_id}},
            #         # {"ts": {"$gt": 1745856000}},    # 过滤ts字段所有大于1743436800的数据
            #     ]
            # }
            
            # find()查找所有内容, sort()按照dealId字段-1降序排序, limit()限制返回条数
            result = collection.find(query).sort("dealId", -1)  #.limit(1000)
            # result = collection.aggregate(pipeline, allowDiskUse=True)    # pipeline必需是list
            # result = await collection.find_one()  # 查询最新一条数据
            num = 0
            profit_sum = 0
            user_list = []
            async for deal in result:
                #print(deal)
                #return
                symbol = deal['symbol']
                date = datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')
                temp_date = date.split(' ')[0]
                if temp_date < begin_date or temp_date > end_date:
                    continue
                num += 1
                #continue
                taker = f"taker:{deal['takerUser']}"
                maker = f"maker:{deal['makerUser']}"
                price = float(deal['price'])
                amount = float(deal['amount'])
                face_value = deal['takerFaceValue']
                if deal['takerUser'] == usr_id:
                    side_type = deal['takerBuyOrSell']
                    fee = deal['takerFee']
                    profit = deal['takerProfitLoss']
                    multiple = deal['takerMultiple']
                if deal['makerUser'] == usr_id:
                    side_type = deal['makerBuyOrSell']
                    fee = deal['makerFee']
                    profit = deal['makerProfitLoss']
                    multiple = deal['makerMultiple']
                side = buy_sell_side[str(side_type)]
                taker_side = deal['takerBuyOrSell']
                maker_side = deal['makerBuyOrSell']
                profit_sum += float(profit)
                # print(taker, maker, price, amount, taker_side, maker_side)
                # print(f"{deal['takerUser']} takerOrderSource:{deal['takerOrderSource']} takerBuyOrSell:{deal['takerBuyOrSell']}\ndeal['makerUser'] makerOrderSource:{deal['makerOrderSource']} makerBuyOrSell:{deal['makerBuyOrSell']}")
                # if deal['symbol'] == 'COAI-USDT':
                try:
                    last_side
                except:
                    last_side = 1
                # if side != last_side:
                #     print(f"{datetime.datetime.utcfromtimestamp(deal['ts']).strftime('%Y-%m-%d %H:%M:%S')} {deal['symbol']} {side} 成交价:{price} 数量:{amount} 盈亏:{round(float(profit), 2)} 手续费:{round(float(fee), 2)} {taker} {maker}")
                last_side = side
                ll = [usr_id, date, symbol, side, float(profit), float(price), float(amount), float(face_value), int(multiple)]
                user_list.append(ll)
            # print(f"{usr_id}用户共{num}条数据, 总盈亏:{int(profit_sum)}")
            
            # 生成dataframe
            df = pd.DataFrame(user_list, columns=['userid', 'tsText', 'symbol', 'buy_sell', 'profit_loss', 'price', 'amount', 'face_value', 'multiple'])
            profit, res = await self.fenxi(df)
            t2 = time.time()
            rank = usr_ids.index(usr_id) + 1
            print(f"第{rank}个用户,耗时:{round((t2-t1), 1)}s")
            if profit > 10000:
                profit_list.append([usr_id, profit, res])

        # 按照profit字段从大到小排序
        for usr_id, profit, res in sorted(profit_list, key=lambda x: x[1], reverse=True):
            print(res)


    async def fenxi(self, df):
        
        # 时间列处理 & 排序
        df['tsText'] = pd.to_datetime(df['tsText'])
        df = df.sort_values('tsText').reset_index(drop=True)
        # 选择tsText字段大于2025-07-12号的
        # df = df[df['tsText'] > '2025-07-12']

        # 初始化队列和结果
        long_opens = []   # 存储开多时间
        short_opens = []  # 存储开空时间
        durations = []    # 存储持仓时长
        profit_loss_sum = []   # 盈亏加总
        deal_amt_list = []   # 成交金额

        # 遍历处理每一行
        last_action = ''
        temp_profit_sum = 0
        for idx, row in df.iterrows():
            action = row['buy_sell']
            ts = row['tsText']
            
            if action == '开多':
                if ts not in long_opens and not long_opens:
                    long_opens.append(ts)
                if '平多' in last_action:
                    profit_loss_sum.append([row['symbol'], int(temp_profit_sum)])
                    deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
                    temp_profit_sum = 0
                last_action = action
            elif action == '平多':
                temp_profit_sum += row['profit_loss']
                if long_opens:
                    open_ts = long_opens.pop(0)
                    durations.append(ts - open_ts)
                last_action = action
            elif action == '开空':
                if ts not in short_opens and not short_opens:
                    short_opens.append(ts)
                if '平' in last_action:
                    profit_loss_sum.append([row['symbol'], int(temp_profit_sum)])
                    deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
                    temp_profit_sum = 0
                last_action = action
            elif action == '平空':
                temp_profit_sum += row['profit_loss']
                if short_opens:
                    open_ts = short_opens.pop(0)
                    durations.append(ts - open_ts)
                last_action = action
        # profit_loss_sum.append(int(temp_profit_sum))
        try:
            deal_amt_list.append(row['price'] * row['amount'] * row['face_value'])
        except:
            pass

        # 结果统计
        mess = ""
        trade_num = df.shape[0]     # 交易笔数
        num_trades = len(profit_loss_sum)  # len(durations) 这里差别很大，需要优化算法
        if num_trades == 0:
            print("没有成交记录")
            return 0, 0
        else:
            # 统计周期内的总盈利
            begin_time = str(df.iloc[0]['tsText'])
            end_time = str(df.iloc[-1]['tsText'])
            mess += (f"{df.iloc[0]['userid']}统计周期{begin_time}——{end_time}\n")
            mess += (f"成交笔数:{trade_num}\n")
            mess += (f"开平仓次数: {len(profit_loss_sum)}\n")   #  盈亏次数: {len(profit_loss_sum)}")
            # 分币对盈亏
            profit_loss_dict = {}
            for i in profit_loss_sum:
                if i[0] in profit_loss_dict:
                    profit_loss_dict[i[0]] += i[1]
                else:
                    profit_loss_dict[i[0]] = i[1]
            symbol_mess = ""
            for i in profit_loss_dict:
                symbol_mess += f"{i}: {profit_loss_dict[i]}\n"
            profit_loss_sum = [i[1] for i in profit_loss_sum]
            mess += (f"总盈亏: {sum(profit_loss_sum)}\n")
            mess += (f"分币对盈亏:{symbol_mess}\n")
            mess += (f"平均每笔盈亏: {round(sum(profit_loss_sum) / num_trades, 2)}\n")
            mess += (f"最大盈利: {max(profit_loss_sum)}\n")
            mess += (f"最大亏损: {min(profit_loss_sum)}\n")
            # 计算胜率
            profit_num = sum(pl > 0 for pl in profit_loss_sum)
            loss_num = sum(pl <= 0 for pl in profit_loss_sum)
            win_rate = round(profit_num / num_trades * 100, 2)
            mess += (f"胜率: {win_rate}%\n")
            # 计算盈亏比 
            profit_sum = sum(pl for pl in profit_loss_sum if pl > 0)
            loss_sum = sum(pl for pl in profit_loss_sum if pl < 0)
            try:
                profit_loss_ratio = round((profit_sum / profit_num) / abs(loss_sum / loss_num), 2)
            except:
                profit_loss_ratio = None
            mess += (f"盈亏比: {profit_loss_ratio}\n")
            # 计算最大持仓时长
            max_duration = max(durations)
            mess += (f"最大持仓时长: {max_duration}\n")
            # 计算最短持仓时长
            min_duration = min(durations)
            mess += (f"最短持仓时长: {min_duration}\n")
            # 计算最大连续盈利笔数
            max_win_num = 0
            cur_win_num = 0
            for pl in profit_loss_sum:
                if pl > 0:
                    cur_win_num += 1
                    if cur_win_num > max_win_num:
                        max_win_num = cur_win_num
                else:
                    cur_win_num = 0
            mess += (f"最大连续盈利笔数: {max_win_num}")
            # 计算最大连续亏损笔数
            max_loss_num = 0
            cur_loss_num = 0
            for pl in profit_loss_sum:
                if pl < 0:
                    cur_loss_num += 1
                    if cur_loss_num > max_loss_num:
                        max_loss_num = cur_loss_num
                else:
                    cur_loss_num = 0
            mess += (f"最大连续亏损笔数: {max_loss_num}\n")
            # 计算单笔最大下单金额
            df['deal_amt'] = df['price'] * df['amount'] * df['face_value']
            max_deal_amt = df['deal_amt'].max()
            mess += (f"单笔最大下单金额: {int(max_deal_amt)}\n")
            # 计算平均时长（转换为总秒数再计算）
            total_seconds = sum(d.total_seconds() for d in durations)
            avg_seconds = total_seconds / num_trades
            avg_duration = pd.Timedelta(seconds=avg_seconds)
            mess += (f"平均持仓时长: {avg_duration}\n")
            
            # 计算交易频率 日均/周均/月均交易次数
            date_end = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            date_begin = datetime.datetime.strptime(begin_time, "%Y-%m-%d %H:%M:%S")
            days = (date_end-date_begin).days
            days = days if days > 0 else 1
            weeks = int(days/7) if int(days/7) > 0 else 1
            months = int(days/30) if int(days/30) > 0 else 1
            total_trades = len(profit_loss_sum)
            daily_trades = round(total_trades/days, 2)        # 日均
            weekly_trades = daily_trades*7
            monthly_trades = daily_trades*30
            # weekly_trades = int(total_trades/weeks)      # 周均
            # monthly_trades = int(total_trades/months)    # 月均
            mess += (f"日均交易次数: {daily_trades} 周均:{weekly_trades} 月均:{monthly_trades}\n")
            
            # 杠杆使用率： (合约用户) 平均或最大使用的杠杆倍数
            avg_leverage = round(df['multiple'].mean(), 2)
            max_leverage = df['multiple'].max()
            mess += (f"平均杠杆倍数: {avg_leverage} 最大杠杆倍数: {max_leverage}\n")
            
            # 夏普比率： (平均收益率 - 无风险利率) / 收益率标准差 (衡量风险调整后收益，需要计算一段时间内的收益率序列)
            risk_free_rate = 0.02 / 252  # 年化2%，换算为日收益率（252个交易日）
            return_list = [profit_loss_sum[i]/deal_amt_list[i] for i in range(len(profit_loss_sum))]
            
            # df_pro = df[df['profit_loss'] != 0]
            # df_pro['return'] = df_pro['profit_loss'] / df_pro['deal_amt']
            # mean_return = statistics.mean(return_list)
            # std_return = statistics.stdev(return_list)
            # sharpe_ratio = (mean_return - risk_free_rate) / std_return if std_return != 0 else None
            # print(f"夏普比率: {round(sharpe_ratio, 2)}")
            
            # 胜率盈亏比综合指标： 期望收益 = 胜率 * 平均盈利 - (1 - 胜率) * 平均亏损
            # 算法一
            # wins = df[df['profit_loss'] > 0]
            # losses = df[df['profit_loss'] < 0]
            # win_rate = len(wins) / len(df)
            # avg_profit = wins['profit_loss'].mean() if not wins.empty else 0
            # avg_loss = abs(losses['profit_loss'].mean()) if not losses.empty else 0
            # 算法二
            wins = np.array([pl for pl in profit_loss_sum if pl > 0])
            losses = np.array([pl for pl in profit_loss_sum if pl < 0])
            avg_profit = wins.mean()
            avg_loss = abs(losses.mean())
            # print(win_rate, avg_profit, avg_loss)
            expected_return = win_rate/100 * avg_profit - (1 - win_rate/100) * avg_loss
            mess += (f"期望收益:{round(expected_return, 2)}\n")
            
            # 最大回撤： 在选定周期内，账户净值从最高点到最低点的最大跌幅百分比
            # 仓位大小： 单笔交易金额占总资产的比例（平均或最大值）
            # 没有用户账户余额无法计算
            # 手续费占比： 总手续费 / 总交易额 * 100%
            return sum(profit_loss_sum), mess


def main():
    strategy().run()


if __name__ == '__main__':
    main()





