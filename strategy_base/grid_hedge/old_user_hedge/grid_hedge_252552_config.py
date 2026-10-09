
config = {'base_token': '343d6405c5bceb820d47c502a3357775',
          'base_secret': '9lbvo1ti1jlznd3czjwp',
          'base_uid': 19,
          #'hedge_token': 'e1934f97-f831-418e-b154-1ec5a0415f9a',
          #'hedge_secret': 'A2D8A15D0DE4B59BD7B270A34BB1273C',
          #'passphrase': 'usRolUVNuyBbEF@7 '
          'hedge_token': 'b1c2b6e5-daf6-4ce4-8198-96aee61c3b17',
          'hedge_secret': 'E26B8728F3F85FED0451AB50F3CD1A25',
          'passphrase': 'Tbtb794972.'
          }

# symbol = []
# id = 483735     # 471973
leads = {252552: 81152429}
tag = 'Z2'  # 大户
hedge_pos_ratio = 1   # 对冲持仓量的百分比
hedge_step = 1          # 分5次去对冲
price_ratio = 0.1       # 表示开仓价距离爆仓价格10%,至少开10倍杠杆以上才可能在波动10%以内爆仓
hedge_price_ratio = 0.4 # 对冲价格区间占爆仓价格范围的比例,0.4表示若价格下跌20%爆仓,则价格下跌20%*0.4=8%时,
                        # 完成全部5次对冲,每次对冲价格是价格每下跌8%/5=1.6%对冲一次
hedge_amt_limit = 100000  # 对冲金额限制,单位是usdt
slip = 0.005            # 下单滑点

use_balance_ratio = 1   # 0.5表示使用账户余额的50%作为对冲资金
hedge_ratio = 0.2       # 0.1表示每次对冲用户交易量的10%
#hedge_balance_limit = 1000  # 对冲资金限制,单位是usdt
max_follow_amt = 10000   # 最大对冲金额
level = 10              # 对冲交易对杠杆倍数
later_ratio = 0       # 非实时对冲量占需要对冲量的比例

#each_vol = 20           # 每笔下单数量 TODO 改成根据过往5min内所有成交的中位数,因为不可能支持所有币对
total_time = 30         # 最大对冲时间
each_interval_time = 0.5  # 每次下单间隔时间1s
db_name = 'okx_hedge_acct2'  # 查询数据库的名称

