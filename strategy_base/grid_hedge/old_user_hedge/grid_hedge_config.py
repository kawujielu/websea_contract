
config = {'base_token': '343d6405c5bceb820d47c502a3357775',
          'base_secret': '9lbvo1ti1jlznd3czjwp',
          'base_uid': 19,
          #'hedge_token': 'e1934f97-f831-418e-b154-1ec5a0415f9a',
          #'hedge_secret': 'A2D8A15D0DE4B59BD7B270A34BB1273C',
          #'passphrase': 'usRolUVNuyBbEF@7 '
          'hedge_token': 'b29736e4-6a9b-4a29-ba07-048d75703924',
          'hedge_secret': '9E0648BE0E41C21C857C93D1CF4F2588',
          'passphrase': 'Tbtb794972.'
          }

symbol = ''
id = 478061
tag = 'Z'
hedge_pos_ratio = 1     # 对冲持仓量的百分比
hedge_step = 5          # 分5次去对冲
price_ratio = 0.1       # 表示开仓价距离爆仓价格10%,至少开10倍杠杆以上才可能在波动10%以内爆仓
hedge_price_ratio = 0.4 # 对冲价格区间占爆仓价格范围的比例,0.4表示若价格下跌20%爆仓,则价格下跌20%*0.4=8%时,
                        # 完成全部5次对冲,每次对冲价格是价格每下跌8%/5=1.6%对冲一次
hedge_amt_limit = 100   # 对冲金额限制,单位是usdt
price_slip = 0.002
