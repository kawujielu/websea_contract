config = {'base_token': '7f81fa4496ec0064dda3ccbc5bd45703200',
          'base_secret': 'c8a0lwr60cb7ws3ktl86',
          'hedge_token': 'e1934f97-f831-418e-b154-1ec5a0415f9a',
          'hedge_secret': 'A2D8A15D0DE4B59BD7B270A34BB1273C',
          'passphrase': 'usRolUVNuyBbEF@7 '
          }

tag = 'E12'
symbol = 'ETH-USDT'
leads = {457017: 60142803}
follow_ratio = 2        # 对冲量比例,1是完全对冲,小于1部分对冲,在大概率能盈利的前提下可以大于1
slip = 0.002            # 滑点
liqu_risk_raito = 0.3   # 距离爆仓价波动小于30%报警
hedge_amt_limit = 10000 # 单笔最大对冲量
max_hedge_amt = 2000000 # 最大对冲金额
