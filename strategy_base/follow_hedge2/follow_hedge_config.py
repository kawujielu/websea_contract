config = {'base_token': '7f81fa4496ec0064dda3ccbc5bd45703200',
          'base_secret': 'c8a0lwr60cb7ws3ktl86',
          #'hedge_token': 'b29736e4-6a9b-4a29-ba07-048d75703924',
          #'hedge_secret': '9E0648BE0E41C21C857C93D1CF4F2588',
          #'passphrase': 'Tbtb794972.'
          #'hedge_token': 'b1c2b6e5-daf6-4ce4-8198-96aee61c3b17',
          #'hedge_secret': 'E26B8728F3F85FED0451AB50F3CD1A25',
          #'passphrase': 'Tbtb794972.'
          'hedge_token': 'e1934f97-f831-418e-b154-1ec5a0415f9a',
          'hedge_secret': 'A2D8A15D0DE4B59BD7B270A34BB1273C',
          'passphrase': 'usRolUVNuyBbEF@7 '
          }

tag = 'Z2'
symbol = 'BTC-USDT'
symbols = ['BTC-USDT','ETH-USDT']
leads = {644676: 27527439}   # {580609: 30381215}
hedge_amt_limit = 200000   # 每次最多对冲1000u
follow_ratio = 0.1      # 对冲量比例,1是完全对冲,小于1部分对冲,在大概率能盈利的前提下可以大于1
slip = 0.003            # 滑点
liqu_risk_raito = 0.3   # 距离爆仓价波动小于30%报警

