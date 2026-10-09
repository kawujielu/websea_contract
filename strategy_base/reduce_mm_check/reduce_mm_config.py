
config = {
          # 'token': 'e5a9248ff0051a647fa3b3228c9868f9',
          # 'secret': 'ghg628ioeqnnof5i1n5k',
          # 'token': '3f57905a03a32432fae784037cb44f4c', # 测试1,在跑eth做市策略
          # 'secret': 'sx4n8k2nh8hfz5gjs672',
          'token': '5bae7319086ca6cdc135803a5426da33', # 测试2,没有在用
          'secret': 'allhrtvujrbrjdvbgf73',
          'hedge_token': 'AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi',
          'hedge_secret': 'FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj',
        }

symbol = 'LTC-USDT'
place_num = 50                # 买卖盘总挂单量
leverage = 2                  # 最大杠杆
default_shape = 0.75          # 价格偏移的默认范围
max_budget_utilization = 0.3  # 最大预算利用率
frequency_controller = 1      # 频率控制器???
taker_fee = 0.0004            # taker手续费
maker_fee = 0.0002            # maker手续费
cancel_price_ratio = 0.001    # 超过最新成交价格千1之外的订单撤销



