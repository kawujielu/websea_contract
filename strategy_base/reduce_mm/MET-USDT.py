
config = {
          'token': '44c614fa9131929291e4fa7325d47653000',
          'secret': 'v5eb4fwocsz4kyzejhd4',
          # 'token': '3f57905a03a32432fae784037cb44f4c', # 测试1,在跑eth做市策略
          # 'secret': 'sx4n8k2nh8hfz5gjs672',
          # 'token': '5bae7319086ca6cdc135803a5426da33', # 测试2,没有在用
          # 'secret': 'allhrtvujrbrjdvbgf73',
          'hedge_token': 'AZcNe2FG4SXf4SQDreEk98um8EtyDgHx82uPEZkdCp3ivU26mBWd0CXcrTqE0gAi',
          'hedge_secret': 'FRLuQbmdHUf5F1RubYvOgpkJn6C3q9jIcNfEVtm7ZoZTZBXZnDI1jFMxSbgOThkj',
        }

symbol = 'MET-USDT'
place_num = 40                # 买卖盘总挂单量
leverage = 2                  # 最大杠杆
default_shape = 0.75          # 价格偏移的默认范围
max_budget_utilization = 0.3  # 最大预算利用率
frequency_controller = 1      # 频率控制器
taker_fee = 0.0005             # taker手续费
maker_fee = 0.0005           # maker手续费
cancel_price_ratio = 0.001    # 超过最新成交价格千1之外的订单撤销
is_close_pos = 500000000
is_open_pos = 100000000
use_balance = 5000000


