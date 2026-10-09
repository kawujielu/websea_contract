
config = {
          #'token': '3f57905a03a32432fae784037cb44f4c', #'5bae7319086ca6cdc135803a5426da33', # 测试环境
          #'secret': 'sx4n8k2nh8hfz5gjs672', #'allhrtvujrbrjdvbgf73',
          'swap_token': '57b49384ee4e0b236f0f38a45fj50733190',    # 生产环境
          'swap_secret': 'bu8qqmoyaq0o10xeepdd',
          'spot_token': 'dd968d7c92833fa59a792ed73fi50256045',   # 现货，获取价格信息
          'spot_secret': 'u7eilfdcwaiz6yrecafn',
          }

symbol = 'MH-USDT'
follow_symbol = 'eth/usdt'
place_num = 50              # 档位数
re_mm_tatio = 0.008        # 价格波动不超过0.04%的比例,则不重新布单
adjust_volatility = 0.1    # 调整波动幅度
max_deviation = 0.5         # 标记价格偏离持仓均价的最大比例
default_shape = 0.75        # 价格偏移的默认范围
dict_max_size = 70          # k线最大保存数量
leverage = 2                # 最大杠杆
buy_budget_adj = 0.05       # buy每次下单预算占全部资金的比例  
sell_budget_adj = 0.05      # sell每次下单预算占全部资金的比例
frequency_controller = 1    # 频率控制器
max_budget_utilization = 0.3  # 最大预算利用率

step_ratio = 0.1            # 下单价格区间占标记价格的比例
bid_offset = 0.01           # 买盘起始价格
ask_offset = 0.01           # 卖盘起始价格


