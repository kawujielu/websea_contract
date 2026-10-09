
config = {
          # 'spot_token': '271e2179a3ea3ba6cdbdd8e3dfft1638964',       # 现货测试账户
          # 'spot_secret': 'npgsvdyw184aycu2ufai',
          # 'contract_token': '271e2179a3ea3ba6cdbdd8e3dfft1638964',   # 合约测试账户
          # 'contract_secret': 'npgsvdyw184aycu2ufai',
          'spot_token': 'dd968d7c92833fa59a792ed73fi50256045',        # 生产环境
          'spot_secret': 'u7eilfdcwaiz6yrecafn',
          'contract_token': '57b49384ee4e0b236f0f38a45fj50733190',    # 生产环境,做市账户:刷量
          'contract_secret': 'bu8qqmoyaq0o10xeepdd',
        }

spot_symbol = 'MH-USDT'
contract_symbol = 'MH-USDT'
spot_fee = 0.001              # 现货手续费
contract_fee = 0.0005         # 合约手续费
arb_ratio_limit = 0.05        # 价差超过5%套利
balance_risk = 10000          # 资金少于这个值就报警
pos_risk = 10000              # 超过持仓阈值报警
init_balance = 10000          # 现货初始usdt
init_coin = 10000             # 现货初始crv
bal_risk_ratio = 0.2          # 资金量少于初始的20%则不再开仓
deal_limit = 10000            # 单笔下单限额,超过1万u不下单

