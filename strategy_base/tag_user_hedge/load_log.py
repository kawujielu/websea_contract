
#!/usr/bin/env python3
import sys
import time
import datetime
import pytz
import traceback

def tail_log_file(file_path, code):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 首次读取直接输出全部内容
            buy_vol = 0
            buy_amt = 0
            sell_vol = 0
            sell_amt = 0
            for line in f:
                if code in line.strip():
                    mess = eval(line.strip().split(code)[1])
                    if 'FILLED' in mess['status'] and mess['market'] == 'BCH-USDT' and 'updateTime' in mess:
                        
                        ts = mess['updateTime']
                        utc_time = datetime.datetime.utcfromtimestamp(ts / 1000)
                        cst_tz = pytz.timezone('Asia/Shanghai')
                        cst_time = utc_time.replace(tzinfo=pytz.utc).astimezone(cst_tz)
                        date = cst_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
                        amt = mess['cumfilledVol'] if mess['cumfilledVol'] != '' else 0
                        try:
                            amt = float(amt)
                            price = round(amt/mess['amount'], 2)
                        except:
                            print(traceback.format_exc())
                            print(mess)
                        print(date, mess['side'], price, mess['amount'])
                        if price == 0:
                            print(mess)
                        {'cumfilledVol': 4.25, 'orderType': 'market', 'lastfilledSize': 0.01, 
                         'amount': 0.01, 'side': 'BUY', 'origQty': 0.01, 'cumfilledSize': 0.01, 
                         'userId': 47383786, 'market': 'BCH-USDT', 'uid': 100022, 'cumfilledVol': '4.25', 
                         'price': '', 'lastfilledprice': '425.3', 'tag': 'Z3', 'status': 'FILLED', 
                         'direction': 'SPACE'}
                        if mess['side'] == 'BUY':
                            buy_vol += mess['amount']
                            buy_amt += amt
                        elif mess['side'] == 'SELL':
                            sell_vol += mess['amount']
                            sell_amt += amt
            print(f"买入成交量：{buy_vol}, 买入成交额：{buy_amt}")
            print(f"卖出成交量：{sell_vol}, 卖出成交额：{sell_amt}")
            profit = sell_amt - buy_amt
            print(f"收益：{profit}")
            # 循环读取后续内容
    except FileNotFoundError:
        print(f"错误：文件 {file_path} 不存在")
    except PermissionError:
        print(f"错误：没有权限读取 {file_path}")
    except KeyboardInterrupt:
        print("\n日志监控已停止")

if __name__ == "__main__":
    
    log_file = 'hedge_on_pos.log'
    code = 'ws成交推送数据:'
    tail_log_file(log_file, code)

