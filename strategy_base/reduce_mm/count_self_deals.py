
import os
import datetime
import pytz
import traceback


def read_log_files(directory_path, file_name, symbol, role, begin_time, end_time):
    try:
        # 验证目录是否存在
        if not os.path.isdir(directory_path):
            raise ValueError(f"目录不存在: {directory_path}")

        # 遍历目录下所有文件
        for filename in os.listdir(directory_path):
            if filename == file_name:
                filepath = os.path.join(directory_path, filename)

                # 逐行读取文件内容
                begin_ts = convert_to_timestamp(begin_time)
                end_ts = convert_to_timestamp(end_time)
                role_deal_vol = 0
                not_role_deal_vol = 0
                with open(filepath, 'r', encoding='utf-8') as file:
                    for line_number, line in enumerate(file, 1):
                        if '启动' in line:
                            continue
                        line_mess = line.strip().split('>>>>> ')[1]
                        deal = eval(line_mess)
                        # {'exchange': 'websea', 'symbol': 'ETH-USDT', 'id': 'BL4765151749285538385MJ1DFJ', 
                        #  'clientOrderId': '', 'price': 2486.22, 'stopPrice': None, 'triggerPrice': None, 
                        #  'amount': 8.05, 'side': 'buy', 'type': 'limit', 'status': 'closed', 
                        #  'leverage': None, 'timeInForce': None, 'postOnly': None, 'reduceOnly': False, 
                        #  'marginMode': None, 'average': None, 'filled': 8.05, 'cost': None, 'remaining': 0, 
                        #  'takeProfitPrice': None, 'stopLossPrice': None, 'fee': None, 'dealRole': 100022, 
                        #  'timestamp': 1749285543629, 'messageType': 'message', 'info': 
                        #  {'amount': 8.05, 'dealRole': 100022, 'filled': 8.05, 
                        #   'orderId': 'BL4765151749285538385MJ1DFJ', 'orderType': '1', 'price': '2486.22', 
                        #   'side': 1, 'status': 1, 'symbol': 'ETH-USDT', 'timestamp': 1749285543629, 
                        #   'tradeAmt': 8.05}}
                        if deal['symbol'] == symbol and deal['timestamp'] > begin_ts and deal['timestamp'] < end_ts:
                            filled = deal['filled'] if deal['side'] == 'buy' else -deal['filled']
                            if deal['dealRole'] in role:
                                role_deal_vol += filled
                            else:
                                not_role_deal_vol += filled
                print(f"{begin_time} 到 {end_time} 之间\n{symbol}交易对\n对手方{role}成交量: {round(role_deal_vol, 2)}\n对手方非{role}成交量: {round(not_role_deal_vol, 2)}")
    except:
        print(f"发生错误: {traceback.format_exc()}")

def convert_to_timestamp(time_str):
    eastern = pytz.timezone('Asia/Shanghai')
    dt = eastern.localize(datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
    return int(dt.timestamp() * 1000)

if __name__ == "__main__":
    target_dir = "/home/ubuntu/strategy_base/strategy/reduce_mm/log"  # 替换为实际目录路径
    file_name = "deals.log"
    symbol = "ETH-USDT"
    role = [100022]               # 对手方uid
    begin_time = "2025-06-07 16:50:00"  # 开始时间
    end_time = "2025-06-07 17:00:00"    # 结束时间
    read_log_files(target_dir, file_name, symbol, role, begin_time, end_time)

