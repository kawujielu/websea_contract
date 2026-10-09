'''
    进程监控
'''
import datetime
import json
import time
import traceback
import psutil
import requests


strategyList = [
    'takeover.py','check_price_monitor.py'] #, 'single_mm.py']


def pro_exist():
    pl = psutil.pids()
    stList = []
    for i in pl:
        pname = psutil.Process(i).cmdline()
        if pname != [] and pname[-1].endswith('.py'):
            stList.append(pname[-1])
    print(stList)
    for i in strategyList:
        current_time = datetime.datetime.now()
        if i in stList:
            print(f"{current_time} {i} 策略正常运行")
            continue
        else:
            mess = f"{i}策略停止运行,立即查看!!!"
            warning(mess, '策略停止')
            time.sleep(0.2)


def warning(content, contractSymbol='', method='normal'):
    larkDic = {
        'normal': 'https://open.feishu.cn/open-apis/bot/v2/hook/bad3eefd-381c-409e-927d-4900ff58850a',
    }
    if method in larkDic:
        url = larkDic.get(method)
        # ts = await self.timestamp()
        ts = datetime.datetime.now()
        content = f"==={ts} 报警 {contractSymbol}===\n{content}\n"
        headers = {"Content-Type": "application/json ;charset=utf-8 "}
        msg = {"msg_type": "text",
            "content": {"text": content}
            }  # 飞书
        try:
            requests.post(url, headers=headers, data=json.dumps(msg))
        except:
            print(f'飞书报错 {traceback.format_exc()}')
            return


if __name__ == '__main__':
    while 1:
        try:
            pro_exist()
            print('-' * 100)
        except:
            try:
                print(f'程序报错:{traceback.format_exc()}')
            except:
                pass
        time.sleep(120)

