import psutil as psu
import sys
import os
import signal
import time
import traceback

rootPath = '/home/ubuntu/strategy_base/strategy/ok_follow_deal'
serverRootPath = '/usr/local/server'


def _getTargetPath(target):
    if 'DataPushServer' in target:
        path = serverRootPath
        return path, '', '', ''
    symbol, *middle, strategy = target.split('.py')[0].split('_')
    path = os.path.join(rootPath, symbol, strategy)
    return path, symbol, strategy, middle


def _stopStrategy(target):
    path, _, strategy, middle = _getTargetPath(target)
    sys.path.append(os.path.join(rootPath, f"public/{strategy}"))
    module = strategy if len(middle) == 0 else f"{strategy}{middle[0].title()}"
    task = __import__(module)
    config = [i for i in os.listdir(path) if '.yaml' in i or 'config' in i or '.json' in i][0]
    try:
        task._stop(os.path.join(path, config))
    except:
        print(traceback.format_exc())


def _getPidList():
    pids = psu.pids()
    pidNames = {}
    for i in pids:
        try:
            name = psu.Process(i).cmdline()
            if name != []:
                key = name[-1].split('/')[-1]
                pidNames.setdefault(key, [])
                pidNames[key].append(i)
        except:
            continue
    return pidNames


def stop(target, pidList=None):
    pidList = _getPidList() if pidList is None else pidList
    exist = True
    try:
        targets = pidList[target]
        for i in targets:
            os.kill(i, signal.SIGKILL)
        print(f'stopped {target} successfully!')
    except:
        exist = False
        print(f'stopped {target} failed! error: {traceback.format_exc()}')
    try:
        _stopStrategy(target)
    except:
        # print(traceback.format_exc())
        pass
    return exist


def start(target, force=False, log=False):
    pidList = _getPidList()
    if (target not in pidList) or (force):
        path, *_ = _getTargetPath(target)
        print(path, target)
        try:
            if 'DataPushServer' in target:
                if log:
                    os.system(f"cd {path} && nohup python -u {target} >> {target.replace('.py', '.log')} 2>&1 &")
                else:
                    os.system(f"cd {path} && nohup python -u {target} > {target.replace('.py', '.log')} 2>&1 &")
            else:
                if log:
                    os.system(f"cd {path} && nohup python -u {target} >> {target.replace('.py', '.log')} 2>&1 &")
                else:
                    os.system(f"cd {path} && nohup python -u {target} > /dev/null 2>&1 &")
            print(f'started {target} successfully!')
        except:
            print(f"cannot find {target}")
    else:
        print(f'{target} is existed, started failed!')


def restart(target, force=False, log=False):
    if 'DataPushServer' in target:
        pidList = _getPidList()
        start(target, force=True, log=log)
        time.sleep(10)
        stop(target, pidList=pidList)
        return

    exist = stop(target)
    if force:
        start(target, log=log)
    else:
        if exist:
            start(target, log=log)


'''--------------------------------------------------------'''


def stopAll(keyword):
    pidList = _getPidList()
    targets = [target for target in pidList if keyword in target]
    exist = []
    for target in targets:
        b = stop(target)
        if b:
            exist.append(target)
    return exist


def startAll(targets, sleepTime=0):
    for target in targets:
        start(target)
        time.sleep(sleepTime)
    print(targets)


def restartAll(keyword, force=False, sleepTime=0, log=False):
    pidList = _getPidList()
    targets = [target for target in pidList if keyword in target]
    exist = []
    for target in targets:
        b = stop(target)
        if b:
            start(target, log=log)
            exist.append(target)
            time.sleep(sleepTime)

    print(exist)


'''-----------------------------------------------------------'''

