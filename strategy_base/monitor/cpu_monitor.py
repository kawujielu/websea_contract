import os
import sys
import time
import datetime
import shutil
import traceback
import numpy as np
import psutil
import asyncio
import socket
import subprocess
sys.path.append('../..')
from utils import restclient as rc    # 对冲服务器
# from utils.restClient import RestClient as rc   # 做市服务器

server = "@kawujielu_sky 10-0-208-53服务器"
# server = "@XQ9527 10-0-209-213服务器"
# server = "@XQ9527 10-0-208-111服务器"
warning_limit = 90  # cpu、内存、磁盘、警告阈值
load_limit = 20     # 负载阈值
thr_limit = 5000    # 线程阈值
risk_limit = 3      # 连续3次超过阈值报警
loop_num = 20       # 连续检查次数

class Monitor(object):

    def __init__(self):
        self.men_num = 0
        self.cpu_num = 0
        self.file_num = {}
        self.thr_num = 0
        self.cpu_flag = {}
        # self.rc_task = rc()     # 做市服务器
        self.rc_task = rc.RestClient()    # 对冲服务器
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        self.cpu_risk_num = 0
        self.mem_risk_num = 0
        self.load_risk_num = 0
        self.thr_risk_num = 0
        self.disk_risk_num = 0
        
        self.ave_cpu_risk = []
        self.ave_mem_risk = []
        self.ave_load_1_risk = []
        self.ave_load_5_risk = []
        self.ave_load_15_risk = []
        self.ave_thr_risk = []
        
        self.last_cpu_risk_ts = 0
        self.last_mem_risk_ts = 0
        self.last_load_risk_ts = 0
        self.last_thr_risk_ts = 0
        self.last_disk_risk_ts = 0
    

    def run(self):
        self.cpuMonitor()
        self.memMonitor()
        self.diskMonitor()
        self.threadMonitor()
        self.save_py()
        res = self.get_system_load()
        res = self.get_network_usage()
        self.log(f"网络带宽使用情况:{res}")
        res = self.get_connection_stats()
        self.log(f"网络连接统计信息:{res}")
        self.log("-" * 100)
        
    def log(self, info):
        print(f"{datetime.datetime.now()} =====> {info}")

    async def send_tg(self, info):
        info = f"{server} {info}"
        await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-4893362175, content=info)

    def save_py(self):
        result = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
                if '.py' in cmdline or '.go' in cmdline:
                    result.append(cmdline)
            except:
                continue
        self.log(result)
        
    def get_system_load(self):
        """获取系统负载（1分钟、5分钟、15分钟）"""
        load_avg = os.getloadavg()
        load_1min = round(load_avg[0], 2)
        load_5min = round(load_avg[1], 2)
        load_15min = round(load_avg[2], 2)
        self.log(f"系统负载:{load_1min}, {load_5min}, {load_15min}")
        self.ave_load_1_risk.append(load_1min)
        ave_load_1_risk = round(sum(self.ave_load_1_risk[-loop_num:])/loop_num, 2)
        if ave_load_1_risk > load_limit:
            if time.time() - self.last_load_risk_ts > 60:
                self.loop.run_until_complete(self.send_tg(f"连续{loop_num}次1min负载均值达到{ave_load_1_risk} 负载过高立即处理"))
                self.last_load_risk_ts = time.time()
        else:
            # self.load_risk_num = 0
            self.ave_load_1_risk = self.ave_load_1_risk[-loop_num:]
        
    def get_network_usage(self,interface="eth0"):
        """获取网络带宽使用情况"""
        net_io = psutil.net_io_counters(pernic=True).get(interface)
        if not net_io:
            # 尝试自动检测主网络接口
            interfaces = psutil.net_if_addrs()
            interface = next((iface for iface in interfaces 
                            if not iface.startswith(('lo', 'docker', 'veth'))), 
                            list(interfaces.keys())[0] if interfaces else "eth0")
            net_io = psutil.net_io_counters(pernic=True).get(interface)
        
        if net_io:
            return {
                "interface": interface,     # 网络接口
                "bytes_sent": net_io.bytes_sent,    # 发送字节数
                "bytes_recv": net_io.bytes_recv,    # 接收字节数
                "packets_sent": net_io.packets_sent,    # 发送包数
                "packets_recv": net_io.packets_recv    # 接收包数
            }
        return None

    def get_connection_stats(self):
        """获取网络连接统计信息"""
        try:
            # 使用 ss 命令获取更详细的连接信息
            result = subprocess.run(
                ["ss", "-s"], 
                capture_output=True, 
                text=True, 
                check=True
            )
            
            # 解析 ss 命令输出
            stats = {}
            for line in result.stdout.split('\n'):
                if "Total:" in line:
                    stats["total"] = int(line.split()[1])
                elif "TCP:" in line:
                    parts = line.split()
                    stats["tcp_established"] = int(parts[2])
                    stats["tcp_closed"] = int(parts[4].strip(')'))
                    stats["tcp_orphaned"] = int(parts[6])
                    stats["tcp_synrecv"] = int(parts[8])
                    stats["tcp_timewait"] = int(parts[10])
                elif "UDP:" in line:
                    stats["udp"] = int(line.split()[1])
            return stats
        except Exception as e:
            # 回退到 psutil 方法
            connections = psutil.net_connections(kind='inet')
            return {
                "total": len(connections),
                "tcp": len([c for c in connections if c.type == socket.SOCK_STREAM]),
                "udp": len([c for c in connections if c.type == socket.SOCK_DGRAM]),
                "states": {
                    state: len([c for c in connections if c.status == state]) 
                    for state in set(c.status for c in connections)
                }
            }
    
    def cpuMonitor(self):
        # cpu监控
        try:
            data = psutil.cpu_percent(interval=1, percpu=True)
            ratio = round(np.nansum(data) / len(data), 2)
            info = f"cpu占用均值{ratio}%,最大值{max(data)}%,最小值{min(data)}%"
            self.log(info)
            self.ave_cpu_risk.append(ratio)
            ave_cpu_num = round(sum(self.ave_cpu_risk[-loop_num:])/loop_num, 2)
            if ave_cpu_num > warning_limit:
                if time.time() - self.last_cpu_risk_ts > 60:
                    self.loop.run_until_complete(self.send_tg(f"连续{loop_num}次CPU占用均值达到{ave_cpu_num}% CPU占用过高立即处理"))
                    self.last_cpu_risk_ts = time.time()
            else:
                # self.cpu_risk_num = 0
                self.ave_cpu_risk = self.ave_cpu_risk[-loop_num:]
        except:
            self.log(traceback.format_exc())

    def memMonitor(self):
        try:
            # 内存监控
            data = psutil.virtual_memory()
            total = round(data.total / 1024 / 1024 / 1024, 2)  # 总内存,单位为byte
            used = round(data.used / 1024 / 1024 / 1024, 2)  # 使用内存,单位为byte
            ratio = round(used / total * 100, 2)  # 使用比例
            info = (f"总内存:{total}G 内存占用:{used}G 使用比例:{ratio}%")
            self.log(info)
            self.ave_mem_risk.append(ratio)
            ave_mem_risk = round(sum(self.ave_mem_risk[-loop_num:])/loop_num, 2)
            if ave_mem_risk > warning_limit:
                if time.time() - self.last_mem_risk_ts > 60:
                    self.loop.run_until_complete(self.send_tg(f"连续{loop_num}次内存占用均值达到{ave_mem_risk}% MEM占用过高立即处理"))
                    self.last_mem_risk_ts = time.time()
            else:
                # self.mem_risk_num = 0
                self.ave_mem_risk = self.ave_mem_risk[-loop_num:]
        except:
            self.log(traceback.format_exc())

    def diskMonitor(self):
        # 磁盘监控
        try:
            gb = 1024 ** 3  # GB == gigabyte
            total_b, used_b, free_b = shutil.disk_usage('/')  # 查看磁盘的使用情况
            # print('总的磁盘空间: {:6.2f} GB '.format( total_b / gb))
            # print('已经使用的 : {:6.2f} GB '.format( used_b / gb))
            # print('未使用的 : {:6.2f} GB '.format( free_b / gb))
            ratio = round((used_b / gb) / (total_b / gb) * 100, 2)  # 使用比例
            info = f"磁盘使用占比:{ratio}%"
            self.log(info)
            if ratio > warning_limit:
                self.disk_risk_num += 1
                if self.disk_risk_num >= risk_limit:
                    if time.time() - self.last_disk_risk_ts > 60:
                        self.loop.run_until_complete(self.send_tg(f"{info} 磁盘占用过高立即处理"))
                        self.last_disk_risk_ts = time.time()
            else:
                self.disk_risk_num = 0
        except:
            self.log(traceback.format_exc())

    def threadMonitor(self):
        # 线程监控
        try:
            data = psutil.pids()
            allThr = 0
            data_list = []
            for thr in data:
                p = psutil.Process(thr)
                pname = p.cmdline()
                if (pname == []) or ('python' not in pname[0]):
                    continue
                name = pname[-1]
                if not name.endswith('.py'):
                    continue
                num_threads = p.num_threads()
                allThr += num_threads
                data_list.append((name, num_threads))

            data_list.sort(key=lambda a: a[-1], reverse=True)
            info = f"线程数:{allThr}"
            self.log(info)
            self.ave_thr_risk.append(allThr)
            ave_thr_risk = round(sum(self.ave_thr_risk[-loop_num:])/loop_num, 2)
            if ave_thr_risk > thr_limit:
                if time.time() - self.last_thr_risk_ts > 60:
                    self.loop.run_until_complete(self.send_tg(f"连续{loop_num}次线程数均值达到{ave_thr_risk} 线程数量过高立即处理"))
                    self.last_thr_risk_ts = time.time()
            else:
                # self.thr_risk_num = 0
                self.ave_thr_risk = self.ave_thr_risk[-loop_num:]
        except:
            self.log(traceback.format_exc())


def run():
    m = Monitor()

    while 1:
        try:
            m.run()
        except:
            m.log(traceback.format_exc())
        time.sleep(10)


if __name__ == '__main__':
    run()



