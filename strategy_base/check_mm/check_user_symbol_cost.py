'''
    查询指定用户指定交易对的资费
    版本信息:v1.0.0
    日期:2025-12-20
    作者:sky
'''

import sys
import datetime
import traceback
import asyncio

sys.path.append("../..")
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract



class strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''
    def __init__(self, id, symbol):
        super().__init__(scheduler=True, gcc=True)
        self.id, self.symbol = id, symbol
        self._initParams()         # 初始化参数
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','')
        self.rest.DEBUG = False
        self.rest.rest_timeout = 60
        
    def _initParams(self):
        """初始化参数
        """
        self.symbol_cost = {}       # 保存分币对资费
        pass
    
    async def on_first(self):
        self.log.info("=======first======")
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")

    # 查询资费
    async def get_cost(self, user_type, begin_ts, end_ts):
        last_page = 0
        cost_data = []
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_treaty_cost(symbol=self.symbol, page_size=100, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    self.log.info(f"{data['data'][0]['create_time_text']}数据量:{data['pager']}")
                    # 'pager': {'item_count': '36', 'page': '1', 'page_size': '1000', 'total_page': 1}}
                    total_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                    # print(cost_data)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, total_page+1):
                    last_page = n
                    data = await self.rest.fetch_treaty_cost(symbol=self.symbol, page=n, page_size=100, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                    await asyncio.sleep(0.2)
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
        return cost_data

    async def get_cost2(self, user_type, begin_ts, end_ts):
        last_page = 0
        cost_data = []
        while 1:
            try:
                if last_page == 0:
                    data = await self.rest.fetch_treaty_cost(page_size=100, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    self.log.info(f"{data['data'][0]['create_time_text']}数据量:{data['pager']}")
                    # 'pager': {'item_count': '36', 'page': '1', 'page_size': '1000', 'total_page': 1}}
                    total_page = data['pager']['total_page']
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                begin = 2 if last_page == 0 else last_page
                for n in range(begin, total_page+1):
                    last_page = n
                    data = await self.rest.fetch_treaty_cost(page=n, page_size=100, user_type=user_type, min_time=begin_ts, max_time=end_ts)
                    for i in data['data']:
                        if i not in cost_data:
                            cost_data.append(i)
                    await asyncio.sleep(0.2)
                break
            except:
                self.log.info(traceback.format_exc())
                await asyncio.sleep(5)
        return cost_data
    
    async def count_user_cost(self, mm_cost):
        # 按用户统计资费
        user_cost = 0
        user_cost_group_by_ts = {}
        for i in mm_cost:
            if i['create_time_text'] not in user_cost_group_by_ts:
                user_cost_group_by_ts[i['create_time_text']] = {}
            if i['symbol_name'] not in user_cost_group_by_ts[i['create_time_text']]:
                user_cost_group_by_ts[i['create_time_text']] = {i['symbol_name']: 0}
            uid = i['user_id']
            cost = float(i['settle_cost'])
            # if uid == self.id:
            user_cost_group_by_ts[i['create_time_text']][i['symbol_name']] += cost
            user_cost += cost
        mess = ""
        for ts, cost in user_cost_group_by_ts.items():
            for symbol, cost in cost.items():
                mess += f"{ts} {symbol}交易对的资费为:{cost}\n"
        print(mess)
        return user_cost
    
    ''' ==========================================================================='''
    ''' ==================================== main ================================='''
    ''' ==========================================================================='''
    async def main(self):
        # user_type = 1   # 0(全部用户) | 1(仅量化用户) | 2(仅普通用户)
        # 计算统计区间时间戳,统计最近8小时资费收取情况
        # 规则:开始时间是0点,会包含0点结算数据;结束时间是8点,不会包含8点数据
        # now_utc_8 = datetime.datetime.now() + datetime.timedelta(hours=8)
        # eight_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=self.hour_interval)
        # begin_time = eight_hours_ago.replace(minute=0, second=10, microsecond=0)
        # begin_ts = int(begin_time.timestamp())
        # end_ts = int(datetime.datetime.now().timestamp())

        # 修改str为datetime格式
        begin_time = datetime.datetime.strptime("2026-06-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        begin_ts = int(begin_time.timestamp())
        end_time = datetime.datetime.strptime("2026-07-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        end_ts = int(end_time.timestamp())
        # end_ts = int(datetime.datetime.now().timestamp())
        if self.symbol is not None:
            mm_cost = await self.get_cost(user_type=2, begin_ts=begin_ts, end_ts=end_ts)
            cost = await self.count_user_cost(mm_cost)
            print(f"用户{self.id}在{self.symbol}交易对的资费为:{cost}")
        else:
            mm_cost = await self.get_cost2(user_type=2, begin_ts=begin_ts, end_ts=end_ts)
            cost = await self.count_user_cost(mm_cost)
            print(f"用户{self.id}在{self.symbol}交易对的资费为:{cost}")
        

def main(id, symbol):
    strategy(id, symbol).run()


if __name__ == '__main__':
    try:
        symbol = sys.argv[2]
    except:
        symbol = None
    main(sys.argv[1], symbol)




