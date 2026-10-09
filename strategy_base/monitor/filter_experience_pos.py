
import sys
import traceback
import time
import asyncio
sys.path.append('../..')
from client.env_pro.rest.websea import contract
from utils import restclient as rc
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.websea.contract_pro import WebseaContractNew as Contract  # 新合约接口


class takeOverStrategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self._load_config()         # 读取配置
        self.symbol_pos = {}        # 交易对持仓
        self.symbol_price = {}      # 交易对价格
        self.symbol_size = {}       # 交易对合约单位
        self.all_symbols = []       # 所有交易对
        self.pos_limit = 50000      # 持仓金额阈值
        self.fund_rate = 0.001      # 资金费率调整值
        self.rc_task = rc.RestClient()

    def _load_config(self):
        self.rest = Contract('c1cf4185b2bed317aeb6e6674491fbef','',dev=False)     # 修改资费的token
        self.old_rest = contract.WebseaContract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 旧合约
        self.rest1 = Contract('343d6405c5bceb820d47c502a3357775','9lbvo1ti1jlznd3czjwp')    # 近盘口
        self.rest2 = Contract('f92b5404bb7814170ab65caf3e3541c3','b3cihitxwlxpjoltntb1')    # 远盘口
        self.rest3 = Contract('e5a9248ff0051a647fa3b3228c9868f9','ghg628ioeqnnof5i1n5k')    # 补挡位
        self.rest4 = Contract('84ad0e771f0f5f2df264e56d0ecddef9','9cvay7ieczqp01lwy0zr')    # 刷量
        self.rest5 = Contract('3a4f68d2dce6a863ae7a0eb271cf6820','zvjnkwctuwtzhgstdgwz')    # 新策略
        self.rest6 = Contract('44c614fa9131929291e4fa7325d47653000','v5eb4fwocsz4kyzejhd4') # 压盘口策略
        self.rest7 = Contract('5accd1fb1d03e26f299a4297d9q52646135','nyuc69aljff4g297uj99') # MH合约
        self.rest8 = Contract('9c09cc01ddb22536b80cf5526e0e0d08','r8b6uw0vojwtnpzuevt3',dev=False)    # 防守策略

    async def on_first(self):
        self.log.info("=======first======")
        await self.get_symbol_unit()        # 内盘合约单位
        await self.main()

    async def on_timer(self):
        self.log.info("=======timer======")
        self.schedule.add_job(self.main, CronTrigger(minute="20"))    # 每20min执行一次

    async def get_symbol_unit(self):
        # 查询合约单位
        while True:
            try:
                # 获取所有交易对
                res = await self.old_rest.get_symbols(quan=True)
                # print(f"websea交易对:{res}")
                for s in res:
                    self.symbol_size[s.symbol] = s.contract_size
                    self.all_symbols.append([s.symbol, s.id])    # 交易对,交易对id
                break
            except:
                self.log.warning(f"get_symbols函数报错:{traceback.format_exc()}")
            await asyncio.sleep(0.5)

    ''' ============================================================================'''
    ''' =============================== strategy ==================================='''
    ''' ============================================================================'''
    async def main(self):
        # 获取交易对资金费率
        symbols_fund = {}
        for s in self.all_symbols:
            while 1:
                try:
                    res = await self.old_rest.get_capitalRate(s[0])
                    break
                except:
                    self.log.warning(f"get_capitalRate函数报错:{traceback.format_exc()}")
                await asyncio.sleep(0.5)
            symbols_fund[s[0]] = res.capitalRate
        # print(f"资金费率:{symbols_fund}")
    
        # 获取所有标签用户持仓
        while 1:
            try:
                hold_list = []
                temp_hold_list = []
                tag_user_list = []
                data = await self.rest.fetch_hold_list(page_size=100)
                hold_list_page = data['pager']['total_page']
                temp_hold_list += data['data']
                for n in range(2, hold_list_page+1):
                    data = await self.rest.fetch_hold_list(page=n, page_size=100)
                    temp_hold_list += data['data']
                break
            except:
                self.log.warning(f"hold_list函数报错:{traceback.format_exc()}")
            await asyncio.sleep(1)
        for i in temp_hold_list:
            if i not in hold_list and 'E' in i['tag']:
                hold_list.append(i)
        detail_pos = {}     # 为了保存一个交易对下非模拟金用户的持仓
        for i in temp_hold_list:
            if i not in tag_user_list and 'E' not in i['tag']:
                tag_user_list.append(i)
                detail_pos[i['symbol']] = []

        # 统计模拟金用户持仓
        filter_pos = {}
        for i in hold_list:
            pos = int(i['amount']) if i['openDirection'] == 1 else -int(i['amount'])    # 张数
            try:
                filter_pos[i['symbol']] += pos
            except:
                filter_pos[i['symbol']] = pos
        # print(f"模拟金用户持仓:{filter_pos}")

        # 统计非模拟金标签用户持仓
        tag_user_pos = {}
        for i in tag_user_list:
            pos = int(i['amount']) if i['openDirection'] == 1 else -int(i['amount'])    # 张数
            try:
                tag_user_pos[i['symbol']] += pos
                detail_pos[i['symbol']].append([i['user_id'], pos])
            except:
                tag_user_pos[i['symbol']] = pos
        # print(f"非模拟金标签用户持仓:{tag_user_pos}")

        # 统计所有做市账户持仓
        acct_pos = {}
        for num, task in enumerate([self.rest1, self.rest2, self.rest3, self.rest4, self.rest5, self.rest6, self.rest7]):
            pos = await task.fetch_position()
            #print(f"做市账户持仓:{pos}")
            if pos is None:
                continue
            symbol_pos = {}
            for i in pos:
                amount = i['contracts'] if i['side'] == 'long' else -i['contracts']
                try:
                    symbol_pos[i['symbol']] += amount
                except:
                    symbol_pos[i['symbol']] = amount
            
            # 过滤模拟金用户持仓
            if num == 0:
                for s, v in filter_pos.items():
                    if s in symbol_pos:
                        symbol_pos[s] += v
                    else:
                        symbol_pos[s] = v
                        while 1:
                            try:
                                res = await self.old_rest.get_depth(s, 1, quan=True)
                                break
                            except:
                                self.log.warning(f"get_capitalRate函数报错:{traceback.format_exc()}")
                            await asyncio.sleep(0.5)
                        mark_price = (res.bids[0].price + res.asks[0].price)/2
                        self.symbol_price[s] = mark_price

            # 调整保存格式
            for k, v in symbol_pos.items():
                if v != 0 and k in self.symbol_price:
                    try:
                        acct_pos[k][0] += v
                        acct_pos[k][1] += round(v*self.symbol_size[k]*self.symbol_price[k])
                    except:
                        acct_pos[k] = [v, round(v*self.symbol_size[k]*self.symbol_price[k])]
        
        # 调整输出格式
        mess = '所有做市账户过滤体验金用户持仓后的总持仓:\n'
        mess1 = '所有做市账户过滤体验金用户持仓后的总持仓:\n'
        amt_mess = ''
        for k, v in sorted(acct_pos.items(), key=lambda x: abs(x[1][1]), reverse=True):
            user_pos = ''
            if k in detail_pos and k in self.symbol_price:
                for i in sorted(detail_pos[k], key=lambda x: abs(x[1]), reverse=True):
                    uid, pos = i[0], i[1]
                    user_pos += f"    用户:{uid} 持仓:{pos}张\n"
                    # 根据持仓金额做监控
                    pos_amt = abs(pos)*self.symbol_size[k]*self.symbol_price[k]
                    # print(f"用户:{uid} 持仓金额:{pos_amt}")
                    if pos_amt > self.pos_limit:
                        amt_mess += f"用户:{uid} {k}持仓金额:{int(pos_amt)}U\n"
            if v[0] != 0:
                tag_pos = tag_user_pos[k] if k in tag_user_pos else 0
                mess += f"【{k}】: \npos{v[0]}张 amt:{v[1]}u\n非模拟金标签用户持仓:{tag_pos}\n散户持仓:{(v[0]+tag_pos)*-1}\n" #标签用户详细持仓:\n{user_pos}"
                mess1 += f"【{k}】: \npos{v[0]}张 amt:{v[1]}u\n非模拟金标签用户持仓:{tag_pos}\n散户持仓:{(v[0]+tag_pos)*-1}\n标签用户详细持仓:\n{user_pos}"
        print(f"{mess1}")
        if amt_mess:
            amt_mess += '关注以上用户是否对冲'
            print(amt_mess)
            tb.warning(amt_mess, 'risk')
            tb.sendmail(f"内盘分币对持仓监控", amt_mess)

        # 发送tel
        text_temp = mess.split('\n')
        send_text = ''
        for t in text_temp:
            send_text += f"{t}\n"
            if len(send_text) > 4000:
                await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=send_text)
                send_text = ''
        if send_text:
            await self.rc_task.tg_warning(token='6431006677:AAFPjHsu3ZiowA8vyKYPmK8-b-XSPnBUu3Q', chat_id=-1002413824899, content=send_text)
        


def main():
    takeOverStrategy().run()
    # try:
    #     await task.main()  # 主循环
    # except:
    #     print(f'策略报错!\n{traceback.format_exc()}')
    # while 1:
    #     try:
    #         await task.main()  # 主循环
    #     except:
    #         print(f'策略报错!\n{traceback.format_exc()}')
    #     time.sleep(1200)

# asyncio.run(main())
if __name__ == '__main__':
    main()





