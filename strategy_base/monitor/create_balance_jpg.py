
import sys
import datetime
import traceback
import asyncio
import matplotlib.pyplot as plt
import mysql.connector
sys.path.append('../..')
from utils import Toolbox as tb
from template.template_timer import TemplateTimer, CronTrigger
from crypto_center.client.rest.okex import contract as okx_rest

# plt.rcParams['font.sans-serif'] = ['SimHei']  # Windows系统
# plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']  # macOS
plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei']  # Linux

class Strategy(TemplateTimer):

    ''' ============================================================================'''
    ''' =================================== init ==================================='''
    ''' ============================================================================'''

    def __init__(self):
        super().__init__(scheduler=True, gcc=True)
        self.loop = asyncio.get_event_loop()
        self.account_id = 'e1934f97'
        self.okx_rest = okx_rest.OkexContract('e1934f97-f831-418e-b154-1ec5a0415f9a','A2D8A15D0DE4B59BD7B270A34BB1273C','usRolUVNuyBbEF@7 ')
        
    async def on_first(self):
        await self.load_mysql()
        pass

    async def on_timer(self):
        self.log.info("=======timer======")
    
    # 连接数据库
    async def connect_db(self):
        self.db = mysql.connector.connect(
            host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
            user="contract_user",
            password="}jnB+wZ#EgmUpob",
            database="contract_db",
            port=3306  # 默认端口
        )
        self.cursor = self.db.cursor()
        
    # 对冲
    async def load_mysql(self):
        try:
            await self.connect_db()
            # 查询表信息
            self.cursor.execute("SELECT * FROM okx_hedge_balance")
            print(f"{datetime.datetime.now()} 查询mysql对冲账户权益数据:")
            balance = []
            date = []
            for row in self.cursor.fetchall():
                # print(row[2])
                balance.append(row[2])
                date.append(row[-1])
            # 生成折线图
            # 示例数据（替换为你的实际数据）
            data = balance

            # 创建图形和坐标轴
            plt.figure(figsize=(10, 6))  # 设置图形大小（宽度, 高度）英寸

            # 绘制折线图
            plt.plot(date,             # X轴
                    data,              # Y轴
                    marker='o',        # 数据点标记为圆形
                    linestyle='-',     # 实线连接
                    color='red',       # 线条颜色
                    linewidth=2,       # 线条宽度
                    markersize=8)      # 标记大小

            # 添加标题和标签
            # plt.title('对冲账户净值', fontsize=14)

            # 添加网格线
            plt.grid(True, linestyle='--', alpha=0.7)

            # 可选：自定义X轴刻度（如果数据有特定含义）
            # plt.xticks(range(len(data)), ['点1', '点2', '点3', ...])

            # 调整布局
            plt.tight_layout()

            # 保存为JPG文件（300 DPI高质量）
            plt.savefig('hedge_balance.jpg', dpi=300, format='jpg')

            # 显示图形（可选，保存后会自动关闭图形）
            # plt.show()

            print("折线图已保存为 'hedge_balance.jpg'")
            
        except:
            mess = f"{datetime.datetime.now()} 对冲账户权益数据保存mysql失败,报错:{traceback.format_exc()}"
            print(mess)
            tb.warning(mess, 'risk')
            tb.sendmail(f'对冲账户权益保存mysql失败', mess)
        self.db.close()  # 关闭数据库连接
        

def main():
    Strategy().run()



if __name__ == '__main__':
    main()
