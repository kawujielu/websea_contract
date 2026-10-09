import datetime
import mysql.connector
from decimal import Decimal


db = mysql.connector.connect(
    host="abc-mysql-instance-1.cr8a0xsju0u1.ap-southeast-1.rds.amazonaws.com",
    user="admin",  #"contract_user",
    password="A(?xvw8~v(ke0(O,=Se!W(!UGBujuh(XkBHuQTRu2",   #"}jnB+wZ#EgmUpob",
    database="fund",
    port=3306  # 默认端口
)
cursor = db.cursor()

db2 = mysql.connector.connect(
    host="10.0.208.249",
    user="lh_sky",
    password="sky_123456",
    database="contract_dws",
    port=33306  # 默认端口
)
cursor2 = db2.cursor()


try:
    if db.is_connected():
        print("成功连接数据库")
        cursor.execute("SHOW TABLES")
        print("当前数据库包含的表：")
        for table in cursor.fetchall():
            print(table[0])
        cursor.execute("SELECT * FROM mongoorders LIMIT 10")
        for row in cursor.fetchall():
            print(row)
except Exception as e:
    print("数据库操作错误:", e)
finally:
    cursor.close()
    db.close()
exit()

try:
    if db2.is_connected():
        print("成功连接数据库")
        cursor2.execute("SHOW TABLES")
        print("当前数据库包含的表：")
        for table in cursor2.fetchall():
            print(table[0])
        cursor2.execute("SELECT * FROM mongoorders")
        for row in cursor2.fetchall():
            print(row)
except Exception as e:
    print("数据库操作错误:", e)
finally:
    cursor2.close()
    db2.close()
exit()

#cursor.execute("TRUNCATE TABLE okx_teacher_deals")
#db.commit()
#cursor.execute("DROP TABLE okx_hedge_balance3")
# cursor.execute("DROP TABLE crypto_filter_funding_rates")
#cursor.execute("DROP TABLE okx_teacher_deals")
#db.commit()  # 提交事务
# 修改表名称
#cursor.execute("RENAME TABLE okx_teacher_deals TO okx_teacher_deals_bak")
#db.commit()

# 修改数值
# sql = """
# UPDATE okx_hedge_profit
# SET profit = %s,
#     profit_add_unrealized_pnl = %s
# WHERE id = %s
# """
#cursor2.execute(sql, (-5, -1700, 60))
#db2.commit()

# 插入一条数据
#data = ('3', '外部资费套利团队专用对冲账户', Decimal('9873.0000'), Decimal('11155.0400'), Decimal('3585.0000'), Decimal('7571.0000'), datetime.datetime(2025, 9, 15, 6, 0, 1))
#insert_sql = f"INSERT INTO hedge_pnl_monitor (id, account_name, balance, balance_add_unrealized_pnl, free, frozen, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
#cursor.executemany(insert_sql, data)
#db.commit()
#exit()

# 删除指定数据
#cursor.execute("DELETE FROM okx_hedge_acct WHERE id = 1310")
#db.commit()  # 提交事务
#exit()

# 创建表
# 指定对冲账户pnl
# cursor.execute("""CREATE TABLE `hedge_pnl_monitor` (
#     `id` INT AUTO_INCREMENT PRIMARY KEY,
#     `account_name` VARCHAR(36) NOT NULL,
#     `balance` DECIMAL(20,4) NOT NULL DEFAULT 0,
#     `balance_add_unrealized_pnl` DECIMAL(20,4) NOT NULL DEFAULT 0,
#     `free` DECIMAL(20,4) NOT NULL DEFAULT 0,
#     `frozen` DECIMAL(20,4) NOT NULL DEFAULT 0,
#     `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
# db.commit()  # 提交事务

# 指定对冲账户成交明细
# cursor.execute("""CREATE TABLE `hedge_deal_monitor` (
#   `id` INT AUTO_INCREMENT PRIMARY KEY,
#   `open_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#   `close_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#   `symbol` VARCHAR(15) NOT NULL COMMENT '交易对',
#   `pos` DECIMAL(10,8) NOT NULL COMMENT '持仓数量',
#   `side` VARCHAR(15) NOT NULL COMMENT '持仓方向',
#   `open_price` DECIMAL(10,8) NOT NULL COMMENT '开仓价格',
#   `close_price` DECIMAL(10,8) NOT NULL COMMENT '平仓价格',
#   `profit` DECIMAL(10,8) NOT NULL COMMENT '盈亏',
#   `fee` DECIMAL(10,8) NOT NULL COMMENT '手续费',
#   `fund` DECIMAL(10,8) NOT NULL COMMENT '资金费率',
#   `level` DECIMAL(3) NOT NULL COMMENT '杠杆'
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
# db.commit()  # 提交事务
# db.close()   # 关闭数据库连接

# 存资金费率数据
# cursor.execute("""CREATE TABLE `crypto_funding_rates` (
#   `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
#   `pair_name` VARCHAR(20) NOT NULL COMMENT '交易对名称如BTC-USDT',
#   `exchange` VARCHAR(15) NOT NULL COMMENT '交易所标识',
#   `fund_rate` DECIMAL(10,8) NOT NULL COMMENT '资金费率值(支持正负)',
#   `fund_value` DECIMAL(10,8) NOT NULL COMMENT '资金费率盈亏(支持正负)',
#   `create_ts` DATETIME(3) NOT NULL COMMENT '精确到毫秒的采集时间',
#   `timestamp` DATETIME(3) NOT NULL COMMENT '收取资金费率的整点时间',
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")

# 资金费率
# cursor.execute("""CREATE TABLE `crypto_filter_funding_rates` (
#   `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
#   `pair_name` VARCHAR(20) NOT NULL COMMENT '交易对名称如BTC-USDT',
#   `exchange` VARCHAR(15) NOT NULL COMMENT '交易所标识',
#   `fund_rate` DECIMAL(10,8) NOT NULL COMMENT '资金费率值(支持正负)',
#   `fund_value` DECIMAL(10,8) NOT NULL COMMENT '资金费率盈亏(支持正负)',
#   `create_ts` DATETIME(3) NOT NULL COMMENT '精确到毫秒的采集时间',
#   `timestamp` DATETIME(3) NOT NULL COMMENT '收取资金费率的整点时间',
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
# db.commit()  # 提交事务
# exit()

# 存okx对冲账户权益数据
# cursor.execute("""
#        CREATE TABLE IF NOT EXISTS `okx_hedge_acct` (
#            `id` INT AUTO_INCREMENT PRIMARY KEY,
#            `account_id` VARCHAR(36) NOT NULL,
#            `balance` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `balance_add_unrealized_pnl` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `free` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `frozen` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#        )
#        """)
# 存okx对冲盈亏
#cursor.execute("""
#        CREATE TABLE IF NOT EXISTS `okx_hedge_profit` (
#            `id` INT AUTO_INCREMENT PRIMARY KEY,
#            `account_id` VARCHAR(36) NOT NULL,
#            `profit` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `profit_add_unrealized_pnl` DECIMAL(20,12) NOT NULL DEFAULT 0,
#            `date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#        )
#        """)

# 从其他表中将数据导入新表
#cursor2.execute(f"SELECT * from contract_dws.symbol_fee_rank")
#data = cursor2.fetchall()
# print(data)
#for i in data:
#    print(i)
#    save_sql_data = [(i[1], i[2], i[3], i[4], i[5], datetime.datetime.now())]
#    print(save_sql_data)
#    insert_sql = f"INSERT INTO okx_hedge_acct2 (account_id, balance, balance_add_unrealized_pnl, free, frozen, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
#    cursor.executemany(insert_sql, save_sql_data)
#    db.commit()  # 提交事务

# db.commit()  # 提交事务
# db.close()   # 关闭数据库连接

# 查询表信息
# cursor.execute("SELECT * FROM crypto_funding_rates WHERE id = 928")
# db.commit()  # 提交事务
# for row in cursor.fetchall():
#     print(row)
# exit()

# 删除指定数据
# cursor.execute("DELETE FROM crypto_funding_rates WHERE id = 929")
# db.commit()  # 提交事务

# 删除表
# cursor.execute("DROP TABLE crypto_funding_rates")
# db.commit()  # 提交事务








