import mysql.connector

db = mysql.connector.connect(
    host="10.0.208.249",
    user="lh_sky",
    password="sky_123456",
    database="contract_dws",
    port=33306  # 默认端口
)
cursor = db.cursor()

# 按照date分组,查询每个date下交易量最小的5个合约
# cursor.execute(f"SELECT date, symbol, deal_amt FROM "
#                 "(SELECT date, symbol, deal_amt, RANK() "
#                 "OVER (PARTITION BY date ORDER BY deal_amt ASC) AS rnk "
#                 "FROM contract_dws.hot_vol_rank) t "
#                 "WHERE rnk <= 5 "
#                 "ORDER BY date, deal_amt;")

# 查询所有合约的总交易量,并返回交易量最小的5个合约
cursor.execute(f"SELECT symbol, SUM(deal_amt) AS total_deal_amt "
                "FROM contract_dws.hot_vol_rank "
                "WHERE date >= CURDATE() - INTERVAL 7 DAY "
                "GROUP BY symbol "
                "ORDER BY total_deal_amt ASC "
                "LIMIT 5;")
data = cursor.fetchall()
print(f"交易量最小的5个合约:")
for i in data:
    print(i)

# 查询所有合约的总交易用户数,并返回交易用户数最小的5个合约
cursor.execute(f"SELECT symbol, SUM(user_num) AS total_user_num "
                "FROM contract_dws.hot_user_rank "
                "WHERE date >= CURDATE() - INTERVAL 7 DAY "
                "GROUP BY symbol "
                "ORDER BY total_user_num ASC "
                "LIMIT 5;")
data = cursor.fetchall()
print(f"交易用户数最小的5个合约:")
for i in data:
    print(i)

# 查询所有合约的总交易用户数,并返回交易用户数最小的5个合约
cursor.execute(f"SELECT symbol, SUM(fee) AS total_fee "
                "FROM contract_dws.symbol_fee_rank "
                "WHERE date >= CURDATE() - INTERVAL 7 DAY "
                "GROUP BY symbol "
                "ORDER BY total_fee ASC "
                "LIMIT 5;")
data = cursor.fetchall()
print(f"手续费最小的5个合约:")
for i in data:
    print(i)
    
cursor.close()
db.close()




