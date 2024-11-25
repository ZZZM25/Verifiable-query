import pymysql
import time
# 配置数据库连接信息
config = {
    'user': 'root',       # MySQL用户名
    'password': '123456',   # MySQL密码
    'host': '172.19.8.25',           # MySQL服务器地址
    'database': 'eth',   # 要连接的数据库名称
    'port': 3306                   # MySQL端口，默认3306
}

list_t=[]
list2=[]

l1 = 500
block_number_limit='11001001'
gas_limit='200000'
# 连接到MySQL数据库
try:
    connection = pymysql.connect(**config)
    print("数据库连接成功")
    start_time = time.time()
    # 创建一个游标对象，执行SQL查询
    with open("../query/output_sim1000.txt", "r") as file1:
        for line_number, line in enumerate(file1, start=1):
            print(line_number)
            if line_number <= l1:
                s=line.strip()
                with connection.cursor() as cursor:

                    #####################################################无属性

                    #####################  5
                    # query = (
                    #     "SELECT DISTINCT "
                    #     "    sub_e5.end AS end "
                    #     "FROM "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e1 "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e2 "
                    #     "    ON sub_e1.end = sub_e2.start "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e3 "
                    #     "    ON sub_e2.end = sub_e3.start "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e4 "
                    #     "    ON sub_e3.end = sub_e4.start "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e5 "
                    #     "    ON sub_e4.end = sub_e5.start "
                    #     "WHERE sub_e1.start = %s AND sub_e1.block_number < %s;"
                    # )
                    #
                    # cursor.execute(query, (
                    #     block_number_limit,  # for sub_e1
                    #     block_number_limit,  # for sub_e2
                    #     block_number_limit,  # for sub_e3
                    #     block_number_limit,  # for sub_e4
                    #     block_number_limit,  # for sub_e5
                    #     s,  # start node for sub_e1
                    #     block_number_limit  # block_number limit for sub_e1
                    # ))
                    #####################  5-end


                    ####################  3
                    # query = (
                    #     "SELECT DISTINCT "
                    #     "    sub_e3.end AS end "
                    #     "FROM "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e1 "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e2 "
                    #     "    ON sub_e1.end = sub_e2.start "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s) AS sub_e3 "
                    #     "    ON sub_e2.end = sub_e3.start "
                    #     "WHERE sub_e1.start = %s AND sub_e1.block_number < %s;"
                    # )
                    #
                    # cursor.execute(query, (
                    #     block_number_limit,  # for sub_e1
                    #     block_number_limit,  # for sub_e2
                    #     block_number_limit,  # for sub_e3
                    #     s,  # start node for sub_e1
                    #     block_number_limit  # block_number limit for sub_e1
                    # ))

                    ####################  3-end


                    #####################  1
                    # query = "SELECT DISTINCT end FROM node_tx_node_eth WHERE start = %s AND block_number < %s;"
                    # cursor.execute(query, (s,block_number_limit))  # 使用字符串查询
                    #####################  1-end



                    #####################################################有属性
                    #####################  5
                    query = (
                        "SELECT DISTINCT "
                        "    sub_e5.end AS end "
                        "FROM "
                        "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e1 "
                        "JOIN "
                        "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e2 "
                        "    ON sub_e1.end = sub_e2.start "
                        "JOIN "
                        "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e3 "
                        "    ON sub_e2.end = sub_e3.start "
                        "JOIN "
                        "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e4 "
                        "    ON sub_e3.end = sub_e4.start "
                        "JOIN "
                        "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e5 "
                        "    ON sub_e4.end = sub_e5.start "
                        "WHERE sub_e1.start = %s AND sub_e1.block_number < %s;"
                    )

                    cursor.execute(query, (
                        block_number_limit, gas_limit,  # for sub_e1
                        block_number_limit, gas_limit,  # for sub_e2
                        block_number_limit, gas_limit,  # for sub_e3
                        block_number_limit, gas_limit,  # for sub_e4
                        block_number_limit, gas_limit,  # for sub_e5
                        s,  # start node for sub_e1
                        block_number_limit  # block_number limit for sub_e1
                    ))

                    #####################  5-end

                    #####################  3

                    # query = (
                    #     "SELECT DISTINCT "
                    #     "    sub_e3.end AS end "
                    #     "FROM "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e1 "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e2 "
                    #     "    ON sub_e1.end = sub_e2.start "
                    #     "JOIN "
                    #     "    (SELECT * FROM node_tx_node_eth WHERE block_number < %s AND gas > %s) AS sub_e3 "
                    #     "    ON sub_e2.end = sub_e3.start "
                    #     "WHERE sub_e1.start = %s AND sub_e1.block_number < %s;"
                    # )
                    #
                    # cursor.execute(query, (
                    #     block_number_limit, gas_limit,  # for sub_e1
                    #     block_number_limit, gas_limit,  # for sub_e2
                    #     block_number_limit, gas_limit,  # for sub_e3
                    #     s,  # start node for sub_e1
                    #     block_number_limit  # block_number limit for sub_e1
                    # ))

                    #####################  3-end

                    #####################  1
                    # query = "SELECT DISTINCT end FROM node_tx_node_eth WHERE start = %s AND block_number < %s AND gas > %s;"
                    # cursor.execute(query, (s,block_number_limit,gas_limit))  # 使用字符串查询
                    #####################  1-end


                    # 获取结果
                    rows = cursor.fetchall()
                    # for row in rows:
                    #     print(row)

                    # # 获取结果2
                    # result_exists = cursor.fetchone() is not None
                    #
                    # if result_exists:
                    #     print("查询到结果")
                    # else:
                    #     print("未查询到结果")

                if line_number % 100 == 0:
                    wo_time = time.time()
                    list2.append(line_number)
                    # print("time:", wo_time, "seconds")
                    tim = wo_time - start_time
                    print("Execution time:", tim, "seconds")
                    list_t.append(tim)
            else:
                break



except pymysql.MySQLError as e:
    print(f"数据库错误: {e}")

finally:
    # 关闭连接
    connection.close()
    print("数据库连接已关闭")

end_time = time.time()
execution_time = end_time - start_time
print("Execution time:", execution_time, "seconds")
for t in list_t:
    print(t)
for aab in list2:
    print(aab)
