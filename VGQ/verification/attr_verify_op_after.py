import networkx as nx
import time
import csv
import pandas as pd
import hashlib
# def search_in_csv_single_pass(file_path, queries):
#     results = []
#     query_set = set(queries)  # 使用集合来跟踪已匹配的查询条件
#     with open(file_path, 'r', newline='') as csv_file:
#         reader = csv.DictReader(csv_file)
#         i = 0
#         for row in reader:
#             # 假设查询条件在CSV的两列（例如 'column1' 和 'column2'）
#             # print(row)
#             current_query = (int(row['block_number']), int(row['transaction_index']))
#             # print(current_query)
#
#             if current_query in query_set:
#                 results.append(row)
#                 i=i+1
#                 query_set.remove(current_query)  # 从集合中移除已匹配的查询条件
#                 if not query_set:
#                     break  # 所有查询条件都已找到，提前结束循环
#     print(i)
#
#     return results

# def hash_row(row):
#     """对行内容进行哈希处理"""
#     row_str = ','.join([f"{key}:{value}" for key, value in row.items()])
#     return hashlib.md5(row_str.encode()).hexdigest()

def hash_row(row):
    """对行内容进行哈希处理"""
    row_str = str(row)
    return hashlib.md5(row_str.encode()).hexdigest()

def update_hash(existing_hash, new_hash):
    """更新现有哈希值与新哈希值的组合"""
    combined = existing_hash + new_hash
    return hashlib.md5(combined.encode()).hexdigest()


def count_query_occurrences(queries, q):
    count = 0
    for query in queries:
        if query == q:
            count += 1
    return count

def search_in_csv_single_pass(file_path, queries):
    # 初始的空哈希值
    # result_hash = hashlib.md5().hexdigest()
    # print(len(queries))
    # query_set = set(queries)  # 使用集合来跟踪已匹配的查询条件
    # print(len(query_set))


    i=0
    with open(file_path, 'r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)



        for row in reader:

            if i >= len(queries):
                break
            if row and row['from_address']!='' and row['to_address']!='' and row['from_address']!=row['to_address']:
                while row == queries[i]:
                    # print('q', queries[i])
                    # print(queries[i])
                    i = i + 1
                    # print(i)
                    # print(len(queries))
                    if i >= len(queries):
                        print('yes')
                        break

            # 假设查询条件在CSV的两列（例如 'column1' 和 'column2'）





                # count_query_occurrences(queries, current_query)
                # i = i + 1
                # row_hash = hash_row(row)
                # result_hash = update_hash(result_hash, row_hash)
                # l1=len(query_set)
                # query_set.remove(current_query)
                # l2=len(query_set)
                # print(l2-l1)
                # 从集合中移除已匹配的查询条件
                # if not query_set:
                #     break  # 所有查询条件都已找到，提前结束循环
    # print(i)
    # return result_hash



def find_all_n_length_paths_with_attr_lt(graph, start_node, n, attr, attr_value, path=None):
    if path is None:
        path = [start_node]
    else:
        path = path + [start_node]

    if len(path) == n:
        return [path]

    if len(path) > n:
        return []

    paths = []
    for neighbor in graph.neighbors(start_node):
        # edge_attr = graph.edges[start_node, neighbor]
        edge_attrs = graph.get_edge_data(start_node, neighbor)
        # print( edge_attrs )
        for edge_key, edge_attr in edge_attrs.items():
            if attr in edge_attr and str(edge_attr[attr]) < str(attr_value):
                # if neighbor not in path:
                new_paths = find_all_n_length_paths_with_attr_lt(graph, neighbor, n, attr, attr_value, path)
                for new_path in new_paths:
                    paths.append(new_path)
                break
    return paths

name = ['hash', 'nonce', 'block_hash', 'block_number','transaction_index', 'from_address', 'to_address', 'value', 'gas', 'gas_price', 'input', 'block_timestamp', 'max_fee_per_gas', 'max_priority_fee_per_gas', 'transaction_type']


G = nx.MultiDiGraph()
with open('E:\\code\\jiaoben\\py111\\window100.csv', encoding='utf-8', newline='') as cs:
    reader = csv.reader(cs)
    next(reader)
    for row in reader:
        if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
            # edge_data = {f'Attribute_{i}': row[i] for i in range(len(row)) if i not in [5, 6]}
            edge_data = {name[i]: str(row[i]) for i in range(len(row))}
            G.add_edge(row[5], row[6], **edge_data)


l1 = 500
listtt=[]
list_tx=[]
list_tx_set=set()

ttttt=0
df = pd.read_csv('E:\\code\\jiaoben\\py111\\window100.csv', encoding='utf-8')

# 加载图数据

with open("E:\\code\\jiaoben\\baseline\\output_sim100.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        print(line_number1)
        if line_number1<=l1:
            start_node = line.strip()  # 起始节点
            n =1# 想要查找的路径长度
            n = n + 1
            attr = 'block_number'  # 属性名称
            attr_value = '11000101'  # 属性值
            if start_node not in G:
                print("起始节点不存在于图中。")
            else:
                # t1=time.time()
                paths = find_all_n_length_paths_with_attr_lt(G, start_node, n, attr, attr_value)
                # t2=time.time()
                # print(t2-t1)
                start_time = time.time()
                edge=[]
                edge_set = set()
                for path in paths:
                    # visited=set()
                    edge_attrs = []
                    # print(path)
                    edge_attrs_list = []  # 用于存储所有边的属性
                    for i in range(len(path) - 1):
                        # 获取两个节点之间的所有边的属性字典列表
                        edge_attrs_dict = G.get_edge_data(path[i], path[i + 1])
                        # print(edge_attrs_dict)
                        if edge_attrs_dict is not None:
                            for edge_attrs in edge_attrs_dict.values():
                                # 将每条边的属性添加到列表中
                                edge_attrs_list.append(edge_attrs)
                                # print(edge_attrs)

                                # print(edge_attr)
                                # print(edge_attr['Attribute_3'],edge_attr['Attribute_4'])
                                # with open('E:\\code\\jiaoben\\py111\\window1000.csv', encoding='utf-8', newline='') as f1:
                                #     reader = csv.DictReader(f1)
                                #     for row in reader:
                                #         if row['block_number'] == edge_attrs['block_number'] and row['transaction_index'] == edge_attrs['transaction_index']:
                                #             print(row)
                                #             print(edge_attrs)
                                #             if(edge_attrs==row):
                                #                 print('yes')
                                #             break

                                edge_attr_block_number = int(edge_attrs['block_number'])  # 假设它应该是整数
                                edge_attr_transaction_index = int(edge_attrs['transaction_index'])  # 假设它应该是整数

                                # list_tx.append((edge_attr_block_number,edge_attr_transaction_index))

                                # list_tx.append(tuple((edge_attr_block_number,edge_attr_transaction_index)))

                                # list_tx_set.add((edge_attr_block_number, edge_attr_transaction_index))
                                # list_tx_set.add(tuple((edge_attr_block_number, edge_attr_transaction_index)))

                                # edge_set.add(hash(frozenset(edge_attrs.items())))
                                # edge_set.add(tuple(edge_attrs))
                                # edge_set.add(tuple(edge_attrs.items()))
                                edge.append(edge_attrs)


                # print(len(edge_set))
                # print(len(edge))
                # edge_set = set(frozenset(d.items()) for d in edge)
                # print(len(edge_set))
                # print(len(list_tx))
                # print(len(list_tx_set))
                sorted_edge = sorted(edge, key=lambda x: (int(x['block_number']), int(x['transaction_index'])))
                # sorted_points = sorted(list_tx, key=lambda point: (point[0], point[1]))
                # sorted_points = sorted(list_tx, key=lambda point: (point[0], point[1]))
                # print(sorted_points)

                # print(len(sorted_points))

                file_path = 'E:\\code\\jiaoben\\py111\\window1000.csv'
                # results = search_in_csv_single_pass(file_path, sorted_edge)
                search_in_csv_single_pass(file_path, sorted_edge)
                # print(len(results))

                t2 = time.time()
                t23=t2 - start_time
                print("对比时间:", t2 - start_time)
                ttttt=ttttt+t23
                print(ttttt)



                                # 过滤符合条件的行
                                # filtered_df = df[
                                #     (df['block_number'].astype(int) == edge_attr_block_number) &
                                #     (df['transaction_index'].astype(int) == edge_attr_transaction_index)
                                #     ]
                                #
                                # # 检查是否有行匹配
                                # for _, row in filtered_df.iterrows():
                                #     row_dict = {key: str(value) if not pd.isnull(value) else '' for key, value in
                                #                 row.items()}
                                #     # print("Row:", row_dict)
                                #     if str(edge_attrs) == str(row_dict):
                                #         print('yes')





        if line_number1 % 100 == 0:
            # wo_time = time.time()
            # # print("time:", wo_time, "seconds")
            # tim = wo_time - start_time
            # print("Execution time:", tim, "seconds")
            listtt.append(ttttt)
        if line_number1 > l1:
            break

for tt in listtt:
    print(tt)



