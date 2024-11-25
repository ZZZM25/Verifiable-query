import networkx as nx
import time
import csv
import pandas as pd


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



df = pd.read_csv('E:\\code\\jiaoben\\py111\\window100.csv', encoding='utf-8')

listtt = []
list_tx = []
list_tx_set = set()

ttttt = 0
# 加载图数据

with open("E:\\code\\jiaoben\\baseline\\output_sim500.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        print(line_number1)
        if line_number1 <= l1:
            start_node = line.strip()  # 起始节点
            n = 1 # 想要查找的路径长度
            n = n + 1
            attr = 'block_number'  # 属性名称
            attr_value = '11000101'  # 属性值
            if start_node not in G:
                print("起始节点不存在于图中。")
            else:
                paths = find_all_n_length_paths_with_attr_lt(G, start_node, n, attr, attr_value)
                start_time = time.time()

                for path in paths:
                    edge_attrs = []
                    # print(path)
                    edge_attrs_list = []  #
                    for i in range(len(path) - 1):
                        edge_attrs_dict = G.get_edge_data(path[i], path[i + 1])
                        if edge_attrs_dict is not None:
                            for edge_attrs in edge_attrs_dict.values():
                                edge_attr_block_number = int(edge_attrs['block_number'])  # 假设它应该是整数
                                edge_attr_transaction_index = int(edge_attrs['transaction_index'])  #
                                filtered_df = df[
                                    (df['block_number'].astype(int) == edge_attr_block_number) &
                                    (df['transaction_index'].astype(int) == edge_attr_transaction_index)
                                    ]
                                for _, row in filtered_df.iterrows():
                                    row_dict = {key: str(value) if not pd.isnull(value) else '' for key, value in
                                                row.items()}
                                    # print("Row:", row_dict)
                                    if str(edge_attrs) != str(row_dict):
                                        print('no')
                                    else:
                                        print('yes')
                        # print(edge_attr)
                        # print(edge_attr['Attribute_3'],edge_attr['Attribute_4'])
                        # with open('E:\\code\\jiaoben\\py111\\window1000.csv', encoding='utf-8', newline='') as f1:
                        #     reader = csv.DictReader(f1)
                        #     for row in reader:
                        #         if row['block_number'] == edge_attr['Attribute_3'] and row['transaction_index'] == edge_attr['Attribute_4']:
                        #             print(row)
                        #             print(edge_attr['data'])
                        #             if(edge_attr['data']==row):
                        #                 print('yes')


                       # 假设它应该是整数

                        # 过滤符合条件的行


                        # 检查是否有行匹配

                t2 = time.time()
                t23 = t2 - start_time
                print("对比时间:", t2 - start_time)
                ttttt = ttttt + t23
                print('111111',ttttt)

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

