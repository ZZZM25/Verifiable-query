import networkx as nx
import time
import csv


def find_all_n_length_paths_with_attr_lt(graph, start_node, n, attr1, attr1_value, attr2, attr2_value, path=None):
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
            if attr1 in edge_attr and str(edge_attr[attr1]) < str(attr_value1) and str(edge_attr[attr2]) <= str(attr_value2):
                # if neighbor not in path:
                new_paths = find_all_n_length_paths_with_attr_lt(graph, neighbor, n, attr1, attr1_value, attr2, attr2_value, path)
                for new_path in new_paths:
                    paths.append(new_path)
                break
    return paths

list_t=[]
t11=time.time()
G = nx.MultiDiGraph()
with open('/tocsv_only_tx/23123/tron100.csv', encoding='utf-8', newline='') as cs:
# with open('E:\\code\\jiaoben\\py111\\combined_transaction1.csv', encoding='utf-8', newline='') as cs:
    reader = csv.reader(cs)
    next(reader)
    for row in reader:
        if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
            edge_data = {f'Attribute_{i}': row[i] for i in range(len(row)) if i not in [5, 6]}
            G.add_edge(row[5], row[6], **edge_data)
t22=time.time()
print("构建模型:",t22-t11)

list_t=[]

l1 = 500

# 加载图数据
start_time = time.time()
with open("E:\\code\\jiaoben2\\1tron\\query_output_5_tron.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        print(line_number1)
        if line_number1 <=l1:

            start_node = line.strip()  # 起始节点
            n =1  # 想要查找的路径长度
            n = n + 1
            attr1 = 'Attribute_0'  # 属性名称
            # attr_value1 = '1300500'  # 属性值
            attr_value1 = '1300100'  # 属性值
            # attr_value1 = '1301000'  # 属性值
            attr2 = 'Attribute_4'  # 属性2名称
            attr_value2 = '1'  # 属性2值
            if start_node not in G:
                print("起始节点不存在于图中。")
            else:
                paths = find_all_n_length_paths_with_attr_lt(G, start_node, n, attr1, attr_value1, attr2, attr_value2)
                for path in paths:
                    edge_attrs = []
                    print(path)
                    edge_attrs_list = []  # 用于存储所有边的属性
                    for i in range(len(path) - 1):
                        # 获取两个节点之间的所有边的属性字典列表
                        edge_attrs_dict = G.get_edge_data(path[i], path[i + 1])
                        # print(edge_attrs_dict)
                        if edge_attrs_dict is not None:
                            for edge_attrs in edge_attrs_dict.values():
                                # 将每条边的属性添加到列表中
                                edge_attrs_list.append(edge_attrs)
                    # print("路径:")
                    # print("路径:", path)
                    # print("边属性:", edge_attrs_list)
        if line_number1 % 100 == 0:
            wo_time = time.time()
            # print("time:", wo_time, "seconds")
            # tim = wo_time - start_time + (t22 - t11) * line_number1
            tim = wo_time - start_time
            list_t.append(tim)
            print("Execution time:", tim, "seconds")
        if line_number1 > l1:
            break

# print("Execution time:",t22-t11, "seconds")
# print("Execution time:", wo_time - start_time, "seconds")

for tt in list_t:
    print(tt)