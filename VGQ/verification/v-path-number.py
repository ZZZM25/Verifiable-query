import networkx as nx
import time
import csv
#
# def find_all_n_length_paths_with_attr_lt(graph, start_node, n, attr, attr_value, path=None):
#     if path is None:
#         path = [start_node]
#     else:
#         path = path + [start_node]
#
#     if len(path) == n:
#         return [path]
#
#     if len(path) > n:
#         return []
#
#     paths = []
#     for neighbor in graph.neighbors(start_node):
#         edge_attr = graph.edges[start_node, neighbor]
#         if attr in edge_attr and str(edge_attr[attr]) < str(attr_value):
#             if neighbor not in path:
#                 new_paths = find_all_n_length_paths_with_attr_lt(graph, neighbor, n, attr, attr_value, path)
#                 for new_path in new_paths:
#                     paths.append(new_path)
#     return paths

######条数验证
def find_all_n_length_paths_with_weight(graph, start_node, n, path=None, weight=1):
    if path is None:
        path = [start_node]
    else:
        path = path + [start_node]

    if len(path) == n:
        return [(path, weight)]

    if len(path) > n:
        return []

    paths = []
    for neighbor in graph.neighbors(start_node):
        edge_weight = graph.edges[start_node, neighbor].get('weight', 1)
        new_weight = weight * edge_weight
        new_paths = find_all_n_length_paths_with_weight(graph, neighbor, n, path, new_weight)
        for new_path, new_weight in new_paths:
            paths.append((new_path, new_weight))
    return paths


cccc=0
s1 = time.time()
G_weighted = nx.DiGraph()
# with open('E:\\code\\jiaoben\\py111\\window750.csv', encoding='utf-8', newline='') as cs:
with open('E:/code/jiaoben2/tocsv_only_tx/23123/tron100.csv', encoding='utf-8', newline='') as cs:
    reader = csv.reader(cs)
    next(reader)
    for row in reader:
        if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
        # if row and row[5] != '' and row[6] != '' and row[5] != row[6] and row[8] > '200000':
            edge_to_check = (row[5], row[6])
            if G_weighted.has_edge(*edge_to_check):
                G_weighted[edge_to_check[0]][edge_to_check[1]]['weight'] += 1
            else:
                G_weighted.add_edge(*edge_to_check, weight=1)
                cccc = cccc+1

s2 = time.time()
print("Execution time:", s2-s1, "seconds")
print(cccc)

l1 = 10

# 加载图数据
start_time = time.time()
with open("E:\\code\\jiaoben2\\1tron\\query_output_5_tron.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        print(line_number1)
        if line_number1 <= l1:
            start_node = line.strip()  # 起始节点
            n =1 # 想要查找的路径长度+1
            n=n+1
            if start_node not in G_weighted:
                print("0")
            else:
                paths = find_all_n_length_paths_with_weight(G_weighted, start_node, n)
                weight1 = 0
                for path, weight in paths:
                    # print("Path:", path)
                    # print("Weight:", weight)
                    weight1 = weight1+weight
                # print(weight1)
        # if line_number1 % 100 == 0:
        #     wo_time = time.time()
        #     # print("time:", wo_time, "seconds")
        #     tim = wo_time - start_time
        #     print("Execution time:", tim/500, "seconds")
        if line_number1 > l1:
            break
wo_time = time.time()
tim = wo_time - start_time
# print("Execution time:", tim/500, "seconds")
print('build',s2-s1)
print("Execution time:", tim/10, "seconds")



