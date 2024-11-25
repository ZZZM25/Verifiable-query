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
def find_n_hop_neighbors_with_attr_at_distance(graph, start_node, n):
    if start_node not in graph:
        print(f"The node {start_node} is not in the graph.")
        return set()

    visited = set()
    queue = [(start_node, 0)]

    while queue:
        node, distance = queue.pop(0)
        if distance == n:
            visited.add(node)  # 包括起始节点
            return list(visited)
        if distance > n:
            return list(visited)
        if distance > 0:  # 排除起始节点
            visited.add(node)
        neighbors = list(graph.neighbors(node))
        for neighbor in neighbors:
            if neighbor not in visited:
                queue.append((neighbor, distance + 1))

    return list(visited)

cccc=0
s1 = time.time()
G_weighted = nx.DiGraph()
# with open('E:\\code\\jiaoben\\py111\\window1000.csv', encoding='utf-8', newline='') as cs:
with open('E:/code/jiaoben2/tocsv_only_tx/23123/tron100.csv', encoding='utf-8', newline='') as cs:
    reader = csv.reader(cs)
    next(reader)
    for row in reader:
        # if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
        if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
            edge_to_check = (row[5], row[6])
            if G_weighted.has_edge(*edge_to_check):
                continue
            else:
                G_weighted.add_edge(*edge_to_check)
                cccc = cccc+1
s2 = time.time()
print("Execution time:", s2-s1, "seconds")
print(cccc)


l1 = 10

# 加载图数据
start_time1 = time.time()
with open("E:\\code\\jiaoben2\\1tron\\query_output_5_tron.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        # print(line_number1)
        if line_number1 <= l1:
            start_node = line.strip()  # 想要查找邻居的节点
            n =1 # 距离
            # attr2 = 'attr2'  # 属性2名称
            # attr_value2 = 20  # 属性2值
            # G_weighted = nx.DiGraph()
            # with open('E:\\code\\jiaoben\\py111\\window1000.csv', encoding='utf-8', newline='') as cs:
            #     reader = csv.reader(cs)
            #     next(reader)
            #     for row in reader:
            #         # if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
            #         if row and row[5] != '' and row[6] != '' and row[5] != row[6] and row[8] > '200000':
            #             edge_to_check = (row[5], row[6])
            #             if G_weighted.has_edge(*edge_to_check):
            #                 continue
            #             else:
            #                 G_weighted.add_edge(*edge_to_check)
            neighbors = find_n_hop_neighbors_with_attr_at_distance(G_weighted, start_node, n)
            # print(neighbors)

        # if line_number1 % 100 == 0:
        #     wo_time = time.time()
        #     # print("time:", wo_time, "seconds")
        #     tim = wo_time - start_time
        #     print("Execution time:", tim/500, "seconds")
        if line_number1 > l1:
            break
wo_time = time.time()
tim = wo_time - start_time1
# print("Execution time:", tim/500, "seconds")
print(s2-s1)
print("Execution time:", tim, "seconds")
#

