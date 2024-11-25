import networkx as nx
import csv
import time


def find_n_hop_neighbors_with_attr_at_distance(graph, start_node, n, attr1, attr_value1, attr2, attr_value2):
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
            edge_attrs = graph.get_edge_data(node, neighbor)
            if edge_attrs is not None:
                for edge_key, edge_attr in edge_attrs.items():
                    # print(attr2 in edge_attr)
                    if attr1 in edge_attr and attr2 in edge_attr and str(edge_attr[attr1]) < str(attr_value1) and str(edge_attr[attr2]) <= str(attr_value2):
                        # print(attr1,attr2)
                        if neighbor not in visited:
                            queue.append((neighbor, distance + 1))
                            break
    return list(visited)


# 使用示例
# G = nx.read_gml('2.gml')
t11=time.time()
G = nx.MultiDiGraph()
with open('/tocsv_only_tx/23123/tron1000.csv', encoding='utf-8', newline='') as cs:
# with open('E:\\code\\jiaoben\\py111\\combined_transaction1.csv', encoding='utf-8', newline='') as cs:
    reader = csv.reader(cs)
    next(reader)
    # # noden = 0
    # line_number = 0
    for row in reader:
        if row:
            # line_number += 1
            # print(line_number)

            if row and row[5] != '' and row[6] != '' and row[5] != row[6]:
                edge_data = {f'Attribute_{i}': row[i] for i in range(len(row)) if i not in [5, 6]}
                G.add_edge(row[5], row[6], **edge_data)


t22=time.time()
print("构建模型:",t22-t11)
l1 = 500

list_t=[]
# 加载图数据
start_time = time.time()
with open("E:\\code\\jiaoben2\\1tron\\query_output_5_tron.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number1, line in enumerate(file1, start=1):
        print(line_number1)
        if line_number1 <= l1:

            start_node = line.strip()  # 想要查找邻居的节点
            n = 5# 距离
            attr1 = 'Attribute_0'  # 属性名称

            # attr_value1 = '11001001'  # 属性值
            i = 1000
            if i == 100:
                attr_value1 = '1300100'
            if i == 500:
                attr_value1 = '1300500'
            if i == 1000:
                attr_value1 = '1301000'
            attr2 = 'Attribute_4'  # 属性2名称
            attr_value2 = '1' # 属性2值
            neighbors = find_n_hop_neighbors_with_attr_at_distance(G, start_node, n, attr1, attr_value1,attr2, attr_value2)
            print(neighbors)
        if line_number1 % 100 == 0:
            wo_time = time.time()

            tim = wo_time - start_time+(t22-t11)*line_number1
            list_t.append(tim)
            print("Execution time:", tim, "seconds")
        if line_number1 >500 :
            break
print("Execution time:",t22-t11, "seconds")
print("Execution time:", wo_time - start_time, "seconds")
for tt in list_t:
    print(tt)
print(attr_value1)


