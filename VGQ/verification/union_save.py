import pandas as pd
import pickle
import time
import os

# 读取 CSV 文件并获取边的数据
# csv_file = 'E:\\code\\jiaoben2\\_data\\window1000.csv'  # 替换为你的CSV文件路径
csv_file = 'E:\\code\\jiaoben2\\_data\\test2.csv'
data = pd.read_csv(csv_file)

# 选择 block_number 从 1 到 100 的数据
filtered_data = data[
    (data['block_number'] >= 0) &
    (data['block_number'] <= 2) &
    (data['from_address'] != data['to_address']) &
    (data['from_address'].notna()) &
    (data['to_address'].notna())
]
# filtered_data = data[(data['block_number'] >= 0) & (data['block_number'] <= 3)]
# 并查集的初始化和函数定义
def make_set():
    """ 初始化空的父指针和秩数组 """
    return {}, {}

def add(parent, rank, x):
    """ 添加新元素到并查集中 """
    if x not in parent:
        parent[x] = x
        rank[x] = 1

def find(parent, x):
    """ 查找元素 x 的根，并进行路径压缩 """
    if x not in parent:
        parent[x] = x
        rank[x] = 1
    if parent[x] != x:
        parent[x] = find(parent, parent[x])
    return parent[x]

def union(parent, rank, x, y):
    """ 合并元素 x 和 y 所属的集合 """
    rootX = find(parent, x)
    rootY = find(parent, y)

    if rootX != rootY:
        if rank[rootX] > rank[rootY]:
            parent[rootY] = rootX
        elif rank[rootX] < rank[rootY]:
            parent[rootX] = rootY
        else:
            parent[rootY] = rootX
            rank[rootX] += 1

def find_connected_components(edges):
    parent, rank = make_set()  # 初始化并查集

    # 添加所有节点到并查集中并处理边
    for u, v in edges:
        add(parent, rank, u)
        add(parent, rank, v)
        union(parent, rank, u, v)

    # 查找所有节点的根，并将它们分组到各个连通分量中
    components = {}
    for node in parent:
        root = find(parent, node)
        if root not in components:
            components[root] = []
        components[root].append(node)

    return parent, rank, components

t1=time.time()
# 按 block_number 分组处理并查集
block_groups = filtered_data.groupby('block_number')
all_sets = []

for block_number, group in block_groups:
    print(f"\n处理 block_number = {block_number} 的边")


    # 获取当前 block_number 下的边数据

    edges = [(row['from_address'], row['to_address']) for index, row in group.iterrows()]

    # 找到当前 block_number 的连通分量
    parent, rank, components = find_connected_components(edges)
    # print(parent['0xa694068fdfd382fa60c249e9a2b92a5548ad50ef'])
    # print(rank['0xa694068fdfd382fa60c249e9a2b92a5548ad50ef'])


    # 保存当前的并查集及其 block_number
    all_sets.append((parent, rank, components, block_number))

    # 输出当前 block_number 的连通分量
    print("\n当前并查集连通分量:")
    for component in components.values():
        print(parent)
        print(rank)
        print(component)

folder_name = "E:\\code\\jiaoben2\\union_find\\aaa"
# 将每个 block_number 的并查集保存到文件
for parent, rank, components, block_number in all_sets:
    print(parent,rank,components,block_number)
    filename = os.path.join(folder_name, f"block_{block_number}_disjoint_set.pkl")
    with open(filename, 'wb') as f:
        # pickle.dump((parent, rank, components), f)
        pickle.dump((parent, rank), f)
        # print(f"保存 {filename} 完成")

# print("所有并查集保存完成")

t2=time.time()
print(t2-t1)