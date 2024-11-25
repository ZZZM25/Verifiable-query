import pickle
import os
import time
# 全局并查集的初始化和函数定义
def make_set():
    """ 初始化空的父指针和秩数组 """
    return {}, {}

def add(parent, rank, x):
    """ 添加新元素到并查集中 """
    if x not in parent:
        parent[x] = x
        rank[x] = 1

def find(parent, rank, x):
    """ 查找元素 x 的根，并进行路径压缩 """
    if x not in parent:
        parent[x] = x
        rank[x] = 1
    if parent[x] != x:
        parent[x] = find(parent, rank, parent[x])  # 传递 rank 给下一层递归
    return parent[x]

# def find(parent, x):
#     """ 查找元素 x 的根，并进行路径压缩 """
#     if x not in parent:
#         parent[x] = x
#         rank[x] = 1
#     if parent[x] != x:
#         parent[x] = find(parent,parent[x])  # 传递 rank 给下一层递归
#     return parent[x]


def union(parent, rank, x, y):
    """ 合并元素 x 和 y 所属的集合 """
    rootX = find(parent,rank, x)
    rootY = find(parent, rank,y)

    if rootX != rootY:
        if rank[rootX] > rank[rootY]:
            parent[rootY] = rootX
        elif rank[rootX] < rank[rootY]:
            parent[rootX] = rootY
        else:
            parent[rootX] = rootY
            rank[rootY] += 1

# 从文件中读取并查集并融合
# def merge_from_files(file_list):
#     global_parent, global_rank = make_set()  # 初始化全局并查集
#
#     for filename in file_list:
#         if not os.path.exists(filename):
#             print(f"{filename} 不存在，跳过该文件")
#             continue
#
#         try:
#             with open(filename, 'rb') as f:
#                 # parent, rank, components = pickle.load(f)
#                 parent, rank = pickle.load(f)
#         except Exception as e:
#             print(f"读取 {filename} 时出错: {e}")
#             continue
#
#         # 将当前文件中的并查集合并到全局并查集中
#         print(parent)
#         for node in parent:
#             add(global_parent, global_rank, node)
#             union(global_parent, global_rank, node, parent[node])
#
#         # 输出当前文件中的并查集状态
#         # print(f"\n{filename} 的并查集父指针 (parent) 数组:")
#         # for node in parent:
#         #     print(f"Node {node}: Parent {parent[node]}")
#         #
#         # print(f"\n{filename} 的并查集秩 (rank) 数组:")
#         # for node in rank:
#         #     print(f"Node {node}: Rank {rank[node]}")
#
#     return global_parent, global_rank
def merge_from_files(file_list):
    if not file_list:
        return {}, {}

    # 读取第一个文件，初始化全局并查集
    first_file = file_list[0]
    if not os.path.exists(first_file):
        print(f"{first_file} 不存在，无法初始化全局并查集")
        return {}, {}

    try:
        with open(first_file, 'rb') as f:
            global_parent, global_rank = pickle.load(f)
    except Exception as e:
        print(f"读取 {first_file} 时出错: {e}")
        return {}, {}

    # 处理剩余文件
    for filename in file_list[1:]:
        if not os.path.exists(filename):
            print(f"{filename} 不存在，跳过该文件")
            continue

        try:
            with open(filename, 'rb') as f:
                parent, rank = pickle.load(f)
        except Exception as e:
            print(f"读取 {filename} 时出错: {e}")
            continue

        # 合并当前文件的并查集到全局并查集中
        for node in parent:
            add(global_parent, global_rank, node)
            union(global_parent, global_rank, node, parent[node])

    return global_parent, global_rank

t1=time.time()
# 指定要从中读取并查集的文件列表
# start_block = 11000001
# end_block = 11000003
start_block = 0
end_block = 2
# Generate the file list using a loop
file_list = []
for i in range(start_block, end_block + 1):
    file_list.append(f"E:\\code\\jiaoben2\\union_find\\aaa\\block_{i}_disjoint_set.pkl")

# 执行并查集的读取和融合操作
global_parent, global_rank = merge_from_files(file_list)

# 查找全局连通分量
global_components = {}
for node in global_parent:
    root = find(global_parent, global_rank,node)
    if root not in global_components:
        global_components[root] = []
    global_components[root].append(node)

# 输出全局并查集的状态
print("\n融合后的全局并查集父指针 (parent) 数组:")
for node in global_parent:
    print(f"Node {node}: Parent {global_parent[node]}")

print("\n融合后的全局并查集秩 (rank) 数组:")
for node in global_rank:
    print(f"Node {node}: Rank {global_rank[node]}")

print("\n融合后的全局连通分量:")
for component in global_components.values():
    print(component)




def find_component(node, global_parent, global_rank):
    if node not in global_parent:
        return []
    root = find(global_parent, global_rank, node)
    component = []
    for key, value in global_parent.items():
        if find(global_parent, global_rank, key) == root:
            component.append(key)
    return component

node_to_find = '11a'
component = find_component(node_to_find, global_parent, global_rank)
print(f"节点 1 所属的连通分量为: {component}")
t2=time.time()
print(t2-t1)