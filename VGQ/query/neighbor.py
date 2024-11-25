from py2neo import Graph, Node, Relationship
import time
# 输出查询结果
# def print_rec(q):
#     for record in q:
#         print(record)
#
def print_rec(q):
    records = list(q)
    if len(records) == 0:
        print("No records found.")
    else:
        for record in records:
            print(record)
def print_rec3(q):
    records = list(q)
    if len(records) == 0:
        print("No records found.")
    else:
        print("found.")
list_t=[]
l1 = 500
start_time = time.time()
graph = Graph('bolt://172.19.8.25:7687/', auth=('neo4j', '141001'))
with open("../query/output_sim1000.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number, line in enumerate(file1, start=1):
        print(line_number)
        if line_number <= l1:


            #55555555555555555aaa
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' AND r1.gas>\'200000\' WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11000101\' AND r2.gas>\'200000\' WITH DISTINCT Node2 '
            #     # 'MATCH (Node2)-[r3:node_tx_node]->(Node3) WHERE r3.block_number<\'11001001\' AND r3.gas>\'200000\' WITH DISTINCT Node3 '
            #     # 'MATCH (Node3)-[r4:node_tx_node]->(Node4) WHERE r4.block_number<\'11001001\' AND r4.gas>\'200000\' WITH DISTINCT Node4 '
            #     'MATCH (Node2)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\' AND r5.gas>\'200000\' RETURN DISTINCT endNode'
            #     , s=line.strip())
            # 5a
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\'  WITH DISTINCT Node1 '
            #     # 'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11001001\'  WITH DISTINCT Node2 '
            #     # 'MATCH (Node2)-[r3:node_tx_node]->(Node3) WHERE r3.block_number<\'11001001\'  WITH DISTINCT Node3 '
            #     'MATCH (Node1)-[r4:node_tx_node]->(Node4) WHERE r4.block_number<\'11000101\'  WITH DISTINCT Node4 '
            #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\'  RETURN DISTINCT endNode'
            #     , s=line.strip())





            q3=graph.run(
                'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' RETURN DISTINCT Node1 '
                , s=line.strip())
            # q3=graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' RETURN DISTINCT Node1 '
            #     , s=line.strip())
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' AND r1.gas>\'200000\' WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(endNode) WHERE r2.block_number<\'11001001\' AND r2.gas>\'200000\' RETURN DISTINCT endNode'
            #     , s=line.strip())
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\'  WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(endNode) WHERE r2.block_number<\'11001001\'  RETURN DISTINCT endNode'
            #     , s=line.strip())
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' AND r1.gas>\'200000\' WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11001001\' AND r2.gas>\'200000\' WITH DISTINCT Node2 '
            #     'MATCH (Node2)-[r3:node_tx_node]->(endNode) WHERE r3.block_number<\'11001001\' AND r3.gas>\'200000\' RETURN DISTINCT endNode'
            #     , s=line.strip())


            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\'  WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11001001\' WITH DISTINCT Node2 '
            #     'MATCH (Node2)-[r3:node_tx_node]->(endNode) WHERE r3.block_number<\'11001001\' RETURN DISTINCT endNode'
            #     , s=line.strip())


            #444444444aaaaaaaaaa
            # q3 = graph.run(
            #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' AND r1.gas>\'200000\' WITH DISTINCT Node1 '
            #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11001001\'  AND r2.gas>\'200000\' WITH DISTINCT Node2 '
            #     'MATCH (Node2)-[r3:node_tx_node]->(Node3) WHERE r3.block_number<\'11001001\' AND r3.gas>\'200000\' WITH DISTINCT Node3 '
            #     'MATCH (Node3)-[r4:node_tx_node]->(endNode) WHERE r4.block_number<\'11001001\' AND r4.gas>\'200000\' RETURN DISTINCT endNode'
            #     , s=line.strip())


            #4444444
            # q3 = graph.run(
            #     'MATCH (sourceNode:user)-[r1:node_tx_node]->(Node1:user) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\'  WITH DISTINCT Node1 '
            #     'MATCH (Node1:user)-[r2:node_tx_node]->(Node2:user) WHERE r2.block_number<\'11001001\'   WITH DISTINCT Node2 '
            #     'MATCH (Node2:user)-[r3:node_tx_node]->(Node3:user) WHERE r3.block_number<\'11001001\'  WITH DISTINCT Node3 '
            #     'MATCH (Node3:user)-[r4:node_tx_node]->(endNode:user) WHERE r4.block_number<\'11001001\'  RETURN DISTINCT endNode'
            #     , s=line.strip())




            # q3 = graph.run(
            #     'MATCH (sourceNode)-[:node_tx_node]->(Node1) WHERE sourceNode.node = $s WITH Node1 '
            #     'MATCH (Node1)-[:node_tx_node]->(Node2) WITH Node2 '
            #     'MATCH (Node2)-[:node_tx_node]->(Node3) WITH Node3 '
            #     'MATCH (Node3)-[:node_tx_node]->(Node4) WITH Node4 '
            #     'MATCH (Node4)-[:node_tx_node]->(endNode) RETURN count(endNode)'
            #      ,s=line.strip())
            # print_rec3(q3)

            if line_number % 100 == 0:
                wo_time = time.time()
                # print("time:", wo_time, "seconds")
                tim = wo_time - start_time
                print("Execution time:", tim, "seconds")
                list_t.append(tim)
        else:
            break
end_time = time.time()
execution_time = end_time - start_time
print("Execution time:", execution_time, "seconds")


for t in list_t:
    print(t)


