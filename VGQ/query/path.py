from py2neo import Graph, Node, Relationship
import time

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

l1 = 500
list_t=[]
start_time = time.time()
graph = Graph('bolt://172.19.8.25:7687/', auth=('neo4j', '141001'))
with open("../query/output_sim100.txt", "r") as file1:
# with open("../query/output_sim500.txt", "r") as file1:
# with open("../query/output_sim1000.txt", "r") as file1:
# with open("../query/output_6.txt", "r") as file1:
    # 逐行读取并输出每一行
    for line_number, line in enumerate(file1, start=1):
        print(line_number)
        if line_number <= l1:
            if line_number >= 1:
                # start_time = time.time()
                q3 = graph.run(
                    'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' '
                    'RETURN r1.transaction_index AS d1'
                    , s=line.strip())
                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' AND r1.gas>\'200000\' '
                #     'RETURN r1.transaction_index AS d1'
                #     , s=line.strip())

                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' AND r1.gas>\'200000\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11001001\' AND r5.gas>\'200000\' RETURN d1,r5.transaction_index AS d5'
                #     , s=line.strip())
                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11001001\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11001001\' RETURN d1,r5.transaction_index AS d5'
                #      ,s=line.strip())



                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11000101\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\' RETURN d1,d2,r5.transaction_index AS d5'
                #      ,s=line.strip())
                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' AND r1.gas>\'200000\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11000101\' AND r2.gas>\'200000\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\' AND r5.gas>\'200000\' RETURN d1,d2,r5.transaction_index AS d5'
                #      ,s=line.strip())



                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' AND r1.gas>\'200000\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WHERE r2.block_number<\'11000101\' AND r2.gas>\'200000\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3) WHERE r3.block_number<\'11000101\' AND r3.gas>\'200000\' WITH DISTINCT Node3,d1,d2,r3.transaction_index AS d3 '
                #     'MATCH (Node3)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\' AND r5.gas>\'200000\' RETURN d1,d2,d3,r5.transaction_index AS d5'
                #      ,s=line.strip())
                # wo_time0 = time.time()


                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2)  WHERE r2.block_number<\'11000101\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3)  WHERE r3.block_number<\'11000101\' WITH DISTINCT Node3,d1,d2,r3.transaction_index AS d3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(endNode) WHERE r4.block_number<\'11000101\' RETURN d1,d2,d3,d4,r4.transaction_index AS d4'
                #     , s=line.strip())




                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\'  AND r1.gas>\'200000\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2)  WHERE r2.block_number<\'11000101\'  AND r2.gas>\'200000\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3)  WHERE r3.block_number<\'11000101\'   AND r3.gas>\'200000\' WITH DISTINCT Node3,d1,d2,r3.transaction_index AS d3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(Node4)  WHERE r4.block_number<\'11000101\'   AND r4.gas>\'200000\' WITH DISTINCT Node4,d1,d2,d3,r4.transaction_index AS d4 '
                #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\'  AND r5.gas>\'200000\'  RETURN d1,d2,d3,d4,r5.transaction_index AS d5'
                #     , s=line.strip())


                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\'  WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2)  WHERE r2.block_number<\'11000101\'  WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3)  WHERE r3.block_number<\'11000101\'   WITH DISTINCT Node3,d1,d2,r3.transaction_index AS d3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(Node4)  WHERE r4.block_number<\'11000101\'   WITH DISTINCT Node4,d1,d2,d3,r4.transaction_index AS d4 '
                #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\'   RETURN d1,d2,d3,d4,r5.transaction_index AS d5'
                #      ,s = line.strip())

                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) '
                #     'WHERE sourceNode.node = $s AND r1.block_number < \'11001001\' '
                #     'WITH sourceNode, Node1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) '
                #     'WHERE r2.block_number < \'11001001\' '
                #     'WITH sourceNode, Node1, Node2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3) '
                #     'WHERE r3.block_number < \'11001001\' '
                #     'WITH sourceNode, Node1,Node2,Node3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(Node4) '
                #     'WHERE r4.block_number < \'11001001\' '
                #     'WITH sourceNode, Node1, Node2, Node3, Node4 '
                #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) '
                #     'WHERE r5.block_number < \'11001001\' '
                #     'RETURN sourceNode, Node1, Node2, Node3, Node4, endNode',
                #     s=line.strip()
                # )

                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s AND r1.block_number<\'11000101\'  AND r1.gas>\'200000\' WITH DISTINCT Node1,r1.transaction_index AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2)  WHERE r2.block_number<\'11000101\'  AND r2.gas>\'200000\' WITH DISTINCT Node2,d1,r2.transaction_index AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3)  WHERE r3.block_number<\'11000101\'  AND r3.gas>\'200000\' WITH DISTINCT Node3,d1,d2,r3.transaction_index AS d3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(Node4)  WHERE r4.block_number<\'11000101\'  AND r4.gas>\'200000\' WITH DISTINCT Node4,d1,d2,d3,r4.transaction_index AS d4 '
                #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) WHERE r5.block_number<\'11000101\'  AND r5.gas>\'200000\' RETURN d1,d2,d3,d4,r5.transaction_index AS d5'
                #     , s=line.strip())
                # wo_time1 = time.time()
                # tim = wo_time1 - wo_time0
                # print("Execution time:", tim, "seconds")

                # q3 = graph.run(
                #     'MATCH (sourceNode)-[r1:node_tx_node]->(Node1) WHERE sourceNode.node = $s WITH DISTINCT Node1,id(r1) AS d1 '
                #     'MATCH (Node1)-[r2:node_tx_node]->(Node2) WITH DISTINCT Node2,d1,id(r2) AS d2 '
                #     'MATCH (Node2)-[r3:node_tx_node]->(Node3) WITH DISTINCT Node3,d1,d2,id(r3) AS d3 '
                #     'MATCH (Node3)-[r4:node_tx_node]->(Node4) WITH DISTINCT Node4,d1,d2,d3,id(r4) AS d4 '
                #     'MATCH (Node4)-[r5:node_tx_node]->(endNode) RETURN d1,d2,d3,d4,id(r5) AS d5'
                #      ,s=line.strip())

                print_rec(q3)
                if line_number % 100 == 0:
                    wo_time = time.time()
                    # print("time:", wo_time, "seconds")
                    tim = wo_time - start_time
                    print("Execution time:", tim, "seconds")
                    list_t.append(tim)
                # end_time = time.time()
                # execution_time = end_time - start_time
                # print("Execution time:", execution_time, "seconds")

        else:
            break

for t in list_t:
    print(t)






