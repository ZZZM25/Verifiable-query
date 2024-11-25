import csv
from py2neo import Graph, Node, Relationship
import networkx as nx
import numpy as np
import time


### 建立图

gragh = Graph('bolt://172.19.8.25:7687/', auth=('neo4j', '141001'))
# with open('/nfs/srv/data2/zmy/data.csv',encoding='utf-8',newline='')as cs:
#with open('C:\\Users\\87778\\Desktop\\data.csv',encoding='utf-8',newline='')as cs:
time_start = time.time()
with open('/nfs/srv/data2/zmy/data/tron_import.csv', encoding='utf-8', newline='') as cs:
# with open('/nfs/srv/data2/zmy/data/eth_import.csv', encoding='utf-8', newline='') as cs:
    reader=csv.reader(cs)
    next(reader, None)
    # 循环遍历reader对象，
    for i in reader:
        if i:
            gragh.run('MERGE (n1:user {node:$a})',a=i[5])
            gragh.run('MERGE (n2:user {node:$a})', a=i[6])
            gragh.run(
                'MATCH (a:user {node:$a}), (b:user {node:$b}) CREATE (a)-[:node_tx_node{block_num:$c1,index:$c2,signature:$c3,txID:$c4,amount:$c5,expiration:$c6,timestamp:$c7,raw_data_hex:$c8}]->(b)',
                a=i[5], b=i[6], c1=i[0], c2=i[1], c3=i[2], c4=i[3], c5=i[4], c6=i[7], c7=i[8], c8=i[9])
            # gragh.run(
            #     'MATCH (a:user {node:$a}), (b:user{node:$b}) CREATE (a)-[:node_tx_node{hash:$c1,block_number:$c2,transaction_index:$c3,value:$c4,gas:$c5,gas_price:$c6,input:$c7,block_timstamp:$c8}]->(b)',
            #     a=i[5], b=i[6], c1=i[0], c2=i[1], c3=i[3], c4=i[4], c5=i[7], c6=i[8], c7=i[9], c8=i[10])

            time_end = time.time()
print(time_end - time_start)

print('build!')




