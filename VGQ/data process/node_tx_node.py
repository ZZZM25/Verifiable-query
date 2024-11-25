import pandas as pd

# 读取CSV文件
# df = pd.read_csv('C:\\Users\\87778\\Desktop\\transactions.csv')
# df = pd.read_csv('E:\\code\\jiaoben\\py111\\15000.csv')
# df = pd.read_csv('E:\\code\\jiaoben2\\1tron\\3m.csv')
df = pd.read_csv('E:\\code\\jiaoben2\\1tron\\13_15.csv')
# df = pd.read_csv('/tocsv_only_tx/fvv.csv')

# df = pd.read_csv('E:/code/jiaoben2/tocsv_only_tx/f.csv')
# 提取需要的列
# column_4 = df.iloc[:, 0].astype(str)
# column_5 = df.iloc[:, 2].astype(str)
column_6 = df.iloc[:, 5]
column_7 = df.iloc[:, 6]

# 检查第6列和第7列数据是否不为空且不相等
mask = (~column_6.isnull()) & (~column_7.isnull()) & (column_6 != column_7)

# 根据条件筛选数据
df_filtered = df[mask].copy()  # 创建副本以避免警告

# # 创建新列1
# df_filtered.loc[:, 'node:START_ID'] = column_6
# df_filtered.loc[:, 'TX:END_ID'] = column_4 + '_' + column_5
# df_filtered.loc[:, ':TYPE'] = 'node_tx_in'
#
# # 仅保留指定列
# df_result1 = df_filtered[['node:START_ID','TX:END_ID',':TYPE']]
#
# # 保存到新文件
# df_result1.to_csv('node_tx_in.csv', index=False)

# # 创建新列2
# df_filtered.loc[:, 'TX:START_ID'] = column_4 + '_' + column_5
# df_filtered.loc[:, 'node:END_ID'] = column_7
# df_filtered.loc[:, ':TYPE'] = 'node_tx_out'
#
# # 仅保留指定列
# df_result2 = df_filtered[['TX:START_ID', 'node:END_ID', ':TYPE']]
#
# # 保存到新文件
# df_result2.to_csv('node_tx_out.csv', index=False)
#
#
#
#
# 创建新列3
df_filtered.loc[:, 'node:START_ID'] = column_6
df_filtered.loc[:, 'node:END_ID'] = column_7
df_filtered.loc[:, ':TYPE'] = 'node_tx_node'
# df_filtered.loc[:, 'BT'] = column_4 + '_' + column_5

# 仅保留指定列
columns_to_keep = list(range(0,5))+list(range(7, 9))  # 保留第2到第19列和第21列
df_result3 = pd.concat([df_filtered[['node:START_ID', 'node:END_ID', ':TYPE']], df_filtered.iloc[:, columns_to_keep]], axis=1)

# df_result3 = df_filtered[['TX:START_ID', 'node:END_ID', ':TYPE']]

# 保存到新文件
df_result3.to_csv('node_tx_node.csv', index=False)






# import pandas as pd
#
# # 读取CSV文件
# df = pd.read_csv('C:\\Users\\87778\\Desktop\\transactions.csv')
#
# # 检查第6列和第7列数据是否不为空
#
# mask = (~df.iloc[:, 5].isnull()) & (~df.iloc[:, 6].isnull()) & (df.iloc[:, 5] != df.iloc[:, 6])
#
#
#
# # df['TX:START_ID'] = df.iloc[:, 3][mask].astype(str) + '_' + df.iloc[:, 4][mask].astype(str)
# # df['node:END_ID'] = df.iloc[:, 6][mask]
# # df[':TYPE'] = ''  # 初始化新列，可以是空字符串或其他合适的默认值
# # df.loc[mask, ':TYPE'] = 'out_tx'
# # df = df[['TX:START_ID','node:END_ID',':TYPE']]
#
#
# df['TX:START_ID'] = df.iloc[:, 3].astype(str) + '_' + df.iloc[:, 4].astype(str)
# df['node:END_ID'] = df.iloc[:, 6]
# df[':TYPE'] = 'out_tx'
# df = df[['TX:START_ID','node:END_ID',':TYPE']]
# df=df[mask]
#
#
# # 保存到新文件
# df.to_csv('out_tx.csv', index=False)