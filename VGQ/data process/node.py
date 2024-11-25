import pandas as pd

# 读取CSV文件
# df = pd.read_csv('C:\\Users\\87778\\Desktop\\transactions.csv')
df = pd.read_csv('E:\\code\\jiaoben2\\1tron\\13_15.csv')
# df = pd.read_csv('E:\\code\\jiaoben2\\tocsvv\\fvv.csv')
# df = pd.read_csv('E:/code/jiaoben2/tocsv_only_tx/f.csv')
# 检查第6列和第7列数据是否不为空
mask = (~df.iloc[:, 5].isnull()) & (~df.iloc[:, 6].isnull())

# 组合第6列和第7列数据并去重
combined_column = pd.concat([df.iloc[:, 5][mask], df.iloc[:, 6][mask]]).drop_duplicates().reset_index(drop=True)

# # 组合第6列和第7列数据并去重
# combined_column = pd.concat([df.iloc[:, 5], df.iloc[:, 6]]).drop_duplicates().reset_index(drop=True)

# 创建新的DataFrame来存储组合后的数据
result_df = pd.DataFrame({'node:ID': combined_column, ':LABEL': 'user'})

# 打印结果或保存到新的CSV文件
result_df.to_csv('node.csv', index=False)