import pandas as pd

# Step 1: 加载CSV文件
file_path = 'E:\\code\\jiaoben2\\1tron\\13_15.csv'
# file_path = 'E:\\code\\jiaoben2\\_data\\window100.csv'

df = pd.read_csv(file_path)

# Step 2: 筛选 block_num 小于等于 170000 的行
# filtered_df = df[(df['block_num'] > 1400000) & (df['block_num'] <= 1500000)]
# filtered_df = df[df['block_num'] <= 1340000]
# # filtered_df = df[df['block_num'] <= 1380000]
# filtered_df = df[df['block_num'] <= 1420000]
filtered_df = df[df['block_num'] <= 1300750]
# filtered_df = df[df['block_number'] <= 11000001]
# Step 3: 保存结果到一个新的CSV文件
filtered_file_path = '23123/tron750.csv'  # 你可以指定新的文件名和路径
filtered_df.to_csv(filtered_file_path, index=False)

print(f"已将筛选后的数据保存到: {filtered_file_path}")
