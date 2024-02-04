import pandas as pd

# 读取原始文件
file_path = '111.csv'
data = pd.read_csv(file_path, encoding='latin1')

# 截取前 97491 行保存到 111.csv 文件
data_111 = data[:97491]
data_111.to_csv('111.csv', index=False)

# 剩余行与标题一起保存到 222.csv 文件
data_222 = data[97491:]
data_222.to_csv('222.csv', index=False)
