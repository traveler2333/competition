import pandas as pd
# 定义一个函数，用于在DataFrame中添加序号







def add_row_numbers(df):
    """
    在DataFrame中添加行号作为第一列

    参数：
    df : pandas.DataFrame
        要添加行号的DataFrame

    返回：
    pandas.DataFrame
        添加行号后的DataFrame副本
    """
    # 创建行号序列
    row_numbers = range(1, len(df) + 1)

    # 插入行号列到DataFrame的第一列
    df_with_row_numbers = df.copy()
    df_with_row_numbers.insert(0, 'time', row_numbers)

    return df_with_row_numbers

# 读取数据集
file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_processed = pd.read_csv(file_path,encoding='latin1')

# 添加行号
wimbledon_matches_with_row_numbers = add_row_numbers(wimbledon_matches_processed)

# 保存处理后的数据集到一个新文件
processed_with_row_numbers_file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_with_row_numbers.to_csv(processed_with_row_numbers_file_path, index=False)

processed_with_row_numbers_file_path
