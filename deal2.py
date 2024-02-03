import pandas as pd
# 定义一个函数，用于计算比赛的连续性指标




def calculate_metrics_complete(df):
    # 对DataFrame进行排序以确保连续性
    df.sort_values(by=['match_id', 'set_no', 'game_no', 'point_no'], inplace=True)
    df['ad_change'] = 0 # 初始化ad_change列

    # 遍历数据，比较p1_Momentum和p2_Momentum的大小，使得当相对大小改变时，ad_change为1
    match_ids = df['match_id'].unique()
    for match_id in match_ids:
        match_df = df[df['match_id'] == match_id]
        match_df.reset_index(drop=True, inplace=True)
        for i in range(1, len(match_df)):
            if i > 0 and i < len(match_df):  # 确保不越界
                if (match_df.at[i, 'p1_Momentum'] > match_df.at[i, 'p2_Momentum'] and 
                    match_df.at[i - 1, 'p1_Momentum'] <= match_df.at[i - 1, 'p2_Momentum']):
                    df.at[match_df.index[i], 'ad_change'] = 1
                elif (match_df.at[i, 'p1_Momentum'] < match_df.at[i, 'p2_Momentum'] and 
                      match_df.at[i - 1, 'p1_Momentum'] >= match_df.at[i - 1, 'p2_Momentum']):
                    df.at[match_df.index[i], 'ad_change'] = 1

    return df

# 读取数据集
file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches = pd.read_csv(file_path, encoding='latin1')

wimbledon_matches_complete = calculate_metrics_complete(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
processed_file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path
