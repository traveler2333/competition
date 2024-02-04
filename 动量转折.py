import pandas as pd

def calculate_metrics(df):

    #df.sort_values(by=['match_id', 'set_no', 'game_no', 'point_no'], inplace=True)
    df.sort_values(by=['match_id', 'set_no', 'game_n', 'point_no'], inplace=True)
    
    # 初始化ad_change1和ad_change2列
    df['ad_change'] = 0

    # 遍历每个match_id的分组
    for _, group in df.groupby(['match_id']):
        indices = group.index
        
        # 从第二个数据点开始遍历，因为我们需要至少两个数据点来比较动量的变化
        for i in range(1, len(indices)):
            # # 检查p1_Momentum的变化
            # if (group.at[indices[i], 'p1_Momentum'] - group.at[indices[i-1], 'p1_Momentum']) * \
            #    (group.at[indices[i-1], 'p1_Momentum'] - group.at[indices[i-2], 'p1_Momentum']) < 0:
            #     df.at[indices[i], 'ad_change1'] = 1
            
            # # 检查p2_Momentum的变化
            # if i > 1 and (group.at[indices[i], 'p2_Momentum'] - group.at[indices[i-1], 'p2_Momentum']) * \
            #              (group.at[indices[i-1], 'p2_Momentum'] - group.at[indices[i-2], 'p2_Momentum']) < 0:
            #     df.at[indices[i], 'ad_change2'] = 1
                        # 检查p1_Momentum的变化
            if (group.at[indices[i-1], 'p1_Momentum'] > group.at[indices[i-1], 'p2_Momentum']) and \
               (group.at[indices[i], 'p1_Momentum'] < group.at[indices[i], 'p2_Momentum']) :
                df.at[indices[i], 'ad_change'] = 1
            
            # 检查p2_Momentum的变化
            if (group.at[indices[i-1], 'p1_Momentum'] < group.at[indices[i-1], 'p2_Momentum']) and \
               (group.at[indices[i], 'p1_Momentum'] > group.at[indices[i], 'p2_Momentum']) :
                df.at[indices[i], 'ad_change'] = 1

    return df

# 假设你已经正确读取了数据到wimbledon_matches DataFrame
# 注意：你需要确保你的DataFrame名和路径是正确的，这里使用的是假设的DataFrame名
# 读取数据集
#file_path = 'Wimbledon_featured_matches_processed.csv'
file_path = '111.csv'
wimbledon_matches = pd.read_csv(file_path,encoding='latin1')

wimbledon_matches_complete = calculate_metrics(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
#processed_file_path = 'Wimbledon_featured_matches_processed.csv'
processed_file_path = '111.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path

