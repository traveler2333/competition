import pandas as pd
#处理女子比赛
#合并p1和p2的指标，因为转折不分1、2的






def add_row_numbers(df):

    df['ad_next_change']= 0
    df.sort_values(by=['match_id', 'set_no', 'point_no'], inplace=True)

    for _,group in df.groupby(['match_id']):
        indices = group.index
        for i in indices:
         #遍历数据集
            if i!=indices[-1]:
                df.at[i,'ad_next_change']=df.at[i+1,'ad_change']

    return df

# 读取数据集
file_path = '222.csv'
wimbledon_matches = pd.read_csv(file_path,encoding='latin1')

wimbledon_matches_complete = add_row_numbers(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
processed_file_path = '222.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path
