# 重新定义函数，以确保可以处理整个数据集并且直接修改原DataFrame
import pandas as pd

def calculate_metrics_complete(df):
    # 对DataFrame进行排序以确保连续性
    df.sort_values(by=['match_id', 'set_no', 'game_no', 'point_no'], inplace=True)
    df['p1_Momentum'] = 0  # 动量
    df['score_gap']=0

    # 首先按match_id进行分组
    for _, match_group in df.groupby(['match_id']):
        match_indices = match_group.index
        first_set_in_match = True  # 标记是否为match中的第一个set

        # 然后在每个match_id内部按set_no进行分组
        for _, set_group in match_group.groupby(['set_no']):
            p1_consecutive = p2_consecutive = 0
            p1_point=0
            p2_point=0
            M_previous = 0 if first_set_in_match else df.at[set_group.index[0] - 1, 'p1_Momentum'] 
             # 如果是match的第一个set，则初始化M_previous为0

            for i in set_group.index:
                row = df.loc[i]
                
                # 计算S_i
                if row['server'] == 1:
                    S_i = 1.17 
                else:   
                    S_i = 1
                
                # 更新连胜场数
                if row['point_victor'] == 1:
                    p1_consecutive += 1
                    p2_consecutive = 0
                    p1_point+=1
                elif row['point_victor'] == 2:
                    p2_consecutive += 1
                    p1_consecutive = 0
                    p2_point+=1
                
                # 计算C_t
                C_t = 1 + 0.2 * max(p1_consecutive, p2_consecutive)  # 计算C_t
                #计算当前set的分差
                df.at[i, 'score_gap'] = p1_point-p2_point
                # 更新得分情况
                P_i = 1 if row['point_victor'] == 1 else -1

                # 特殊情况处理
                if first_set_in_match and i == match_indices[0]:  # 如果是match的第一个set的开始
                    M_previous = 0
                
                if i == set_group.index[0] and not first_set_in_match:  # 如果是set的开始但不是match的开始
                    M_previous *= 0.5
                
                # 计算当前点的势头值
                M_i = 0.9*M_previous + S_i * C_t * P_i-0.2*df.at[i, 'p1_double_fault']-0.1*df.at[i, 'p2_break_pt_won']+0.1*df.at[i, 'p1_break_pt_won']
                df.at[i, 'p1_Momentum'] = M_i

                # 为下一点更新M_previous
                M_previous = M_i

            first_set_in_match = False  # 处理完第一个set后更新标记

    return df

# 读取数据集
file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches = pd.read_csv(file_path,encoding='latin1')

wimbledon_matches_complete = calculate_metrics_complete(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
processed_file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path
