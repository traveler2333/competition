# 重新定义函数，以确保可以处理整个数据集并且直接修改原DataFrame
import pandas as pd

def calculate_metrics_complete(df):
    # 对DataFrame进行排序以确保连续性
    df.sort_values(by=['match_id', 'set_no', 'game_no', 'point_no'], inplace=True)

    # 初始化计数器和胜率变量

    df['p1_win_rate'] = 0.5 # 球员1的胜率（实力） match
    df['p2_win_rate'] = 0.5 # 球员2的胜率（实力） match
  

    df['p1_win']=0 # 球员1胜 match
    df['p2_win']=0 # 球员2胜 match
    df['p1_next_sever']=0 # 球员1发球 match

    df['p1_lead']= 0 # 球员1的领先局数 game
    df['p2_lead']= 0 # 球员2的领先局数 game

    df['p1_consecutive_wins'] = 0 # 球员1的连胜场数 game
    df['p2_consecutive_wins'] = 0 # 球员2的连胜场数 game

    df['dis_gap'] = 0 #局内已跑动距离差 game
    df['next_1win'] = 0 #下一局谁赢 game

    df['useless'] = 0 #无用行 match

    for _,group in df.groupby(['match_id']):
        indices = group.index
        for i in indices:
            
            row = df.loc[i]
            #更新发球方
            if df.at[i,"server"]==1:
                df.at[i,"p1_sever"] = 1
            else:
                df.at[i,"p1_sever"] = 0

            if i != indices[-1]:
                if df.at[i+1,"point_victor"]==1 :
                    df.at[i,"next_1win"] = 1
                else:
                    df.at[i,"next_1win"] = 0
                df.at[i,"p1_next_sever"] = 1 if df.at[i+1,"server"]==1 else 0
                df.at[i,"useless"] = 0
            else :
                df.at[i,"useless"] = 1

            #更新胜率
            df.at[i, 'p1_win_rate'] = df.at[i, 'p1_points_won']/(df.at[i, 'p1_points_won']+df.at[i, 'p2_points_won']) 
            df.at[i, 'p2_win_rate'] = df.at[i, 'p2_points_won']/(df.at[i, 'p1_points_won']+df.at[i, 'p2_points_won'])
            df.at[i, 'p1_win'] = 1 if df.at[i, 'point_victor']==1 else 0
            df.at[i, 'p2_win'] = 1 if df.at[i, 'point_victor']==2 else 0

            #更新赛点局
            if((df.at[i, 'p1_score'] == "AD") or df.at[i, 'p2_score'] == "AD"):
                df.at[i, 'is_important'] = 1
            else:
                df.at[i, 'is_important'] = 0


    # 使用groupby遍历每个match_id, set_no, 和game_no的组合，确保独立计算每一局的统计数据
    for _, group in df.groupby(['match_id', 'set_no', 'game_no']):
        # 获取当前组的索引
        indices = group.index

        # 初始化局内连胜场数和得分
        p1_consecutive = p2_consecutive = 0
        p1_game_won = p2_game_won = 0 
        a = b= 0
        # 遍历当前组的每一行
        for i in indices:
            row = df.loc[i]
            #不更新最后一个索引的数据
            if i == indices[-1]:
                df.at[i,"useless"] = 1
                break

            if row['point_victor'] == 1:
                p1_consecutive += 1
                p2_consecutive = 0
                p1_game_won += 1
            elif row['point_victor'] == 2:
                p2_consecutive += 1
                p1_consecutive = 0
                p2_game_won += 1

                #更新局内已跑动距离差
            a+=df.at[i,"p1_distance_run"]
            b+=df.at[i,"p2_distance_run"]

                #更新局内已跑动距离
            df.at[i,"dis_gap"]=a-b

            # 更新连胜场数
            df.at[i, 'p1_consecutive_wins'] = p1_consecutive
            df.at[i, 'p2_consecutive_wins'] = p2_consecutive

            # 更新局内领先局数
            df.at[i, 'p1_lead'] = p1_game_won - p2_game_won
            df.at[i,'p2_lead'] = p2_game_won - p1_game_won
            

    return df

# 读取数据集
file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches = pd.read_csv(file_path,encoding='latin1')

wimbledon_matches_complete = calculate_metrics_complete(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
processed_file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path
