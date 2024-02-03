import pandas as pd
#合并p1和p2的指标，因为转折不分1、2的






def add_row_numbers(df):
    df['ace']=0
    df['winner']=0
    df['double_fault']= 0
    df['unf_err']= 0
    df['break_pt_won']= 0
    df.sort_values(by=['match_id', 'set_no', 'game_no', 'point_no'], inplace=True)

    for _,group in df.groupby(['match_id']):
        indices = group.index
        for i in indices:
         #遍历数据集
            if i>indices[0]:
                df.at[i,'ace'] = df.at[i-1,'p1_ace']+df.at[i-1,'p2_ace']
                df.at[i,'winner'] = df.at[i-1,'p1_winner']+df.at[i-1,'p2_winner']
                df.at[i,'double_fault'] = df.at[i-1,'p1_double_fault']+df.at[i-1,'p2_double_fault']
                df.at[i,'unf_err'] = df.at[i-1,'p1_unf_err']+df.at[i-1,'p2_unf_err']
                df.at[i,'break_pt_won'] = df.at[i-1,'p1_break_pt_won']+df.at[i-1,'p2_break_pt_won']
            else:
                df.at[i,'ace'] = 0
                df.at[i,'winner'] = 0
                df.at[i,'double_fault'] = 0
                df.at[i,'unf_err'] = 0
                df.at[i,'break_pt_won'] = 0
        return df

# 读取数据集
file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches = pd.read_csv(file_path,encoding='latin1')

wimbledon_matches_complete = add_row_numbers(wimbledon_matches.copy())

# 保存处理后的数据集到一个新文件
processed_file_path = 'Wimbledon_featured_matches_processed.csv'
wimbledon_matches_complete.to_csv(processed_file_path, index=False)

processed_file_path
