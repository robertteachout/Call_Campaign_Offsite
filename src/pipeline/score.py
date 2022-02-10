import pandas as pd
import numpy as np

def rank(df):
    f0 = df.Project_Type.isin(['UHC HEDIS','HEDIS']) # 'ACA-PhysicianCR'
    df['temp_rank'] = np.where(f0, 0, 1)

    return df.sort_values(['meet_sla','temp_rank', 'has_call', 'togo_bin', 'age']
               ,ascending=[True, True, True, False, False]).reset_index(drop=True)

def split(df, sk):
    # new score
    df0 = df[df['Skill'] == sk].copy()
    scored = rank(df0)
    
    f1 = scored.Project_Type == 'Chart Sync'
    scored.Score = np.where(f1, 100000, scored.Score)

    grouping = 'mastersiteID' if sk == 'mastersite_inventory' else 'PhoneNumber'

    non_dup = scored.drop_duplicates([grouping]).reset_index(drop = True)
    df_skill = rank(non_dup)

    f1 = df_skill.Project_Type == 'Chart Sync'
    df_skill.Score = np.where(f1, 100000, df_skill.Score)

    df_skill['Unique_Phone'] = 1
    ### add score column
    df_skill['Score'] = range(len(df_skill))

    # Add Unique ORGs to Rank list 
    df5 = df_skill.append(scored)
    df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    df6['OutreachID'] = df6['OutreachID'].astype(str)
    df6['Matchees'] = df6.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df6
        
def split_drop_score(df):
    df_score_split = pd.DataFrame()
    ### Sort Order and drop Dups
    for i in df['Skill'].unique():
        df_score_split = df_score_split.append(split(df, i))
    return df_score_split