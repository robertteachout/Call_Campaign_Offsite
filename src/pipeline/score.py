import pandas as pd
import numpy as np

def rank(df):
    return df.sort_values(
        by = ['meet_sla','togo_bin', 'age'], 
        ascending=[True,True, False]
        ).reset_index(drop = True)

def split(df, sk):
    # new score
    df0 = df[df['Skill'] == sk].copy()
    scored = rank(df0)
    non_dup = scored.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
    df_skill = rank(non_dup)
    
    df_skill['Unique_Phone'] = 1
    ### add score column
    df_skill['Score'] = range(len(df_skill))
    # Add Unique ORGs to Rank list 
    df5 = df_skill.append(scored)
    df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    df6['OutreachID'] = df6['OutreachID'].astype(str)
    df6['Matchees'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df6
        
def split_drop_score(df):
    df_score_split = pd.DataFrame()
    ### Sort Order and drop Dups
    for i in df['Skill'].unique():
        df_score_split = df_score_split.append(split(df, i))
    return df_score_split