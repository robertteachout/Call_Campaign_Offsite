import pandas as pd
import numpy as np
import time

def rank(df, grouping='PhoneNumber'):
    df.sort_values(['Skill', grouping,'meet_sla','temp_rank', 'no_call', 'togo_bin', 'age']
               ,ascending=[True, True, True, False, False, False, False], inplace=True)
    df['overall_rank'] = 1
    df['overall_rank'] = df.groupby(['Skill', grouping,])['overall_rank'].cumsum()
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)
    df.sort_values(['Skill', 'parent','meet_sla','temp_rank', 'no_call', 'togo_bin', 'age']
                ,ascending=[True, True, True, False, False, False, False], inplace=True)
    df.Score = 1
    df.Score = df.groupby(['Skill', 'parent'])['Score'].cumsum()
    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    # new score
    df['OutreachID'] = df['OutreachID'].astype(str)
    f0 = df.Project_Type.isin(['UHC HEDIS','HEDIS']) # 'ACA-PhysicianCR'
    df['temp_rank'] = np.where(f0, 1, 0)

    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored = rank(notmsid)
    msid_scored = rank(msid, grouping='mastersiteID')

    dubs = scored.append(msid_scored)
    unique = dubs.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    return unique
        
def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)