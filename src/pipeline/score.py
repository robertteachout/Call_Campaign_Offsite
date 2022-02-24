import pandas as pd
import numpy as np


def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def stack_inventory(df, grouping):
    rank_cols = {'meet_target_sla':True, 'temp_rank':False, 'temp_rank2':False,'no_call':False,'age':False} # 'togo_bin':False, 
    if grouping == 'PhoneNumber':
        f0 = df.Project_Type.isin(['HEDIS','Chart Review']) # 'ACA-PhysicianCR'
        df['temp_rank2'] = np.where(f0, 1, 0)

    # group by phone number or msid & rank highest value org
    df = rank(df,'overall_rank',['Skill',grouping], rank_cols)

    # top overall per group = parent
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    df = rank(df,'Score'       , ['Skill','parent'], rank_cols)
    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    df['OutreachID'] = df['OutreachID'].astype(str)
    # f0 = df.Project_Type.isin(['UHC HEDIS','ACA-PhysicianCR']) # 'ACA-PhysicianCR'
    f0 = df.Project_Type.isin(['UHC HEDIS']) # 'ACA-PhysicianCR'
    f1 = df.Project_Type.isin(['ACA-PhysicianCR']) # 'ACA-PhysicianCR'
    df['temp_rank'] = np.where(f0, 1, 0)
    df['temp_rank2'] = np.where(f1, 1, 0)

    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid, 'PhoneNumber')
    msid_scored = stack_inventory(msid, 'MasterSiteId')

    dubs = scored.append(msid_scored)
    unique = dubs.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    f0 = df.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    df['Score'] = np.where(f0, 1000000, df.Score)
    return unique
        
def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)