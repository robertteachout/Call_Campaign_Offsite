import pandas as pd
import numpy as np


def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def project_rank(df, project, rank):
    f1 = df.Project_Type == project # 'ACA-PhysicianCR'
    df[f'temp_rank{rank}'] = np.where(f1, 1, 0)
    return df

def stack_inventory(df, grouping):
    projects = ['UHC HEDIS', 'ACA-PhysicianCR', 'HEDIS', 'Chart Review']
    temp = {}
    for index, project in enumerate(projects):
        # create column on dataframe
        df = project_rank(df, project, index)
        # create dict for scoring
        temp[f'temp_rank{index}'] = False

    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False, 'togo_bin':False, 'age':False} 
    # group by phone number or msid & rank highest value org
    df = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    df = rank(df,'Score', ['Skill','parent'], rank_cols)

    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    df['OutreachID'] = df['OutreachID'].astype(str)

    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid, 'PhoneNumber')
    msid_scored = stack_inventory(msid, 'MasterSiteId')

    dubs = scored.append(msid_scored)
    unique = dubs.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    unique['Score'] = np.where(f0, 1000000, unique.Score)
    return unique
        
def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)