import pandas as pd
import numpy as np
from dataclasses import dataclass


def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def project_rank(df, buz_line, rank):
    f1 = df.Project_Type.isin(buz_line.projects) # 'ACA-PhysicianCR'
    df[f'temp_rank{rank}'] = np.where(f1, 1, 0)
    return df

@dataclass
class business_lines:
    projects: list[str]
    capacity: int
    system:str

def stack_inventory(df, grouping):
    uhc     = business_lines(['UHC HEDIS'], 3650,'CC_ChartFinder')
    hedis   = business_lines(['HEDIS'], 2450,'CC_ChartFinder')
    aca     = business_lines(['ACA-PhysicianCR'], 3550,'CC_ChartFinder')
    medicaid= business_lines(['Chart Review','Clinical Review MCaid PhyCR'], 2500,'CC_Cross_Reference')

    business = [uhc, hedis, aca, medicaid]

    temp = {}
    for index, buz_line in enumerate(business):
        # create column on dataframe
        df = project_rank(df, buz_line, index)
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
    
    if grouping == 'PhoneNumber':  
        for buz_line in business:
            if buz_line.system == 'CC_Cross_Reference':
                pass
            else:
                f0 = df.Skill == buz_line.system
                f1 = df.Project_Type.isin(buz_line.projects)
                f2 = df.parent == 1
                f3 = df.meet_target_sla == 0

                orgs = df[f0 & f1 & f2 & f3].sort_values('Score')['OutreachID'].head(buz_line.capacity).to_list()
                print(f'"{buz_line.projects}"\t target: {buz_line.capacity} == {len(orgs)}')
                try:
                    assert len(orgs) == buz_line.capacity
                except:
                    exit()

                f4 = df.OutreachID.isin(orgs)
                df.master = np.where(f4, 1, df.master)

        new_rank_cols = {'meet_target_sla':True, 'master':False, **temp, 'no_call':False, 'togo_bin':False, 'age':False} 
        df = rank(df,'Score', ['Skill'], new_rank_cols)
    
    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    df['OutreachID'] = df['OutreachID'].astype(str)
    df['master'] = 0
    f1 = df.Project_Type == 'UHC HEDIS'
    piv = df[f1]
    piv2 = piv.pivot_table(index=['Skill','meet_target_sla','no_call'], values=['OutreachID'], aggfunc='count')
    print(piv2)
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