import pandas as pd
import numpy as np
from .business_prioirty import ciox_busines_lines

def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def project_rank(df, buz_line, rank, temp):
    f1 = df.Project_Type.isin(buz_line.projects)
    df[f'temp_rank{rank}'] = np.where(f1, 1, 0)
    # create dict for scoring
    temp[f'temp_rank{rank}'] = False
    return df, temp

def campaign_rank_cycle(temp):
    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False,'age':False, 'togo_bin':False} 
    rank_final = {'meet_target_sla':True, **temp,  'no_call':False, 'age_sort':False, 'togo_bin':False} 

def stack_inventory(df, grouping):
    business = ciox_busines_lines()
    temp = {}
    for index, buz_line in enumerate(business):
        # create column on dataframe
        df, temp = project_rank(df, buz_line, index, temp) 

    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False,'age_sort':False, 'ToGoCharts':False} 
    # group by phone number or msid & rank highest value org
    find_parent = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = find_parent.overall_rank == 1
    find_parent['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    rank_parent = rank(find_parent,'Score', ['Skill','parent'], rank_cols)
    
    if grouping == 'PhoneNumber':
        for buz_line in business:
            if buz_line.system == 'CC_ChartFinder':
                f0 = rank_parent.Skill == buz_line.system
                f1 = rank_parent.Project_Type.isin(buz_line.projects)
                rank_parent.Skill = np.where(f0 & f1, buz_line.skill, rank_parent.Skill)


    full_rank = rank(rank_parent,'Score', ['Skill','parent'], rank_cols)
    full_rank.OutreachID = full_rank.OutreachID.apply(lambda x: str(x))
    full_rank['Matchees'] = full_rank.groupby(['Skill', grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return full_rank

def skill_score(df, skill, skill_rank):
    skill = df[df.Skill == skill].copy()
    dump = rank(skill,'Score', ['Skill','parent'], skill_rank)
    return pd.concat([dump, df]).drop_duplicates(['OutreachID']).reset_index(drop= True)

def custom_skills(table):
    skills = [
        { "CC_Adhoc7": {'meet_target_sla':True, 'no_call':False, 'age':False} },
        { "CC_Adhoc8": {'meet_target_sla':True, 'Centene_HEDIS':True, 'no_call':False, 'ToGoCharts':False} },
        { "CC_Adhoc3": {'meet_target_sla':True, 'no_call':False, 'age':False} },
        { "CC_Adhoc3": {'meet_target_sla':True, 'no_call':False, 'age':False} },
        { "CC_Genpact_Scheduling": {'meet_target_sla':True, 'aetna_comm':False,'no_call':False, 'age':False} },
        { "CC_ChartFinder": {'meet_target_sla':True, 'wellmed':False,'no_call':False, 'age':False} },
    ]
    for skill in skills:    
        for skill_name , rank_order in skill.items():
            table = skill_score(table, skill_name, rank_order)
    return table

def split(df):
    df['Outreach ID'] = df['OutreachID'].astype(str)
   
    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid, 'PhoneNumber')
    msid_scored = stack_inventory(msid, 'MasterSiteId')
    unique =  pd.concat([scored, msid_scored]).drop_duplicates(['OutreachID']).reset_index(drop= True)

    ### skille that need special treatment
    unique = custom_skills(unique)

    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    unique['Score'] = np.where(f0, 1000000, unique.Score)
    return unique

def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)