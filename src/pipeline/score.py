import json
from json.decoder import JSONDecodeError
import pandas as pd
import numpy as np
from .business_prioirty import ciox_busines_lines
from .tables import CONFIG_PATH

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

def stack_inventory(df, grouping):
    business = ciox_busines_lines()
    temp = {}
    for index, buz_line in enumerate(business):
        # create column on dataframe
        df, temp = project_rank(df, buz_line, index, temp) 

    # for chartfinder inventory search projects and move to adhoc skills
    if grouping == 'PhoneNumber':
        for buz_line in business:
            if buz_line.system == 'CC_ChartFinder':
                f0 = df.Skill == buz_line.system
                f1 = df.Project_Type.isin(buz_line.projects)
                df.Skill = np.where(f0 & f1, buz_line.skill, df.Skill)

    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False,'age_sort':False, 'ToGoCharts':False} 
    # group by phone number or msid & rank highest value org
    find_parent = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = find_parent.overall_rank == 1
    find_parent['parent'] = np.where(f1, 1, 0)

    # rank parent orgs
    full_rank = rank(find_parent,'Score', ['Skill','parent'], rank_cols)
    full_rank.OutreachID = full_rank.OutreachID.apply(lambda x: str(x))
    full_rank['Matchees'] = full_rank.groupby(['Skill', grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return full_rank

def skill_score(df, skill, skill_rank):
    # check if skill is active & ranking columns exists
    if df.Skill.isin([skill]).any() and set(skill_rank.keys()).issubset(df.columns):
        skill = df[df.Skill == skill].copy()
        dump = rank(skill,'Score', ['Skill','parent'], skill_rank)
        return pd.concat([dump, df]).drop_duplicates(['OutreachID']).reset_index(drop= True)
    else:
        print(f'Check Skill: {skill} | validate columns: {set(skill_rank.keys())}')
        raise SystemExit

def custom_skills(table, skills):
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

    ### skills that need special treatment
    try:
        with open(CONFIG_PATH / 'custom_skill_rank.json') as json_file:
            skills = json.load(json_file)
    except JSONDecodeError:  # includes simplejson.decoder.JSONDecodeError
        skills = None
    
    if skills != None:
        unique = custom_skills(unique, skills)

    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    unique['Score'] = np.where(f0, 1000000, unique.Score)
    return unique

def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)