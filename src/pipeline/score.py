import numpy as np
import pandas as pd

from .business_prioirty import ciox_busines_lines, Business_Line

business = ciox_busines_lines() 

def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending = [True] * len(groups) + [*rank_cols.values()]

    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def create_custom_skill(df, custom_skill:Business_Line):

    def check_filter(key, value):
        return ' & '.join([f"{item} == 1" for item in value]) \
        if key == "equal_1" \
        else f"{key} in {value}" 
            
    for custom_skill in business:
        filters = [check_filter(k,v) 
                    for k,v in custom_skill.filters.items() 
                    if v != ["default"]]

        if len(filters) == 0:
            pass
        else:
            filters = " & ".join(filters)
        
            get_index = df.query(filters).index
            f = df.index.isin(get_index)
            df.Skill = np.where(f, custom_skill.skill, df.Skill)
    return df

def stack_inventory(df, grouping):
    # if data available in business_lines.json load into scoring logic
    if grouping == "PhoneNumber":
        if isinstance(business, list):
            df = create_custom_skill(df, business)   

    rank_cols = {
        "meet_target_sla": True,
        "no_call": False,
        "age_sort": False,
        "ToGoCharts": False,
    }
    # group by phone number or msid & rank highest value org
    find_parent = rank(df, "overall_rank", ["Skill", grouping], rank_cols)

    # top overall per group = parent
    f1 = find_parent.overall_rank == 1
    find_parent["parent"] = np.where(f1, 1, 0)

    # rank parent orgs
    full_rank = rank(find_parent, "Score", ["Skill", "parent"], rank_cols)
    full_rank.OutreachID = full_rank.OutreachID.apply(lambda x: str(x))
    full_rank["Matchees"] = (
        full_rank.groupby(["Skill", grouping])["OutreachID"]
        .transform(lambda x: "|".join(x))
        .apply(lambda x: x[:3000])
    )
    return full_rank


def skill_score(df, skill, skill_rank:Business_Line):
    # check if skill is active & ranking columns exists
    if df.Skill.isin([skill]).any() and set(skill_rank.keys()).issubset(df.columns):
        skill = df[df.Skill == skill].copy()
        dump = rank(skill, "Score", ["Skill", "parent"], skill_rank)
        return pd.concat([dump, df])\
                    .drop_duplicates(["OutreachID"])\
                    .reset_index(drop=True)
        
    else:
        print(f"Check Skill: {skill} | validate columns: {set(skill_rank.keys())}")
        raise SystemExit


def custom_skills(table, business:list[Business_Line]):
    for line in business:
        table = skill_score(table, line.skill, line.scoring)
    return table

def create_score_column(df, name):
    f1 = df.Project_Type == name
    df[name] = np.where(f1, 1, 0)
    return df

def split(df):
    df["Outreach ID"] = df["OutreachID"].astype(str)

    split = "CC_ChartFinder"
    notmsid = df[df.Skill == split].copy()
    msid = df[df.Skill != split].copy()

    scored = stack_inventory(notmsid, "PhoneNumber")
    msid_scored = stack_inventory(msid, "MasterSiteId")
    unique = (
        pd.concat([scored, msid_scored])
        .drop_duplicates(["OutreachID"])
        .reset_index(drop=True)
    )

    for skill in business:
        if skill.new_columns != ["default"]:
            for name in skill.new_columns:
                unique = create_score_column(unique, name)

    ### skills that need special treatment
    if business != None:
        score_business = [line for line in business 
                            if list(line.scoring.keys()) != ["default"]]
        unique = custom_skills(unique, score_business)

    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(["Chart Sync"])  # 'ACA-PhysicianCR'
    unique["Score"] = np.where(f0, 1000000, unique.Score)
    return unique
