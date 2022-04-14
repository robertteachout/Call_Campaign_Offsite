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
    new_skill_name = custom_skill.Skill

    def check_filter(key, value):
        if key != "general":
            return f"{key} in {value}" 
        else:
            return ' & '.join([f"{item} == 1" for item in value])
            

    for custom_skill in reversed(business):
        check = " & ".join(
            [check_filter(k,v) 
                for k,v in custom_skill.filters.items() 
                if v != ["default"]])

        get_index = df.query(check).index
        f = df.index.isin(get_index)
        df.Skill = np.where(f, new_skill_name, df.Skill)
    

def stack_inventory(df, grouping):
    # init temp ranks
    temp = {}
    # if data available in business_lines.json load into scoring logic
    if grouping == "PhoneNumber":
        if isinstance(business, list):
            df = create_custom_skill(df, business)   

    rank_cols = {
        "meet_target_sla": True,
        **temp,
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

    ### skills that need special treatment
    if business != None:
        score_business = [line for line in business 
                            if list(line.scoring.keys()) != ["default"]]
        unique = custom_skills(unique, score_business)

    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(["Chart Sync"])  # 'ACA-PhysicianCR'
    unique["Score"] = np.where(f0, 1000000, unique.Score)
    return unique
