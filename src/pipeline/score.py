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


def project_rank(df, projects, rank, temp):
    f1 = df.Project_Type.isin(projects)
    df[f"temp_rank{rank}"] = np.where(f1, 1, 0)
    # create dict for scoring
    temp[f"temp_rank{rank}"] = False
    return df, temp


def stack_inventory(df, grouping):
    # init temp ranks
    temp = {}
    # if data available in business_lines.json load into scoring logic
    if isinstance(business, list):
    # remove default skills that only need re-scoring
        # re_skill_business = [line for line in business
        #                         if line.system != "default"]
        # for index, buz_line in enumerate(re_skill_business):
        #     # create column on dataframe
        #     df, temp = project_rank(df, buz_line.projects, index, temp)

        # for chartfinder inventory search projects and move to adhoc skills
        if grouping == "PhoneNumber":
            for buz_line in business:
                if buz_line.system == "CC_ChartFinder":
                    f0 = df.Skill == buz_line.system
                    f1 = df.Project_Type.isin(buz_line.projects)
                    df.Skill = np.where(f0 & f1, buz_line.skill, df.Skill)

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

    split = "CC_Cross_Reference"
    notmsid = df[df.Skill != split].copy()
    msid = df[df.Skill == split].copy()

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
