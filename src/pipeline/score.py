import numpy as np
import pandas as pd
from .business_prioirty import ciox_busines_lines
from .user_input import load_custom_skills  
from .utils import join_tables

business = ciox_busines_lines() 

def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending = [True] * len(groups) + [*rank_cols.values()]

    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def parent_child_link(df, parent:str, child:str="OutreachID"):
    df[child] = df[child].apply(lambda x: str(x))
    df["Matches"] = df.groupby(["Skill", parent])[child]\
                        .transform(lambda x: "|".join(x))\
                        .apply(lambda x: x[:3000])
    return df

def stack_inventory(df, grouping):
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

    linked = parent_child_link(full_rank, grouping)
    return linked

def split(df):
    split = "CC_ChartFinder"
    notmsid = df[df.Skill == split].copy()
    msid = df[df.Skill != split].copy()

    scored = stack_inventory(notmsid, "PhoneNumber")
    msid_scored = stack_inventory(msid, "MasterSiteId")
    return join_tables(scored, msid_scored)

def scored_inventory(df):
    # split regular inventory
    inventory = split(df)
    # if data available in business_lines.json load into scoring logic
    if isinstance(business, list):
        inventory = load_custom_skills(inventory, business)
    return inventory
