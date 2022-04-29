import functools

import numpy as np
import pandas as pd


def combine_filters(operator: str, filters: pd.Series) -> pd.Series:
    def filter_reducer(filter_one: pd.Series, filter_two: pd.Series) -> pd.Series:
        # Helper for combining filters based on the operations
        return f"({filter_one}) {operator} ({filter_two})"

    # Combine all the filters into a single filter
    return functools.reduce(filter_reducer, filters)
    
OPERATOR_SIGNS = {"Or": "|", "And": "&"}

def one_filter(df, new_skill, **kwargs):
    filters = []
    for k, v in kwargs.items():
        condition, value = v[0], v[1]
        operator = None
        if k == "operator":
            operator = v
        else:
            if isinstance(value, list):
                filters.append(f"df.{k}{condition} ({value})")
            elif isinstance(value, int):
                filters.append(f"df.{k}{condition} ({value})")
            else:
                filters.append(f"df.{k}{condition} ('{value}')")

    if operator:
        f = pd.eval(combine_filters(operator, filters))
        df.Skill = np.where(f, new_skill, df.Skill)
    else:
        f = pd.eval(filters[0])
        df.Skill = np.where(f, new_skill, df.Skill)

    return df

    
def MasterSiteId(df):
    f1 = df["Outreach_Status"] != "Scheduled"
    f2 = df.age > 10
    msid = df[f1 | f2].sort_values("age", ascending=False).reset_index().MasterSiteId.unique()[:6000]
    msid = [int(i) for i in list(msid)]
    msid.remove(1000838)

    df = one_filter(df, "CC_Cross_Reference", MasterSiteId=[".isin",msid])
    return df

def Cross_Reference_SPI(df):
    df = one_filter(df, "CC_Cross_Reference_SPI", SPI=[" == ",True])
    return df

def complex_skills(df):
    df.Skill = "CC_ChartFinder"
    df = MasterSiteId(df)

    df = one_filter(df, 
                    "Remove_EMR_Remote", 
                    Project_Type=[".isin",["Oscar","Aetna Medicare"]], 
                    operator="And", 
                    Retrieval_Team=[" == ","Genpact Offshore"])

    df = one_filter(df, "Remove_Schedule", Outreach_Status=[" == ",'Scheduled'],  operator="And" ,age=[">", 10] )
    df = one_filter(df, "Remove_Research", PhoneNumber=[" == ","9999999999"])

    f1 = [" == ","Osprey"]
    df = one_filter(df, "CC_Osprey_Outbound", Project_Type=f1)
    df = one_filter(df, "Remove_Osprey_research", Project_Type=f1, operator = "And" , PhoneNumber=[" == ","9999999999"])
    df = one_filter(df, "Remove_Osprey_Escalation", Project_Type=f1,  operator ="And" ,Outreach_Status=[".isin", ["Escalated","PNP Released"]] )
    
    df = one_filter(df, "Remove_EMR_Remote", Retrieval_Group=[" == ","EMR Remote"])
    df = one_filter(df, "Remove_HIH", Retrieval_Group=[" == ","HIH - Other"])
    df = one_filter(df, "Remove_Onsite", Retrieval_Group=[" == ","Onsite"])
    return df
