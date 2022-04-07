from datetime import date, datetime

import numpy as np
import pandas as pd

from pipeline.etc import last_business_day, next_business_day
from pipeline.tables import append_column, tables

today = date.today()
today_str = today.strftime("%Y-%m-%d")

yesterday = last_business_day(today)
yesterday_str = yesterday.strftime("%Y-%m-%d")

tomorrow = next_business_day(today)
tomorrow_str = tomorrow.strftime("%Y-%m-%d")


def main(today_str=today_str):
    df = tables("pull", "na", f"{today_str}.zip", "data/load/")
    work = {
        "CC_ChartFinder": 8000,
        "CC_Cross_Reference": 2000,
        # 'CC_Adhoc2':1000
    }
    # get top x in each skill
    filters = "|".join(
        [f"(Skill == '{i}' & Score < {j} & parent == 1)" for i, j in work.items()]
    )
    table = df.query(filters)  # .PhoneNumber.tolist()
    # filter skills
    # skills = '|'.join([f"(Skill == '{i}')" for i in work.keys()])
    # full =  df.query(skills)
    # filter numbers
    # table = full[full.PhoneNumber.isin(top_num)]
    parent = df.query(filters)
    # pivot
    called = table.pivot_table(
        index=["Skill", "Project_Type"], values="OutreachID", aggfunc=["count"]
    )
    # clean pivot
    called.columns = called.columns.droplevel()
    name = today_str
    called.columns = [name]
    print(called)
    # append_column(called, 'data/daily_priority/campaign_project_score.csv', ['Skill', 'Project_Type'],'outer')
    # table[['Project_Type','Skill','OutreachID', 'PhoneNumber', 'Score','Last_Call','parent']].to_csv(f'data/daily_priority/{today_str}.csv', index=False)
