import pandas as pd
import numpy as np
from datetime import datetime, date

from pipeline.tables import tables, append_column
from pipeline.etc import last_business_day, next_business_day

today = date.today()
today_str = today.strftime("%Y-%m-%d") 

yesterday = last_business_day(today)
yesterday_str = yesterday.strftime("%Y-%m-%d") 

tomorrow = next_business_day(today)
tomorrow_str = tomorrow.strftime("%Y-%m-%d") 

def main(today_str):
    df = tables('pull', 'na', f'{today_str}.zip','data/load/')
    work = {
        'CC_Tier1':1500,
        'CC_Tier2':5000,
        'CC_Adhoc1':1800,
        'CC_Adhoc2':1000
    }
    filters = '|'.join([f"(Skill == '{i}' & Score < {j})" for i, j in work.items()])
    skills = '|'.join([f"(Skill == '{i}')" for i in work.keys()])
    top_num = df.query(filters).PhoneNumber.tolist()
    
    table = df[df.PhoneNumber.isin(top_num)]
    tb = table.query(skills).copy()
    called = tb.pivot_table(
                            index=['Skill','Project_Type'], 
                            values ='OutreachID', 
                            aggfunc = ['count'])

    called.columns = called.columns.droplevel()
    name = today_str
    called.columns = [name]
    print(called)
    append_column(called, 'data/daily_priority/campaign_project_score.csv', ['Skill', 'Project_Type'])
    tb[['Skill','OutreachID', 'PhoneNumber', 'Score','Last_Call']].to_csv(f'data/daily_priority/{today_str}.csv', index=False)
