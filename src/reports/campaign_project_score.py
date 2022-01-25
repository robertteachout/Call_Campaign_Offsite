from audioop import tomono
import imp
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date

from pipeline.tables import tables, append_column
from pipeline.etc import last_business_day

today = date.today()
today_str = today.strftime("%Y-%m-%d") 

tomorrow = last_business_day(today)
tomorrow_str = tomorrow.strftime("%Y-%m-%d") 

def main():
    df = tables('pull', 'na', f'{today}.zip','data/load/')
    df.sort_values('OutreachID')
    f1 = (df.Skill == 'CC_Tier1') & (df.Score < 1500)
    f2 = (df.Skill == 'CC_Tier2') & (df.Score < 5000)
    df['Called'] = np.where(f1 | f2, 1, 0)

    called = df[df.Called == 1].pivot_table(
                                        index=['Skill','Project_Type'], 
                                        values ='OutreachID', 
                                        aggfunc = ['count'])

    called.columns = called.columns.droplevel()
    name = tomorrow_str
    called.columns = [name]
    called = called.sort_values(name,ascending=False)
    append_column(called, 'docs/campaign_project_score.csv', ['Skill', 'Project_Type'])

