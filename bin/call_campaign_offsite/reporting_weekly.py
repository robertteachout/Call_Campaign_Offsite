from pathlib import Path 
import pandas as pd
import numpy as np
import os
from glob import glob
from data_config import tables
from dbo_query import Query
from datetime import date, timedelta, datetime

def nic(start, end):
    sql = """
        SELECT 
        [Skill_Name],
        Contact_Name,
        [Start_Date]
        FROM [DWWorking].[Prod].[nicagentdata]
        WHERE [Start_Date] >= {}
        AND [Start_Date] <= {}""".format(start, end)
    df = Query('DWWorking', sql, 'Last Call Check')
    return df

batch = 4
s_date = f'\'2021-10-11\''
e_date = f'\'2021-10-22\''

z = Path(f'data/load/check{batch}')
b = list(z.glob('*.zip'))

dub = pd.DataFrame()

for i in b:
    df = pd.read_csv(i, sep=',',low_memory=False)
    filter1 = df['Skill'] != 'Child_ORG'
    filter2 = df['Unique_Phone'] == 1

    df = df[filter1 & filter2]
    df['Daily_Groups'] = pd.to_datetime(df['Daily_Groups']).dt.date
    col = df[['OutreachID','PhoneNumber','Load_Date','Daily_Groups']].astype(str)
    col['PhoneNumber'] = col['PhoneNumber'].str[:-2]
    try:
        dub = dub.append(col)
    except:
        dub = col

clean_dub = dub.drop_duplicates(
    subset= ['PhoneNumber'],
    keep='last').reset_index(drop=True)


n = nic(s_date, e_date)
n = n.rename(columns={'Contact_Name':'PhoneNumber',"Start_Date":"Load_Date"}).astype(str)
n['count'] = 1

clean_nic = n.drop_duplicates(
    subset= ['PhoneNumber'],
    keep='last').reset_index(drop=True)

join = pd.merge(clean_dub, clean_nic, how='left', on=['PhoneNumber'])

call_piv = join.pivot_table(columns= ['Daily_Groups'], values =['count'], aggfunc = ['count'], margins=True,margins_name= 'TOTAL')
call_piv.columns = call_piv.columns.droplevel().droplevel()

fullpiv = clean_dub.pivot_table(columns= ['Daily_Groups'], values =['PhoneNumber'], aggfunc = ['count'], margins=True,margins_name= 'TOTAL')
fullpiv.columns = fullpiv.columns.droplevel().droplevel()

t = pd.DataFrame(columns= fullpiv.columns)
t.loc['     '] = '- - - - -'

report = fullpiv.append(t).append(call_piv).append(t).append(round(call_piv/fullpiv, 2)).append(t)

z = Path('data')

report.to_csv(z / f'batch_unique_break_{batch}.csv')