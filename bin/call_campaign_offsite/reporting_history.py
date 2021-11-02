from pathlib import Path 
import pandas as pd
import numpy as np
import os
from glob import glob
from data_config import tables
from dbo_query import Query
from datetime import date, timedelta, datetime
import sys

def nic(start, end):
    sql = """
        SELECT 
        Contact_Name
        FROM [DWWorking].[Prod].[nicagentdata]
        WHERE [Start_Date] >= '{}'
        AND [Start_Date] <= '{}' """.format(start, end)
    df = Query('DWWorking', sql, 'Last Call Check')
    return df

target = pd.DataFrame()

def new_func(batch):
    z = Path(f'data/batch/batch_{batch}')
    b = list(z.glob('*.txt'))
    dub = pd.DataFrame()
    dt = list()
    for i in b:
        df = pd.read_csv(i, sep='|', on_bad_lines='skip', engine="python")
        col = df[['PhoneNumber']].astype(str)
        # col['PhoneNumber'] = col['PhoneNumber'].str[:-2]
        try:
            dub = dub.append(col)
        except:
            dub = col
        st = (str(i).split('_')[-1][:4])
        print(st)
        dt.append(f'{st[:2]}-{st[2:]}')
    return z,dub,dt

for batch in range(1, 13):
    z, dub, dt = new_func(batch)

    s_dt = f'2021-{dt[0]}'
    e_dt = f'2021-{dt[-1]}'
    print(s_dt)
    print(e_dt)

    clean_dub = dub.drop_duplicates(
        subset= ['PhoneNumber'],
        keep='last').reset_index(drop=True)
    
    print(f'len of numbers: {len(clean_dub)}')

    n = nic(s_dt, e_dt)
    n = n.rename(columns={'Contact_Name':'PhoneNumber'}).astype(str)
    n['count'] = 1

    join = pd.merge(clean_dub, n, how='left', on=['PhoneNumber']).fillna(0).rename(columns={'PhoneNumber':s_dt})

    Unique_Delivery = join[join['count'] == 1]

    clean_uni = Unique_Delivery.drop_duplicates(
        subset= [f'{s_dt}'],
        keep='last').reset_index(drop=True)

    print(f'len Unique_Delivery: {len(clean_uni)}')

    piv = join.pivot_table(index=['count'], values =[f'{s_dt}'], aggfunc = ['count'], margins=True)#.reset_index()
    z = Path(f'data/batch')
    
    # piv.to_csv(z /f'batch_{batch}.csv')
    print('- - - - - - - - - - - - - - - - - ')
    print(f'Finished Batch: {batch}')
    print('- - - - - - - - - - - - - - - - - ')
