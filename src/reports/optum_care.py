import os

from server.query import query
import server.secret
servername  = server.secret.servername
database    = server.secret.database

def nic(start):
    sql = f"""
        SELECT 
        Contact_Name
        ,[Start_Date]
        ,Skill_Name
        ,Disp_Name
        ,Disp_Comments
        
        FROM [DWWorking].[Prod].[nicagentdata]
        WHERE [Start_Date] = '{start}'
        """
    df = query(servername, database, sql, '')
    return df

import pyarrow as pa
import pyarrow.csv as csv

import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path

from pipeline.etc import last_business_day

def main(batch):
    z = Path(f'data\\batch\\batch_{batch}')
    b = list(z.glob('*.csv'))
    projects = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']

    group = pd.DataFrame()
    total = pd.DataFrame({'total':['total_inventory', 'total_calls', 'total_no_calls']}).set_index('total')
    nic_disp = pd.DataFrame()

    for i in b:
        start = csv.read_csv(i)
        start = start.to_pandas()

        st = (str(i).split('\\')[-1][:-4])
        today = datetime.strptime(st, "%Y-%m-%d")
        yesterday = last_business_day(today).strftime("%Y-%m-%d")
        print(yesterday)

        oc = start[start.Project_Type.isin(projects)][['Project_Type', 'PhoneNumber', 'OutreachID', 'Last_Call', 'Skill']]
        oc.Last_Call = pd.to_datetime(oc.Last_Call, errors='ignore')
        oc.PhoneNumber = oc.PhoneNumber.astype(np.int64).astype(str)

        l = oc[oc['Last_Call'] == '0'].drop_duplicates()

        f1 = oc.Last_Call == yesterday
        oc['lc_flag'] = np.where(f1, 1, 0)

        nic_data = nic(yesterday)
        nic_data = nic_data.rename(columns={"Skill_Name":"Skill",'Contact_Name':'PhoneNumber'})
        
        df = pd.merge(oc, nic_data, how='left', on=['PhoneNumber'])
        df_drop = df.drop_duplicates(subset=['PhoneNumber'])

        total_inventory = (len(oc))
        total_calls = (len(oc[oc['lc_flag'] == 1]))
        groups = oc.groupby('Project_Type')['lc_flag'].sum()
        total_no_calls = (len(l))

        total[f"{yesterday}"] = [total_inventory, total_calls, total_no_calls]
        group[f"{yesterday}"] = groups
        
        disp = df_drop.groupby('Disp_Name')['OutreachID'].count().to_frame().rename(columns={"OutreachID":f"{yesterday}"}).sort_values(by=f"{yesterday}",ascending=False)
        top = 10
        disp_clean = disp[:top].copy()
        disp_clean.loc[f'Not Top {top}'] = disp[top:].sum()

        try:
            nic_disp = pd.concat([nic_disp, disp_clean], axis=1)

        except:
            nic_disp = disp_clean
        
    Projects = pd.DataFrame(columns=total.columns)
    Projects.loc['Projects'] = total.columns

    Disposition = pd.DataFrame(columns=total.columns)
    Disposition.loc['Disposition'] = total.columns

    nic_disp = nic_disp.sort_values(by=f'{yesterday}',ascending=False)

    report = total.append(Projects).append(group).append(Disposition).append(nic_disp).fillna('')

    report.to_csv(f'docs/report_oc_{batch}.csv')