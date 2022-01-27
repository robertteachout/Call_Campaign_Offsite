import numpy as np
import pandas as pd
from datetime import date
from pipeline.etc import last_business_day
from server.query import query
import server.queries.call_conversion

import server.secret
servername  = server.secret.servername
database    = server.secret.database

today = date.today()
today_str = today.strftime('%Y-%m-%d')
yesterday = last_business_day(today)
yesterday_str = yesterday.strftime("%Y-%m-%d")

def main(day, projects=list()):
    # sql extract
    def data(lbd):
        sql = server.queries.call_conversion.sql(lbd)
        df = query(servername, database, sql, '')
        return df
    df = data(day)
    # filter for projects
    df = df[df.ProjectType.isin(projects)].copy()
    # map calculation columns
    f1 = df.cf_last_call.isna()
    df['cf_no_call'] = np.where(f1, 1, 0)
    f1 = df.cf_last_call == day
    df['cf_call'] = np.where(f1, 1, 0)
    f1 = df.Disp_Name.notna()
    df['disp_count'] = np.where(f1, 1,0)
    # aggregate
    totals = df[['OutreachID','disp_count','cf_call','cf_no_call']].agg(
        {'OutreachID':'count','disp_count':'sum','cf_call':'sum','cf_no_call':'sum'}
        ).to_frame().rename(columns={0:day})

    project = df.groupby('ProjectType')['OutreachID'].count().to_frame(
        ).sort_values(by="OutreachID",ascending=False
        ).rename(columns={"OutreachID":f"{day}"})

    disp = df.groupby('Disp_Name')['OutreachID'].count().to_frame(
        ).sort_values(by="OutreachID",ascending=False
        ).rename(columns={"OutreachID":f"{day}"})
    # consolidate top dispositions
    top = 10
    disp_clean = disp[:top].copy()
    disp_clean.loc[f'Not Top {top}'] = disp[top:].sum()
    # add breaks for appending all three df's
    project_break = pd.DataFrame(disp.columns,columns=disp.columns, index=['Projects'])
    disp_break = pd.DataFrame(disp.columns,columns=disp.columns, index=['Disposition'])

    return (totals.append(project_break).append(project).append(disp_break).append(disp_clean))
if __name__ == '__main__':
    print('test')
    # projects = ['ACA-PhysicianCR','ACA-HospitalCR']
    # main(projects)