import numpy as np
import pandas as pd
from datetime import date
from pipeline.etc import last_business_day
from server.query import query
from pipeline.tables import append_column

import server.queries.call_conversion_site

import server.secret
servername  = server.secret.servername
database    = server.secret.database

today = date.today()
today_str = today.strftime('%Y-%m-%d')
yesterday = last_business_day(today)
yesterday_str = yesterday.strftime("%Y-%m-%d")

def main(projects=list(),day=yesterday_str):
    # sql extract
    def data(lbd):
        sql = server.queries.call_conversion_site.sql(lbd)
        df = query(servername, database, sql, '')
        return df

    df = data(day)
    # filter for projects
    f1 = df.ProjectType.isin(projects)
    f2 = df.DaysSinceCreation > 10
    df = df[f1 & f2].copy()
    # map calculation columns
    f1 = df.cf_last_call.isna()
    df['cf_no_call'] = np.where(f1, 1, 0)
    f1 = df.cf_last_call == day
    df['cf_call'] = np.where(f1, 1, 0)
    f1 = df.Disp_Name.notna()
    df['disp_count'] = np.where(f1, 1,0)
    # aggregate
    totals = df.agg(
        {'OutreachID':'count','disp_count':'sum','cf_call':'sum','cf_no_call':'sum'}
        ).to_frame().rename(columns={0:day})
    # create pivots
    def grouper(group):
        return df.groupby(group)['OutreachID'].count().to_frame(
        ).sort_values(by="OutreachID",ascending=False
        ).rename(columns={"OutreachID":f"{day}"})

    project = grouper('ProjectType')
    disp = grouper('Disp_Name')

    # consolidate top dispositions
    top = 10
    disp_clean = disp[:top].copy()
    disp_clean.loc[f'Not Top {top}'] = disp[top:].sum()
    # add breaks for appending all three df's
    project_break = pd.DataFrame(disp.columns,columns=disp.columns, index=['Projects'])
    disp_break = pd.DataFrame(disp.columns,columns=disp.columns, index=['Disposition'])
    # append to one df
    final = (totals.append(project_break).append(project).append(disp_break).append(disp_clean))
    final.index.rename('Totals', inplace=True)
    # create and store new data to this location
    append_column(final, 'docs/aca_historical_reporting.csv',['Totals'])