import sys, os
import pandas as pd
from datetime import date

import server.secret
import server.query
import server.queries.project_breakdown
from pipeline.etc import daily_piv, time_check, next_business_day, x_Bus_Day_ago, Next_N_BD, date_list_split, last_business_day
servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
today = date.today()
tomorrow = next_business_day(today)
yesterday = last_business_day(today)
yesterday_str = yesterday.strftime(date_format)

def main():
    ### Add to query
    project_breakdown_sql = server.queries.project_breakdown.sql(yesterday_str)
    project_breakdown = server.query.query(servername, database,  project_breakdown_sql, 'project_breakdown')

    projects =  ['AHN','CDQI HCR', 'NAMMCA', 'OC-AZ', 'OC-NV', 'OCN-WA', 'OC-UT', 'Reliant', 'Riverside', 'WellMed']
    project_breakdown = project_breakdown[project_breakdown.ProjectType.isin(projects)]

    def group(df, target_col, agg_method):
        clean = df[['OutreachID','ProjectType',target_col]]
        clean = clean.sort_values(
                                by=['OutreachID',target_col]
                                ).drop_duplicates(
                                subset='OutreachID',
                                keep='last')
        return clean, clean.groupby('ProjectType').agg({target_col:agg_method})

    cc = project_breakdown[['OutreachID','ProjectType']].drop_duplicates(subset='OutreachID')
    cc_agg = cc.groupby('ProjectType')['OutreachID'].count()

    def full(df):
        c, c_agg        = group(df, 'daily_target_flag','sum')
        nic, nic_agg    = group(df, 'disp_count','count')
        cf, cf_agg      = group(df, 'CallNoteFlag','sum')
        return (pd.concat([cc_agg, c_agg, nic_agg, cf_agg], axis=1))

    # daily_inventory = project_breakdown[project_breakdown.daily_target_flag == 1]
    # daily_target_flag = full(daily_inventory)
    final = full(project_breakdown)

    df = pd.DataFrame()
    df['daily_group_conversion'] = final.daily_target_flag / final.OutreachID 
    df['nic_conversion'] = final.disp_count / final.OutreachID 
    df['cf_conversion'] = final.CallNoteFlag / final.OutreachID

    df = df.apply(lambda series: series.apply('{:.2%}'.format))
    df_full = pd.concat([final,df],axis=1).sort_values('OutreachID',ascending=False)
    print(df_full)

