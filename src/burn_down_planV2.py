import numpy as np
import pandas as pd
import re

def match(df, match):
    # full df, group by location & project, get smallest value for the match in each projects
    t2 = df.groupby([match, 'Project_Type']).agg({'sla':'min','days_before_out_sla':'min'}).reset_index()
    # group projects by location | match
    t2['groups'] = t2.groupby(match)['Project_Type'].transform(lambda x: ' | '.join(x))
    # group by group of projects, find smallest value for each location
    t3 = t2.groupby(['groups',match]).agg({'sla':'min','days_before_out_sla':'min'}).reset_index()
    # pivot groups by smallest value, counting sites
    return t3.pivot_table(index=['groups','sla'],columns='days_before_out_sla',values=match,aggfunc='count')

def buckets(df):
    split = {
        'Medicare': '(Chart Sync|HCR)',
        'Medicaid':'(Chart Review|Medicaid- HospCR)',
        'ACA':'(ACA-PhysicianCR|ACA-HospitalCR)',
        'UHC HEDIS':'UHC HEDIS',
        'HEDIS':'HEDIS_c'
        }
    df['Bucket'] = 'none'
    for bucket, search in split.items():
        f1 = df.groups.apply(lambda x: bool(re.search(search, x)))
        df.Bucket = np.where(f1, bucket, df.Bucket)
    return df

def sites(df0, projects):
    # create column
    df0['days_before_out_sla'] = df0.sla - df0.age
    df0['days_before_out_sla'] = np.where(df0.days_before_out_sla < 0, 0, df0.days_before_out_sla)
    # change HEDIS Commercail
    f1 = df0.Project_Type == 'HEDIS'
    df0.Project_Type = np.where(f1, 'HEDIS_c', df0.Project_Type)

    df = df0[df0.Project_Type.isin(projects)]

    cf = df[(df.MasterSiteId == 1000838) | (df.MasterSiteId.isna())]
    msid = df[(df.MasterSiteId != 1000838) & (df.MasterSiteId.notna())]

    t  = match(cf,'PhoneNumber')
    t2 = match(msid,'MasterSiteId')
    t3 = (t2 + t)
    t3.reset_index(inplace=True)
    return buckets(t3)


if __name__ == "__main__":
    from datetime import date
    from pipeline.tables import tables
    today = date.today()
    today_str = today.strftime('%Y-%m-%d')
    df0 = tables('pull','na',f"{today_str}.zip", 'data/load')
    projects = ['UHC HEDIS', 'HEDIS','ACA-PhysicianCR'] #'Chart Sync','HCR', 'ACA-PhysicianCR','ACA-HospitalCR','UHC HEDIS', 'HEDIS', 'Chart Review'
    sites(df0)
    