import numpy as np
import pandas as pd
import re
from dataclasses import dataclass

@dataclass
class business_lines:
    business_line:str
    projects: list

def business(full, new_line, projects)
    try:
        full.append( business_lines(new_line, projects) ) 
    except:
        full = [business_lines(new_line, projects)]
b2 = business_lines('Medicare', ['RADV'])
b1 = business_lines('RADV', ['RADV'])
b1 = business_lines('RADV', ['RADV'])
b1 = business_lines('RADV', ['RADV'])
b1 = business_lines('RADV', ['RADV'])



def match(df, match):
    group = 'Project_Type'
    # full df, group by location & project, get smallest value for the match in each projects
    t2 = df.groupby([match, group]).agg({'sla':'min','days_before_out_sla':'min'}).reset_index()
    # group projects by location | match
    t2['groups'] = t2.groupby(match)[group].transform(lambda x: ' | '.join(x))
    # group by group of projects, find smallest value for each location
    t3 = t2.groupby(['groups',match]).agg({'sla':'min','days_before_out_sla':'min'}).reset_index()
    # pivot groups by smallest value, counting sites
    gp = t3.groupby(['groups','sla','days_before_out_sla'])[match].count()
    return gp 

def buckets(df):
    split = {
        'RADV':'RADV',
        'Medicare': '(Chart Sync|HCR)',
        'Medicaid':'(Chart Review|Medicaid- HospCR|Clinical Review MCaid PhyCR)',
        'ACA':'(ACA-PhysicianCR|ACA-HospitalCR)',
        'UHC HEDIS':'UHC HEDIS',
        'HEDIS':'HEDIS_c'
        }
    split = {'RADV': '(RADV)',
        'Medicare': '(Chart Sync|HCR)',
        'Medicaid': '(Chart Review|Medicaid- HospCR|Clinical Review MCaid PhyCR)',
        'ACA': '(ACA-PhysicianCR|ACA-HospitalCR)',
        'UHC HEDIS': '(UHC HEDIS)',
        'HEDIS': '(HEDIS_c)'}

    df['Bucket'] = 'none'

    for bucket, search in split.items():
        f1 = df.groups.apply(lambda x: bool(re.search(search, x)))
        df.Bucket = np.where(f1, bucket, df.Bucket)
    return df

def sites(df0, projects, project=None):
    # create column
    df0['days_before_out_sla'] = df0.sla - df0.age + 1
    df0.days_before_out_sla = np.where(df0.days_before_out_sla < 0, 0, df0.days_before_out_sla)
    
    # 
    if projects == None:
        f1 = df0.Project_Type.isin([x for x in projects if x not in project])
        df0['days_before_out_sla'] = np.where(f1, np.nan, df0.days_before_out_sla)
    
    df = df0[df0.Project_Type.isin(projects)].copy()
    
    f1 = df.Project_Type == 'HEDIS'
    df.Project_Type = np.where(f1, 'HEDIS_c', df.Project_Type)

    cf = df[(df.MasterSiteId == 1000838) | (df.MasterSiteId.isna())]
    msid = df[(df.MasterSiteId != 1000838) & (df.MasterSiteId.notna())]

    t  = match(cf,'PhoneNumber')
    t2 = match(msid,'MasterSiteId')
    t3 = (t2 + t).reset_index()
    t3.columns = ['groups','sla','days_before_out_sla','count']
    return buckets(t3)


if __name__ == "__main__":
    from datetime import date
    from pipeline.tables import tables
    today = date.today()
    today_str = today.strftime('%Y-%m-%d')
    import os

    df0 = tables('pull','na',f"{today_str}.zip", 'data/load')
    from dataclasses import dataclass

    @dataclass
    class business_lines:
        business_line:str
        projects: list

    def business(full, new_line, projects):
        full = full.append( business_lines(new_line, projects) ) 

    def clean_lists(ls):
        l = []
        for i in ls:
            if i is list():
                l += clean_lists(i)
            else:
                l += i
        return l
    
    all_business = []
    business(all_business, 'RADV',      ['RADV'])
    business(all_business, 'Medicare',  ['Chart Sync','HCR'])
    business(all_business, 'Medicaid',  ['Chart Review','Medicaid- HospCR','Clinical Review MCaid PhyCR'])
    business(all_business, 'ACA',       ['ACA-PhysicianCR','ACA-HospitalCR'])
    business(all_business, 'UHC HEDIS', ['UHC HEDIS'])
    business(all_business, 'HEDIS',     ['HEDIS_c'])

    all_projects = clean_lists([project.projects for project in all_business])

    
    for i in all_business:
        if i.business_line == :
            isolate = i.projects 

    projects = ['UHC HEDIS', 'HEDIS','ACA-PhysicianCR','ACA-HospitalCR','Chart Review','Medicaid- HospCR'] #'Chart Sync','HCR', 'ACA-PhysicianCR','ACA-HospitalCR','UHC HEDIS', 'HEDIS', 'Chart Review'
    project = 'UHC HEDIS'
    df = sites(df0, all_projects, project)#.to_csv(f'data/burndown/{name}.csv',index=False)
    print(df)

    