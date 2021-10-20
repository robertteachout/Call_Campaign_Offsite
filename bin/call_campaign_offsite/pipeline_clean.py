import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from pipeline_skills import complex_skills
from etc_function import next_business_day
from data_config import zipfiles, tables

today = date.today()
tomorrow = (today + timedelta(days = 1))
yesterday = (today + timedelta(days = -1))
nxt_day = next_business_day(today)

def Load():
    ### Load from data config ###
    filename = str('Call_Campaign_v4_' + today.strftime("%m%d") + '*')
    df = zipfiles('pull', 'NA', filename)
    df.columns = df.columns.str.replace('/ ','')
    df = df.rename(columns=lambda x: x.replace(' ', "_"))
    df['PhoneNumber'] = pd.to_numeric(df['PhoneNumber'], errors='coerce')
    df['Site_Clean_Id'] = pd.to_numeric(df['Site_Clean_Id'], errors='coerce')
    df = df[df['Retrieval_Group'] != 'EMR Remote'] ### Remove and push to separet campaign
    return df

def Format(File):
    Format_Now = File.copy()
    Format_Now['Last_Call'] = pd.to_datetime(Format_Now['Last_Call'], errors='coerce').dt.date
    Format_Now['Recommended_Schedule_Date'] = pd.to_datetime(Format_Now['Recommended_Schedule_Date'], errors='coerce').dt.date
    Format_Now['Project_Due_Date'] = pd.to_datetime(Format_Now['Project_Due_Date'])
    return Format_Now

def Clean_Numbers(df):
    filter1 = df['PhoneNumber'] < 1111111111
    filter2 = df['PhoneNumber'].isna()
    df['PhoneNumber'] = np.where(filter1 | filter2, 9999999999,df['PhoneNumber'])
    return df

def region_col(df):
    lookup = tables('pull', 'NA', 'Region_Lookup.csv')
    lookup = lookup[['State', 'Region']]
    return pd.merge(df, lookup, how="left", on=["State"])

def Last_Call(df):
    df1 = df
    df1 = df1[df1['Last_Call'].notnull()].copy()
    df1['New'] = tomorrow

    df1['DateDiff'] = df1['New'] - df1['Last_Call']
    df1 = df1.copy()
    df1['Age'] = (df1['DateDiff']/np.timedelta64(1,'D')).astype(int)

    df['Age'] = 0
    df['Age'] = df1['Age']
    df['age_category'] = 0

    filter = df['Last_Call'].isna()
    df['age_category'] = np.where(filter, 0, df['age_category'])
    
    def flt(df0, category, start, stop):
        df = df0
        ft = (df['Age'] >= start) & (df['Age'] <= stop)
        df['age_category'] = np.where(ft, category, df['age_category'])
        return df
    
    df = flt(df,  5,  0,  5)
    df = flt(df, 10,  6, 10)
    df = flt(df, 15, 11, 15)
    df = flt(df, 20, 16, 20)
    filter = (df['Age'] >= 21)
    df['age_category'] = np.where(filter, 21, df['age_category'])
    df['Age'] = df['Age'].fillna(0)
    return df

def Test_Load(df):
        df0 = df
        test = df0[df0['Last_Call'] == today]['Last_Call']
        if today == test.max():
            test_results = 'Pass'
        else:
            test_results = 'Fail'
        return test_results

def Number_stats(df):
    df0 = df
    audit_sort = {'RADV':0, 'Medicaid Risk':1, 'HEDIS':2, 'Specialty':3,  'ACA':4, 'Medicare Risk':5}
    name_sort = {'Unscheduled':0, 'Escalated':2, 'PNP Released':1,'Past Due':3,'Scheduled':4}
    rm_sort = {'EMR Remote': 0, 'HIH - Other': 2, 'Onsite':1,'Offsite':3}
    age_sort = {21: 0, 0: 1, 20:2, 15:3, 10:4, 5:5}
    df0['status_sort'] = df0['Outreach_Status'].map(name_sort)
    df0['rm_sort'] = df0['Retrieval_Group'].map(rm_sort)
    df0['age_sort'] = df0['age_category'].map(age_sort)
    df0['audit_sort'] = df0['Audit_Type'].map(audit_sort)
    df3 = df0
    if not 'Daily_Priority' in df3.columns:
        df3['Daily_Priority'] = 0
    df3 = df3.sort_values(by = ['Daily_Priority','audit_sort','age_sort']).reset_index(drop = True)
    return df3

def Final_Load():
    df = Last_Call(region_col(Clean_Numbers(Format(Load()))))
    df['Load_Date'] = nxt_day.strftime("%Y-%m-%d")
    df['Daily_Groups'] = 0
    df['Cluster'] = 0
    ### Add info to main line and reskill
    df2 = df.groupby(['PhoneNumber']).agg({'PhoneNumber':'count'}).rename(columns={'PhoneNumber':'OutreachID_Count'}).reset_index()
    df = pd.merge(df,df2, on='PhoneNumber')
    df = complex_skills(df)
    test = Test_Load(df)
    return df, test

if __name__ == "__main__":
    df, test2 = Final_Load()
    print(df)