import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import glob
import os
from Skills import complex_skills
from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath
import csv
csv.field_size_limit(1000)

today = date.today()
tomorrow = (today + timedelta(days = 1))#.strftime("%m/%d/%Y")
yesterday = (today + timedelta(days = -1))#.strftime("%m/%d/%Y")
nxt_day = next_business_day(today)
F_today = str('Call_Campaign_v4_' + today.strftime("%m%d") +'*.txt')
Dpath = newPath('dump','Call_Campaign') + F_today

for file in glob.glob(Dpath):
    filename = file

def Load():
    df = pd.read_csv(filename, sep='|', error_bad_lines=False, engine="python")
    df['PhoneNumber'] = pd.to_numeric(df['PhoneNumber'], errors='coerce')
    df['Site_Clean_Id'] = pd.to_numeric(df['Site_Clean_Id'], errors='coerce')
    df.columns = df.columns.str.replace('/ ','')
    df = df.rename(columns=lambda x: x.replace(' ', "_"))
    return df

def Format(File):
    Format_Now = File.copy()
    Format_Now['Last_Call'] = pd.to_datetime(Format_Now['Last_Call'], errors='coerce').dt.date
    Format_Now['Recommended_Schedule_Date'] = pd.to_datetime(Format_Now['Recommended_Schedule_Date'], errors='coerce').dt.date
    Format_Now['Project_Due_Date'] = pd.to_datetime(Format_Now['Project_Due_Date'])
    return Format_Now

def Clean_Numbers(df):
    df1 = df.dropna(subset=['PhoneNumber'])
    return df1

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

def Final_Load(Precheck):
    df = Last_Call(Clean_Numbers(Format(Load())))
    df = df.loc[df['OutreachID'].notnull()].copy()
    df['Cluster'] = 0
    df['Load_Date'] = nxt_day.strftime("%Y-%m-%d")
    
    df['Daily_Groups'] = 0
    # Precheck = Precheck
    def Test_Load(df, Precheck):
        df0 = df
        today = date.today()
        test = df0[df0['Last_Call'] == today]['Last_Call']
        if today == test.max():
            test_results = print('||| Pass ||| Last_Call Count:\t' + str(test.count()))
        else:
            test_results = 'Fail'
        return test_results
    test = Test_Load(df, Precheck)
    
    df2 = df.groupby(['PhoneNumber']).agg({'PhoneNumber':'count'}).rename(columns={'PhoneNumber':'OutreachID_Count'}).reset_index()
    ### Add info to main line and reskill
    df = pd.merge(df,df2, on='PhoneNumber')

    ft1 = df['Retrieval_Team'] == 'Genpact Offshore'
    ft2 = df['OutreachID_Count'] == 1

    ft3 = df['Project_Type'] == 'WellMed'

    genpact  = df.loc[ft1 & ft2]
    wellmed  = df.loc[ft3]

    df['clean'] = np.where((ft1 & ft2) | ft3, 0, 1)
    df = df[df['clean'] ==1].drop(columns= ['clean'])

    return df, genpact, wellmed, test

# df, genpact, wellmed, test = Final_Load(0)
# def t(df):
#     return df['OutreachID'].count()
# print(t(genpact)+ t(wellmed)+ t(df))

# print(t(Last_Call(Clean_Numbers(Format(Load())))))
# print(genpact)
# print(wellmed)
# print(test)
# print(df)



if __name__ == "__main__":
    print("File will load")