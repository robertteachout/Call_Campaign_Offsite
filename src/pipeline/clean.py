import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

today = date.today()
tomorrow = (today + timedelta(days = 1))
yesterday = (today + timedelta(days = -1))

def formate_col(df, col, type):
    if type == 'date':
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    elif type == 'num':
        df[col] = pd.to_numeric(df[col], errors='coerce', downcast="integer")
    else:
        print('add new type')
    return df

def format(df):
    df.columns = df.columns.str.replace('/ ','')
    df = df.rename(columns=lambda x: x.replace(' ', "_"))
    df = formate_col(df, 'PhoneNumber', 'num')
    df = formate_col(df, 'Site_Clean_Id', 'num')
    df = formate_col(df, 'Last_Call', 'date')
    df = formate_col(df, 'Project_Due_Date', 'date')
    df = formate_col(df, 'Recommended_Schedule_Date', 'date')
    return df

def clean_num(df):
    filter1 = df['PhoneNumber'] < 1111111111
    filter2 = df['PhoneNumber'].isna()
    df['PhoneNumber'] = np.where(filter1 | filter2, 9999999999,df['PhoneNumber'])
    return df

def Last_Call(df):
    df.drop('Age', axis=1, inplace=True)
    df['age'] = (tomorrow - df['Last_Call']).dt.days

    f1 = df.Last_Call.isna()
    df.age = np.where(f1, df.age.max(), df.age)
    
    f1 = df.age < df.DaysSinceCreation
    df.age = np.where(f1, df.age, df.DaysSinceCreation)

    cut_bins = [0, 5, 10, 15, 20, 10000]
    label_bins = [5, 10, 15, 20, 21]
    df['age_category'] = pd.cut(df['age'], bins=cut_bins, labels=label_bins, include_lowest=True)
    return df

def check_load(df, today):
        if any(df['Last_Call'] == today):
            test_results = 'Pass'
        else:
            test_results = 'Fail'
        return df, test_results

### Covert fire flag with specific client project to a 5 day cycle _> add to RADV
def fire_flag(df, skill_name):
    filer1 = df['Score'].str[:1].isin(1,2,3)
    df['Outreach_Status'] = np.where(filer1, skill_name, df['Outreach_Status'])
    return df

def add_columns(df, tomorrow_str):
    ### init columns
    df['Load_Date'] = tomorrow_str
    df['Daily_Groups'] = 0
    df['Daily_Priority'] = 0
    df['rolled'] = 0 
    df['NewID'] = 0

    ### score columns
    # map
    audit_sort  = {'RADV':1, 'Medicaid Risk':1, 'HEDIS':2, 'Specialty':3,  'ACA':0, 'Medicare Risk':5}
    age_sort    = {21:0, 20:1, 15:2, 10:3, 5:4}
        # name_sort = {'Unscheduled':0, 'Escalated':2, 'PNP Released':1,'Past Due':3,'Scheduled':4}
        # rm_sort = {'EMR Remote': 0, 'HIH - Other': 2, 'Onsite':1,'Offsite':3}
    df['audit_sort'] = df['Audit_Type'].map(audit_sort)
    df['age_sort'] = df['age_category'].map(age_sort)
    # use map 
    f1 = df.audit_sort <=2
    df['sla'] = np.where(f1, 5, 10)
    f1 = df.sla >= df.age
    df['meet_sla'] = np.where(f1, 1,0)
    # togo charts
    bucket_amount = 5
    labels = list(reversed([x for x in range(bucket_amount)]))
    df['togo_bin'] = pd.cut(df.ToGoCharts, bins=bucket_amount, labels=labels)
    df.togo_bin = df.togo_bin.astype(int)
    # needed for merge
    df['PhoneNumber'] = df['PhoneNumber'].astype(str)
    return df

def clean(df, tomorrow_str):
    df = Last_Call(clean_num(format(df))).reset_index(drop=True)
    new_col = add_columns(df, tomorrow_str)
    ### Add info to main line and reskill
    df2 = new_col.groupby(['PhoneNumber']).agg({'PhoneNumber':'count'}).rename(columns={'PhoneNumber':'OutreachID_Count'}).reset_index()
    df = pd.merge(new_col,df2, on='PhoneNumber')
    return df

if __name__ == "__main__":
    print('test')
