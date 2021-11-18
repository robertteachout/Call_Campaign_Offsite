import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

today = date.today()
tomorrow = (today + timedelta(days = 1))
yesterday = (today + timedelta(days = -1))

def formate_col(df, col, type):
        if type == 'date':
            df[f'{col}'] = pd.to_datetime(df[f'{col}'], errors='coerce').dt.date
        elif type == 'num':
            df[f'{col}'] = pd.to_numeric(df[f'{col}'], errors='coerce', downcast="integer")
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
    df['age'] = (tomorrow - df['Last_Call']).dt.days
    cut_bins = [0, 5, 10, 15, 20, 10000]
    label_bins = [ 5, 10, 15, 20, 21]
    df['age_category'] = pd.cut(df['age'], bins= cut_bins, labels=label_bins)
    df = formate_col(df, 'age_category', 'num')
    df = formate_col(df, 'age', 'num')
    df[['age', 'age_category']] = df[['age', 'age_category']].fillna(0).round(decimals=0).astype(object)
    return df

def Test_Load(df, today):
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

def clean(df):

    # df = Last_Call(region_col(clean_num(format(df))))
    df = Last_Call(clean_num(format(df)))
    df = df[df['Retrieval_Group'] != 'EMR Remote'] ### Remove and push to separet campaign
    df['Daily_Groups'] = 0
    df['Cluster'] = 0
    ### Add info to main line and reskill
    df2 = df.groupby(['PhoneNumber']).agg({'PhoneNumber':'count'}).rename(columns={'PhoneNumber':'OutreachID_Count'}).reset_index()
    df = pd.merge(df,df2, on='PhoneNumber')
    return df

if __name__ == "__main__":
    print('test')
