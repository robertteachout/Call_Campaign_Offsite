from FileLoad import Final_Load
import pandas as pd
import numpy as np
import string
from datetime import date, timedelta, datetime
import holidays
import time
from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath
today = date.today()
tomorrow = next_business_day(today)
D2 = next_business_day(tomorrow)

# df = Final_Load()

def Load_Assignment():
    path = newPath('Table_Drops','')
    # path = "C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\"
    Cluster = pd.read_csv(path + "Assignment_Map.csv", sep=',', error_bad_lines=False, engine="python")
    Cluster = Cluster.join(pd.get_dummies(Cluster['Daily_Groups']))
    return Cluster
# map_piv(Load_Assignment())
def sort(i):
    df = Load_Assignment()
    df0 = df[df[i] == 1]['PhoneNumber']
    return df0

def Cluster(df, Add_Cluster):
    df_local = df
    filter0 = df_local['PhoneNumber'].isin(sort(Add_Cluster).squeeze())
    df_local['Daily_Groups'] = np.where(filter0, Add_Cluster, df_local['Daily_Groups'])
    return df_local

def Daily_Maping(df):
    f = df
    load = Load_Assignment()
    names = list(load['Daily_Groups'].unique())
    for i in names:
        f = Cluster(f,i)
    f['Daily_Groups'] = f['Daily_Groups'].replace('0', D2.strftime('%m/%d'))
    # print(f['Daily_Groups'].isna().sum())
    # filter0 = f['Daily_Groups'] == 0 
    # f['Daily_Groups'] = np.where(filter0, '07/08', f['Daily_Groups'])
    
    # f['Daily_Groups'] = f['Daily_Groups'].fillna(method='ffill')
    return f, names
# df1, label = Daily_Maping(df)
# df1 = df1.drop_duplicates(subset = 'PhoneNumber')
# print(df1.groupby('Daily_Groups')['PhoneNumber'].count())
B10 = Next_N_BD(10)

### Create file with assigned categories to ORG
def Assign_Map(df):
    skills = df['Skill'].unique()
    df_key = pd.DataFrame()
    def assign_skill(sk):
        df_skill = df[df['Skill'] == sk].reset_index(drop = True)
        #### INPUT BY DAY ####
        Sprint = 5
        ######################
        df_len = len(df_skill.index)
        group_size = df_len // Sprint 
        ## What day for what number ##
        listDay = B10 * group_size
        listDay.sort()
        ## Create Same len list of letters as len of df
        Daily_Priority = pd.DataFrame(listDay, columns=['Daily_Groups'])
        add_back = df_len - len(Daily_Priority)
        Daily_Priority = Daily_Priority.append(Daily_Priority.iloc[[-1]*add_back]).reset_index(drop=True)
        df_skill['Daily_Groups'] = Daily_Priority['Daily_Groups']
        return df_skill[['PhoneNumber', 'Skill','Daily_Groups']]
    ## Add together all skills with uniquely broken out sprints
    for i in skills:
        df_key = df_key.append(assign_skill(i))
    path1 = newPath('dump','Assignment_Map')
    path2 = newPath('Table_Drops','')
    # path1 = 'C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\dump\\Assignment_Map\\'
    # path2 = 'C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\'
    df_key.to_csv(path1 + str(tomorrow) +'.csv', index=False)
    df_key.to_csv(path2 + 'Assignment_Map' +  '.csv', index=False)
    return map_piv(df_key)

    ## Sprint Schedulual Day
def Map_categories(df, Day, test):
    df, names = Daily_Maping(df)
    if test == 1:
        df['Daily_Priority'] = 0
        df['Daily_Groups'] = 0
        return df
    else:
        Sprint = len(names)
        ### Map and Sort
        Sprint_schedual = list(range(0,Sprint))
        Category = names
        Sprint_schedual = Sprint_schedual[-Day:] + Sprint_schedual
        Daily_sort = dict(zip(Category,Sprint_schedual))
        df['Daily_Priority'] = df['Daily_Groups'].map(Daily_sort)
        return df

if __name__ == "__main__":
    print("Clusters")