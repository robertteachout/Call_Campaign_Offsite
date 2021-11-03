# from FileLoad import Final_Load
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from pipeline_check_missing import pull_list
from etc_function import next_business_day, Next_N_BD, date_list_split, daily_piv
from data_config import tables
from dbo_query import lc_search
import pipeline_clean

today = date.today()
tomorrow = next_business_day(today)
D2 = next_business_day(tomorrow)
B10 = Next_N_BD(today, 10)

FivDay = today + timedelta(days=7)
test = next_business_day(FivDay)

### Create static two week sprint ###
def static_schedule():
    dt = pd.DataFrame(
            {'startdate'    :B10,
                'Number'    :range(10)} )
    dt['startdate'] = pd.to_datetime(dt['startdate']) 
    return dt

def Load_Assignment():
    assignment = tables('pull', 'NA','Assignment_Map.csv' )
    ### at the begining of new cylce remove cut this section
    assignment['Daily_Groups'] = pd.to_datetime(assignment['Daily_Groups'], format='%Y-%m-%d')
    start = assignment[assignment['Daily_Groups'] >= tomorrow.strftime('%Y-%m-%d')]
    end =  assignment[assignment['Daily_Groups'] < tomorrow.strftime('%Y-%m-%d')]
    assignment = start.append(end).reset_index(drop=True).drop_duplicates(subset='PhoneNumber').sort_values('Daily_Groups').reset_index(drop=True)
    assignment['Daily_Groups'] = pd.to_datetime(assignment['Daily_Groups'], format='%Y-%m-%d').dt.date
    ### end
    assignment = assignment.join(pd.get_dummies(assignment['Daily_Groups']))
    return assignment

def sort(i):
    df = Load_Assignment()
    df0 = df[df[i] == 1]['PhoneNumber']
    return df0

def assignment(df, Add_assignment):
    df_local = df
    filter0 = df_local['PhoneNumber'].isin(sort(Add_assignment).squeeze())
    df_local['Daily_Groups'] = np.where(filter0, Add_assignment, df_local['Daily_Groups'])
    return df_local

def Daily_Maping(df):
    f = df
    load = Load_Assignment()
    names = list(load['Daily_Groups'].unique())
    for i in names:
        f = assignment(f,i)
    return f, names

### Create file with assigned categories to ORG
def Assign_Map(df):
    skills = df['Skill'].unique()
    df_clean = df.drop_duplicates('PhoneNumber')
    df_key = pd.DataFrame()
    B10 = Next_N_BD(today, 10)
    num1 , num2 = date_list_split(B10, 2)
    def assign_skill(sk, j, BusDay):
        df_skill = df_clean[df_clean['Skill'] == sk].reset_index(drop = True)
        if j == 5:
            df_skill = df_skill[df_skill['audit_sort'] <= 2].reset_index(drop = True)
        elif j == 10:
            df_skill = df_skill[df_skill['audit_sort'] > 2].reset_index(drop = True)
        else:
            print('error')
        #### INPUT BY DAY ####
        Sprint = j
        ######################
        df_len = len(df_skill)
        group_size = df_len // Sprint 
        ## What day for what number ##
        listDay = BusDay * group_size
        listDay.sort()
        ## Create Same len list of letters as len of df
        Daily_Priority = pd.DataFrame(listDay, columns=['Daily_Groups'])
        add_back = df_len - len(Daily_Priority)
        Daily_Priority = Daily_Priority.append(Daily_Priority.iloc[[-1]*add_back]).reset_index(drop=True)
        df_skill['Daily_Groups'] = Daily_Priority['Daily_Groups']
        return df_skill[['PhoneNumber', 'Skill','Daily_Groups']]
    def assign_audit(sk):
        D5_1 = assign_skill(sk, 5, list(num1))
        D5_2 = assign_skill(sk, 5, list(num2))
        D10  = assign_skill(sk, 10, B10)
        final = D5_1.append(D5_2).append(D10)
        return final
    ## Add together all skills with uniquely broken out sprints
    for i in skills:
            df_key = df_key.append(assign_audit(i))
    df_key['NewID'] = 0
    tables('push', df_key, str('../assignment_map/' + tomorrow.strftime('%Y-%m-%d') + '.csv'))
    tables('push', df_key, 'Assignment_Map.csv')

    ## Sprint Schedulual Day
def Map_categories(df, Day, test):
    if test == 1:
        df['Daily_Priority'] = 0
        df['Daily_Groups'] = 0
        df['NewID'] = 0
        list_add = pd.DataFrame()
        return df, list_add
    else:
        df, names = Daily_Maping(df)
        df['NewID'] = 0
        filter1 = df['Daily_Groups'] == 0
        df['NewID'] = np.where(filter1, 1, df['NewID'])
        ### Add dynamic daily group based on remaining sprint ###
        df = NewID_sprint_load_balance(df)
        df['OutreachID'] = df['OutreachID'].astype(int)
        if Day != 0:
            ### Add yesterdays daily group that was missed
            list_add = lc_search(CF=3, NIC=Day)
            filter0 = df['OutreachID'].isin(list_add['OutreachID'].squeeze())
            df['Daily_Groups'] = np.where(filter0, tomorrow, df['Daily_Groups'])
            df['rolled'] = np.where(filter0, 1, 0)
        else:
            list_add = pd.DataFrame()
        Sprint = len(names)
        ### Map and Sort
        Sprint_schedule = list(range(0,Sprint))
        Category = names
        Sprint_schedule = Sprint_schedule[-Day:] + Sprint_schedule
        Daily_sort = dict(zip(Category,Sprint_schedule))
        df['Daily_Priority'] = df['Daily_Groups'].map(Daily_sort)
        return df, list_add

def NewID_sprint_load_balance(df):
    if any(df['NewID'] == 1):
        pass
    else:
        return df
    df_new = df[df['NewID'] == 1].reset_index(drop= True)
    if len(df_new) <= 10:
        df_new['Daily_Groups'] = tomorrow.strftime('%Y-%m-%d')
        p = df_new.append(df).drop_duplicates(['OutreachID']).reset_index(drop= True)
        return p
    ### get remaining days in sprint schedule ###
    dt = tables('pull', 'NA', 'start.csv')
    dt['startdate'] = pd.to_datetime(dt['startdate']).dt.date
    BusDay = dt[dt['startdate'] >= next_business_day(today)]['startdate'].to_list()
    ### split total NewIds into groups ###
    group_size = ( len(df_new) // len(BusDay) ) * BusDay
    Daily_Priority = pd.DataFrame(group_size, columns=['Daily_Groups'])
    add_back = len(df_new) - len(Daily_Priority)
    Daily_Priority = Daily_Priority.append(Daily_Priority.iloc[[-1]*add_back]).reset_index(drop=True)
    df_new['Daily_Groups']  = Daily_Priority['Daily_Groups'].reset_index(drop=True)
    p = df_new.append(df).drop_duplicates(['OutreachID']).reset_index(drop= True)
    p['Daily_Groups'] = pd.to_datetime(p['Daily_Groups']).dt.date
    return p

if __name__ == "__main__":
    df, test2 = pipeline_clean.Final_Load()
    print(df)
    df2, list_add = Map_categories(df, 8, 0)
    print(df2)