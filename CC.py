import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

from FileLoad import Final_Load, Number_stats
from Skills import complex_skills, Re_Skill_Project
from Sprint_Schedule import Assign_Map, Map_categories
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath, time_check
from missedORGs import pull_list

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def Full_Campaign_File(Day, Master_List):
    ### Get data and mutate
    df, test = Final_Load()
    if test == 'Fail':
        print('Failed Upload')
    
    time_check(startTime_1, 'File Load')

    df00 = df[df['Skill'] != 'CC_GenpactPRV_Priority'].copy()
    df_test = df[df['Skill'] == 'CC_GenpactPRV_Priority'].copy()

    df0 = Map_categories(df00, Day, Master_List) ### Trigger for lauching sprint schedual
    
    time_check(startTime_1, 'Sprint Schedule')
    
    filter0 = df0['OutreachID'].isin(pull_list().squeeze())
    df0['Daily_Groups'] = np.where(filter0, tomorrow, df0['Daily_Groups'])
    
    time_check(startTime_1, 'Missed ORGs')

    def Score(df1):
        End = len(df1)
        Range = list(range(0, End, 1))
        Retrieval = pd.DataFrame(Range,columns= ['Score'])
        df1['Score'] = Retrieval['Score']
        return df1.sort_values('Score').reset_index(drop= True)

    def spilt(df, sk, Master_List):
            df3 = df[df['Skill'] == sk]
            df4 = df3.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
            df4['Unique_Phone'] = 1
            df_skill = Score(df4)
            # Add Unique ORGs to Rank list 
            df5 = df_skill.append(df3)
            df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
            ### Piped ORGs attached to phone numbers
            df6['OutreachID'] = df6['OutreachID'].astype(str)
            df6['Matches'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
            ### Convert to Child ORG
            # if Master_List == 0:
            #     filter_Child_ORG = df6['Unique_Phone'].isnull() 
            #     df6['Skill'] = np.where(filter_Child_ORG, 'Child_ORG', df6['Skill'])
            # df6 = Re_Skill_Project(df6, 'Scheduled', 'WellMed', 1, 300,'Child_ORG')
            #     return df6
            # else:
            return df6
    
    def drop_dup(df, Master_List):
        df3 = Number_stats(df)
        df_score_spilt = pd.DataFrame()
        ### Sort Order and drop Dups
        for i in df3['Skill'].unique():
            df_score_spilt = df_score_spilt.append(spilt(df3, i, Master_List))
        return df_score_spilt

    df_test = Score(df_test)
    dff = drop_dup(df0, Master_List)
    dffin = dff.append(df_test)
    
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')

    if 'NewID' in dffin.columns:
        NewID = dffin[dffin['NewID'] == 1][['PhoneNumber', 'Skill', 'Daily_Groups', 'NewID']].reset_index(drop=True)
        NewID['Daily_Groups'] = pd.to_datetime(NewID['Daily_Groups']).dt.strftime('%m/%d/%Y')
        mapPath = newPath('Table_Drop','')
        Daily_Groups = pd.read_csv(mapPath + "Assignment_Map.csv", sep=',', error_bad_lines=False, engine="python")
        N_Daily_Groups = Daily_Groups.append(NewID)
        N_Daily_Groups.to_csv(mapPath + 'Assignment_Map.csv', sep=',', index=False)
    
    time_check(startTime_1, 'Add NewIDs to list')
    # print(N_Daily_Groups)
    def Save():
        path = newPath('dump','Group_Rank')
        path2 = newPath('Table_Drop','')
        # dffin.to_csv(path + str(tomorrow) +  '.csv', index=False)
        # dffin.to_csv(path2 + 'Group_Rank.csv', index=False)
        dffin.to_csv(path2 + 'test_transfer.csv', index=False)
        time_check(startTime_1, 'Save files')

        # daily_piv(dffin)

    ## Run the File
    if Master_List == 0:
        return Save()
 
    ###calculate ever 2 weeks
    if Master_List == 1:
        return Assign_Map(dffin)

### [ What Day, test last nights file, Master list ]
Date = {'M1':0,'T1':1,'W1':2,'TH1':3,'F1':4,'M2':5,'T2':6,'W2':7,'TH2':8,'F2':9}

Full_Campaign_File(Date['W1'], 0)

time_check(startTime_1, 'Create Pivot Table')
