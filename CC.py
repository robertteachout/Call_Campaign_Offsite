import zipfile
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time
import shutil
import os
from zipfile import ZipFile, ZipInfo
from FileLoad import Final_Load, Number_stats
from Skills import complex_skills, Re_Skill_Project, convert
from Sprint_Schedule import Assign_Map, Map_categories
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath, time_check, x_Bus_Day_ago
from SQL_Table import Insert_SQL

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def Full_Campaign_File(Day, Master_List):
    ### Get data and mutate
    df, test0 = Final_Load()
    time_check(startTime_1, 'File Load \t' + test0)
    ####################################
    df0 = Map_categories(df, Day, Master_List) ### Trigger for lauching sprint schedual
    time_check(startTime_1, 'Sprint Schedule')
    ####################################
    # convert CF last call date to child org / child ORG's won't be affected
    filter1 = df0['Last_Call'] >= x_Bus_Day_ago(3)
    df0['Skill'] = np.where(filter1, 'Child_ORG', df0['Skill'])
    ####################################

    def Score(df1):
        End = len(df1)
        Range = list(range(0, End, 1))
        Retrieval = pd.DataFrame(Range,columns= ['Score'])
        df1['Score'] = Retrieval['Score']
        return df1.sort_values('Score').reset_index(drop= True)

    def spilt(df, sk, Master_List):
            df0 = df[df['Skill'] == sk]
            
            df_dummy_status = pd.get_dummies(df0['Outreach_Status'])
            df1 = df0.join(df_dummy_status)
            df1['Unique_Phone'] = 0
            col_stat = df0['Outreach_Status'].unique()
            d1 = dict.fromkeys(col_stat, 'sum')
            col = {'TotalCharts':'sum','Cluster':'mean',**d1}
            ### Unique Numbers count and status
            df2 = df1.groupby(['PhoneNumber']).agg(col).rename(columns={'TotalCharts':'TotalChartsAgg','Cluster':'Cluster_Avg'}).reset_index()
            ### Add info to main line and reskill
            df3 = pd.merge(df0,df2, on='PhoneNumber')

            ### put Unschedualed as parent
            df3 = df3.sort_values(by= ['PhoneNumber', 'status_sort']).reset_index(drop = True)
            df4 = df3.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
            ### re-rank after breaking it with status sort
            df4 = df4.sort_values(by = ['Daily_Priority','audit_sort','age_sort']).reset_index(drop = True)
            df4['Unique_Phone'] = 1
            df_skill = Score(df4)
            # Add Unique ORGs to Rank list 
            df5 = df_skill.append(df3)
            df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
            ### Piped ORGs attached to phone numbers
            df6['OutreachID'] = df6['OutreachID'].astype(str)
            df6['Matches'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
            return df6
    
    def drop_dup(df, Master_List):
        df3 = Number_stats(df)
        df_score_spilt = pd.DataFrame()
        ### Sort Order and drop Dups
        for i in df3['Skill'].unique():
            df_score_spilt = df_score_spilt.append(spilt(df3, i, Master_List))
        return df_score_spilt

    dffin = drop_dup(df0, Master_List)
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')

    if 'NewID' in dffin.columns:
        NewID = dffin[dffin['NewID'] == 1][['PhoneNumber', 'Skill', 'Daily_Groups', 'NewID']].reset_index(drop=True)
        NewID['Daily_Groups'] = pd.to_datetime(NewID['Daily_Groups']).dt.strftime('%m/%d/%Y')
        mapPath = newPath('Table_Drop','')
        Daily_Groups = pd.read_csv(mapPath + "Assignment_Map.csv", sep=',', on_bad_lines='skip', engine="python")
        N_Daily_Groups = Daily_Groups.append(NewID)
        N_Daily_Groups.to_csv(mapPath + 'Assignment_Map.csv', sep=',', index=False)
        time_check(startTime_1, 'Add NewIDs to list')
        ####################################

    def Save():
        path = newPath('dump','Group_Rank')
        path2 = newPath('Table_Drop','')
        os.chdir('../')
        os.chdir('dump/Group_Rank')
        filename = tomorrow.strftime("%Y-%m-%d")
        with ZipFile(filename + '.zip', 'w', compression=zipfile.ZIP_DEFLATED) as zip:
            dffin.to_csv(filename + '.csv', index= False)
            zip.write(filename + '.csv')
            zip.close()
            os.remove(filename + '.csv')
        dffin.to_csv(path2 + 'Group_Rank.csv', index=False)
        time_check(startTime_1, 'Save files')
        ####################################
        daily_piv(dffin)
        time_check(startTime_1, 'Create Pivot Table')
        ###################################

    # Run the File
    if Master_List == 0:
        Save()
        Insert_SQL()
        time_check(startTime_1, 'Insert_SQL')

 
    ###calculate ever 2 weeks
    if Master_List == 1:
        dt = pd.DataFrame(
            {'startdate':B10,
             'Number'   :range(10)}
            )
        dt.to_csv('start.csv', index= False)
        return Assign_Map(dffin)

### [ What Day, test last nights file, Master list ]
dt = pd.read_csv('start.csv')
dt = dt[dt['startdate'] == next_business_day(today).strftime('%m/%d/%Y')]['Number'][0]
Full_Campaign_File(dt, 0)

