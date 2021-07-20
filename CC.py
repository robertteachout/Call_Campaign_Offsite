import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import holidays
import time
import string

from FileLoad import Final_Load
# from Cluster import Clusters
from Skills import complex_skills
from Sprint_Schedule import Assign_Map, Map_categories
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def Full_Campaign_File(Day, Precheck, Master_List):
    ### Get data and mutate
    df0, genpact, wellmed, test = Final_Load(Precheck)
    if test == 'Fail':
        return print('Fail File Upload')
    else:
        # print('Pass')
        df0 = Map_categories(df0, Day, Master_List) ### Trigger for lauching sprint schedual

    def Number_stats(df):
        df0 = df
        audit_sort = {'RADV':0, 'Medicaid Risk':1, 'HEDIS':2, 'Specialty':3,  'ACA':4, 'Medicare Risk':5}
        name_sort = {'Escalated':1, 'Unscheduled':2,'PNP Released':0,'Past Due':3,'Scheduled':4}
        rm_sort = {'EMR Remote': 0, 'HIH - Other': 2, 'Onsite':1,'Offsite':3}
        age_sort = {21: 0, 0: 1, 20:2, 15:3, 10:4, 5:5}
        df0['status_sort'] = df0['Outreach Status'].map(name_sort)
        df0['rm_sort'] = df0['Retrieval Group'].map(rm_sort)
        df0['age_sort'] = df0['age_category'].map(age_sort)
        df0['audit_sort'] = df0['Audit Type'].map(audit_sort)

        df_dummy_status = pd.get_dummies(df0['Outreach Status'])
        df1 = df0.join(df_dummy_status)
        df1['Unique_Phone'] = 0 

        col_stat = df0['Outreach Status'].unique()
        d1 = dict.fromkeys(col_stat, 'sum')
        col = {'TotalCharts':'sum','Cluster':'mean',**d1}
        ### Unique Numbers count and status
        df2 = df1.groupby(['PhoneNumber']).agg(col).rename(columns={'TotalCharts':'TotalChartsAgg','Cluster':'Cluster_Avg'}).reset_index()
        ### Add info to main line and reskill
        df3 = pd.merge(df0,df2, on='PhoneNumber')
        df3 = complex_skills(df3)
        if not 'Daily_Priority' in df3.columns:
            df3['Daily_Priority'] = 0
        df3 = df3.sort_values(by = ['Daily_Priority','audit_sort','age_sort', 'Unscheduled', 'Cluster_Avg'], ascending= [True, True, True, False, True]).reset_index(drop = True)
        return df3

        ### Rank and append duplicate list
    def Rank_Individual_skill(df):
        df4 = df
        Skills = df4['Skill'].unique()
        df_skill = pd.DataFrame()

        def Score(df, skill):
            df1 = df[df['Skill'] == skill].reset_index(drop= True)
            End = len(df1)
            Range = list(range(0, End, 1))
            Retrieval = pd.DataFrame(Range,columns= ['Score'])
            df1['Score'] = Retrieval['Score']
            return df1

        for i in Skills:
            df_skill = df_skill.append(Score(df4, i))
        return df_skill.sort_values('Score').reset_index(drop= True)

    def drop_dup(df):
        df3 = Number_stats(df)
        ### Sort Order and drop Dups
        df4 = df3.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
        df4['Unique_Phone'] = 1
        df_skill = Rank_Individual_skill(df4)
        # Add Unique ORGs to Rank list 
        df5 = df_skill.append(df3)
        df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
        return df6

    # df_skill = Rank_Individual_skill(df4)
    df_main = drop_dup(df0)
    genpact = drop_dup(genpact)
    wellmed = drop_dup(wellmed)

    df6 = df_main.append(genpact).append(wellmed)
    

    ### Piped ORGs attached to phone numbers
    df6['OutreachID'] = df6['OutreachID'].astype(str)
    df6['Matches'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])

    df = df6

    def Save():
        path = newPath('dump','Group_Rank')
        path2 = newPath('Table_Drop','')
        df.to_csv(path + str(tomorrow) +  '.csv', index=False)
        df.to_csv(path2 + 'Group_Rank.csv', index=False)
        df_skill = df[df['Unique_Phone'] == 1]
        return daily_piv(df_skill)

    ### Run the File
    if Master_List == 0:
        return Save()

    ###calculate ever 2 weeks
    if Master_List == 1:
        df_skill = df[df['Unique_Phone'] == 1]
        return Assign_Map(df_skill)

### [ What Day, test last nights file, Master list ]
Full_Campaign_File(5, 0, 0)

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")