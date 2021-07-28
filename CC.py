import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

from FileLoad import Final_Load, Number_stats
from Skills import complex_skills, Re_Skill_Project
from Sprint_Schedule import Assign_Map, Map_categories
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def Full_Campaign_File(Day, Precheck, Master_List):
    ### Get data and mutate
    df, test = Final_Load(Precheck)
    if test == 'Fail':
        print('Failed Upload')
    
    df0 = Map_categories(df, Day, Master_List) ### Trigger for lauching sprint schedual

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
            if Master_List == 0:
                filter_Child_ORG = df6['Unique_Phone'].isnull() 
                df6['Skill'] = np.where(filter_Child_ORG, 'Child_ORG', df6['Skill'])
                df6 = Re_Skill_Project(df6, 'Scheduled', 'WellMed', 1, 300,'Child_ORG')
                return df6
            else:
                return df6
    
    def drop_dup(df, Master_List):
        df3 = Number_stats(df)
        df_score_spilt = pd.DataFrame()
        ### Sort Order and drop Dups
        for i in df3['Skill'].unique():
            df_score_spilt = df_score_spilt.append(spilt(df3, i, Master_List))
        return df_score_spilt

    df = drop_dup(df0, Master_List)

    def Save():
        path = newPath('dump','Group_Rank')
        path2 = newPath('Table_Drop','')
        df.to_csv(path + str(tomorrow) +  '.csv', index=False)
        df.to_csv(path2 + 'Group_Rank.csv', index=False)
        daily_piv(df)

    ### Run the File
    if Master_List == 0:
        return Save()

    ###calculate ever 2 weeks
    if Master_List == 1:
        return Assign_Map(df)

### [ What Day, test last nights file, Master list ]
Full_Campaign_File(2, 0, 0)

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")