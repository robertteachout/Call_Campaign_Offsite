import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time
from FileLoad import Final_Load
# from Cluster import Clusters
from Skills import complex_skills
# from CallCampaignRank_V10 import Call_Campaign
startTime_1 = time.time()

today = date.today()
tomorrow = (today + timedelta(days = 1)).strftime("%Y-%m-%d")

### Get data and mutate
df0 = Final_Load()
name_sort = {'Escalated':0, 'Unscheduled':1,'PNP Released':2,'Past Due':3,'Scheduled':4}

rm_sort = {'EMR Remote': 0, 'HIH - Other': 2, 'Onsite':1,'Offsite':3}
age_sort = {21: 0, 0: 1, 20:2, 15:3, 10:4, 5:5}
df0['name_sort'] = df0['Outreach Status'].map(name_sort)
df0['rm_sort'] = df0['Retrieval Group'].map(rm_sort)
df0['age_sort'] = df0['Group Number'].map(age_sort)

df_dummy_status = pd.get_dummies(df0['Outreach Status'])
# df_dummy_age = pd.get_dummies(df0['Group Number'])
df1 = df0.join(df_dummy_status)#.join(df_dummy_age) #,21:'sum', 0:'sum', 20:'sum', 15:'sum', 10:'sum', 5:'sum'
df1['Unique Phone'] = 0 

### Unique Numbers count and status
df2 = df1.groupby(['PhoneNumber']).agg({'PhoneNumber':'count','Cluster':'mean','Escalated':'sum','PNP Released':'sum','Past Due':'sum','Scheduled':'sum','Unscheduled':'sum',}).rename(columns={'PhoneNumber':'OutreachID Count','Cluster':'Cluster_Avg'})
df2 = df2.reset_index()
# print(df2[[21,0,15,10]].sort_values(0, ascending= False))


### Add info to main line and reskill
df3 = pd.merge(df0,df2, on='PhoneNumber')
df3 = complex_skills(df3)

### Sort Order and drop Dups
df4 = df3.sort_values(by = ['rm_sort', 'name_sort','age_sort', 'Unscheduled', 'Cluster_Avg'], ascending= [True,True, True, False, True]).reset_index(drop = True)

df4 = df4.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
df4['Unique Phone'] = 1

### Rank and append duplicate list
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
    df_skill = df_skill.sort_values('Score').reset_index(drop= True)
# print(df_skill.groupby(['Skill'])['Score'].count())

### Map categories


### Add Unique ORGs to Rank list 
df5 = df_skill.append(df3)
df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)

### Piped ORGs attached to phone numbers
df6['OutreachID'] = df6['OutreachID'].astype(str)
df6['ORG list'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x))

df = df6
# df = df[df['ORG Stat'] == 1]
# print(df)
# print(df4.groupby('Skill').agg({'OutreachID Count':'count', 'TotalCharts':'sum'}).sort_values('OutreachID Count', ascending= False))
# print(df.groupby('Skill').agg({'OutreachID Count':'count', 'TotalCharts':'sum'}).sort_values('OutreachID Count', ascending= False))

def Save(Where):
    if Where == 'Work':
        # path = 'C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\dump\\Group_Rank\\'
        path2 = 'C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\'
    else:
        path = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\dump\\Group_Rank\\'
        path2 = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\'

    # df.to_csv(path + str(tomorrow) +  '.csv', index=False)

    df.to_csv(path2 + 'Group_Rank.csv', index=False)

Save('Work')



executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")