import pandas as pd
import numpy as np
from datetime import date
import time
from etc_function import daily_piv, time_check, next_business_day, Next_N_BD, x_Bus_Day_ago
from pipeline_clean import Final_Load, Number_stats
from pipeline_schedual import Assign_Map, Map_categories, static_schedual
from data_config import tables, zipfiles
from dbo_insert import Insert_SQL

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def full_campaign_file():
    ### [ What Day, test last nights file, Master list ]
    ### 1 -> create new two 2 sprint schedual
    ### 0 -> run daily campaign
    dt = tables('pull', 'NA', 'start.csv')
    try:
        Day = dt[dt['startdate'] == next_business_day(today).strftime('%Y-%m-%d')]['Number'].to_list()[0]
        Master_List = 0
    except IndexError:
        Day = 1
        Master_List = 1
    print('Day: ' + str(Day) + ', Master List: '+ str(Master_List))
    ### Get data and mutate
    df, test0 = Final_Load()
    time_check(startTime_1, 'File Load \t' + test0)
    ####################################
    df0, list_add = Map_categories(df, Day, Master_List) ### Trigger for lauching sprint schedual
    time_check(startTime_1, 'Sprint Schedule')
    ####################################
    # convert CF last call date to child org / child ORG's won't be affected
    filter1 = df0['Last_Call'] >= x_Bus_Day_ago(3)
    df0['Skill'] = np.where(filter1, 'Child_ORG', df0['Skill'])
    ####################################

    def Score(df1):
        df1['Score'] = range(0,len(df1))
        return df1

    def split(df, sk):
            df0 = df[df['Skill'] == sk]
            
            df1 = df0.join(pd.get_dummies(df0['Outreach_Status']))
            df1['Unique_Phone'] = 0
            d1 = dict.fromkeys(df0['Outreach_Status'].unique(), 'sum')
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
        df_score_split = pd.DataFrame()
        ### Sort Order and drop Dups
        for i in df3['Skill'].unique():
            df_score_split = df_score_split.append(split(df3, i))
        return df_score_split

    df_p_c = drop_dup(df0, Master_List)
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')

    if 'NewID' in df_p_c.columns:
        NewID = df_p_c[df_p_c['NewID'] == 1]
        NewID = NewID[['PhoneNumber', 'Skill', 'Daily_Groups', 'NewID']].reset_index(drop=True)
        NewID['Daily_Groups'] = pd.to_datetime(NewID['Daily_Groups']).dt.strftime('%Y-%m-%d')
        Daily_Groups = tables('pull','NA',"Assignment_Map.csv")
        N_Daily_Groups = Daily_Groups.append(NewID)
        time_check(startTime_1, 'Add NewIDs to list')
        ####################################

    def Save():
        filename = tomorrow.strftime("%Y-%m-%d")
        zipfiles('push', df_p_c, filename)
        tables('push', df_p_c,'Group_Rank.csv')
        tables('push',N_Daily_Groups,"Assignment_Map.csv")
        tables('push', list_add, 'Missed_ORGs.csv')
        time_check(startTime_1, 'Save files')
        ####################################
        daily_piv(df_p_c)
        time_check(startTime_1, 'Create Pivot Table')
        ###################################

    # Run the File
    if Master_List == 0:
        Save()
        Insert_SQL()
        time_check(startTime_1, 'Insert_SQL')
 
    ###calculate ever 2 weeks
    if Master_List == 1:
        Assign_Map(df_p_c)
        dt = static_schedual()
        tables('push', dt, 'start.csv')
        full_campaign_file()

if __name__ == "__main__":
    full_campaign_file()