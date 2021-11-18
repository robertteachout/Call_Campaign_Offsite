import pandas as pd
import numpy as np
from datetime import date
import time
from etc_function import daily_piv, time_check, next_business_day, Next_N_BD, x_Bus_Day_ago
from pipeline_clean import Final_Load, Number_stats
from pipeline_schedule import Assign_Map, Map_categories, static_schedule
from pipeline.tables import tables, zipfiles
from dbo_insert import Insert_SQL
import log.log as log

startTime_1 = time.time()
today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)

def full_campaign_file():
    ### 1 -> create new two 2 sprint schedule
    ### 0 -> run daily campaign
    dt = tables('pull', 'NA', 'start.csv')
    try:
        Day = dt[dt['startdate'] == next_business_day(today).strftime('%Y-%m-%d')]['Number'].to_list()[0]
        Master_List = 0
    except IndexError:
        Day = 1
        Master_List = 1
    
    print(f'Day: {Day}, Master List: {Master_List}')
    ### Get data and mutate
    df, test0 = Final_Load()
    log.df_len(df)
    time_check(startTime_1, f'File Load \t{test0}')

    df0, list_add = Map_categories(df, Day, Master_List) ### Trigger for lauching sprint schedule
    log.df_len(df0)

    time_check(startTime_1, 'Sprint Schedule')

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

            ### put Unscheduleed as parent
            df3 = df3.sort_values(by= ['PhoneNumber', 'status_sort']).reset_index(drop = True)
            df4 = df3.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
            ### re-rank after breaking it with status sort
            if not 'rolled' in df4.columns:
                df4['rolled'] = 0
            df4 = df4.sort_values(by = ['Daily_Priority','rolled','audit_sort','age_sort'], ascending=[True, False, True, True]).reset_index(drop = True)
            df4['Unique_Phone'] = 1
            ### add score column
            df_skill = df4
            df_skill['Score'] = range(0,len(df_skill))
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
    log.df_len(df_p_c)

    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')

    if 'NewID' in df_p_c.columns:
        NewID = df_p_c[df_p_c['NewID'] == 1]
        log.df_len(NewID)
        NewID = NewID[['PhoneNumber', 'Skill', 'Daily_Groups', 'NewID']].reset_index(drop=True)
        NewID['Daily_Groups'] = pd.to_datetime(NewID['Daily_Groups']).dt.strftime('%Y-%m-%d')
        Daily_Groups = tables('pull','NA',"Assignment_Map.csv")
        log.df_len(Daily_Groups)

        N_Daily_Groups = Daily_Groups.append(NewID)
        log.df_len(N_Daily_Groups)

        time_check(startTime_1, 'Add NewIDs to list')
        ####################################

    def Save():
        filename = tomorrow.strftime("%Y-%m-%d")
        zipfiles('push',df_p_c, filename)
        # tables('push',  df_p_c,             'Group_Rank.csv')
        tables('push',  N_Daily_Groups,     'Assignment_Map.csv')
        tables('push',  list_add,           'Missed_ORGs.csv')
        # tables('push',  count_phone(df_p_c),'unique_phone_count.csv')
        time_check(startTime_1, 'Save files')
        ###################################

    # Run the File
    if Master_List == 0:
        daily_piv(df_p_c)
        time_check(startTime_1, 'Create Pivot Table')
        ####################################
        if test0 == 'Fail':
            pass
        else:
            Save()
            Insert_SQL()
            time_check(startTime_1, 'Insert_SQL')
 
    ###calculate ever 2 weeks
    if Master_List == 1:
        Assign_Map(df_p_c)
        dt = static_schedule()
        tables('push', dt, 'start.csv')
        full_campaign_file()

if __name__ == "__main__":
    full_campaign_file()