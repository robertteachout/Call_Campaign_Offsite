import pandas as pd
import numpy as np
from datetime import date
import time

from pandas.core.frame import DataFrame
import pipeline.clean
import pipeline.score
import pipeline.sprint_schedule
import pipeline.skills
import pipeline.new_inventory
import pipeline.rolled_inventory
from pipeline.etc import daily_piv, time_check, next_business_day, x_Bus_Day_ago, Next_N_BD
from pipeline.tables import tables, zipfiles
import server.insert
import server.query
import server.query_reschedule
import server.query_lc_org_search
import log.log as log

date_format = '%Y-%m-%d'
startTime_1 = time.time()
today = date.today()
tomorrow = next_business_day(today)
tomorrow_str = tomorrow.strftime(date_format)

B10 = Next_N_BD(today, 10)

def full_campaign_file():
    ### 1 -> create new two 2 sprint schedule
    ### 0 -> run daily campaign
    start = tables('pull', 'NA', 'start.csv')
    start['startdate'] = pd.to_datetime(start['startdate']).dt.date
    ### get remaining days in sprint schedule ###
    remaining_bd = start[start['startdate'] >= tomorrow]['startdate'].to_list()

    Day, Master_List = pipeline.sprint_schedule.check_day(start, tomorrow)
    print(f'Day: {Day}, Master List: {Master_List}')
    
    ### Get data and mutate
    filename = str(f'Call_Campaign_v4_{today.strftime("%m%d")}*')
    load = zipfiles('pull', 'NA', filename)
    log.df_len(load)
    
    ### Add to query
    reschedule_sql = server.query_reschedule.sql()
    reSchedule = server.query.query('DWWorking', reschedule_sql, 'Add reschedules')
    log.df_len(reSchedule)

    df_full0 = load.append(reSchedule, ignore_index = True)
    log.df_len(df_full0)

    clean= pipeline.clean.clean(df_full0, tomorrow_str)
    log.df_len(clean)

    # anthem_sql = server.query_reschedule.sql()
    # reSchedule = server.query.query('DWWorking', reschedule_sql, 'Add reschedules')
    anthem = tables('pull', 'NA', 'anthem.csv')
    # f1 = anthem['Outreach Status'] == 'Unscheduled'
    # f2 = anthem['Outreach Status'] == 'Scheduled'
    # anthem = anthem[f1 | f2]['Outreach Id']
    log.df_len(anthem)

    skilled = pipeline.skills.complex_skills(clean, x_Bus_Day_ago(3), anthem)
    log.df_len(skilled)
    skilled['PhoneNumber'] = skilled['PhoneNumber'].astype(str)

    df_full, test0 = pipeline.clean.Test_Load(skilled, today)
    log.df_len(df_full)
    time_check(startTime_1, f'File Load \t{test0}')

    ### Add Schedule -- Daily Groups 
    if Master_List == 1:
        df_full['Daily_Priority'] = 0
        df_full['Daily_Groups'] = 0
        df_full['NewID'] = 0
        clean_for_score = df_full
        list_add = pd.DataFrame()
    else: 
        assignment = tables('pull', 'NA','assignment_map.csv' )
        mapped, names = pipeline.sprint_schedule.daily_maping(df_full, assignment, tomorrow_str)
        log.df_len(mapped)

        ### Add dynamic daily group based on remaining sprint ###
        load_balanced_inv = pipeline.new_inventory.NewID_sprint_load_balance(mapped,tomorrow_str,remaining_bd)
        log.df_len(load_balanced_inv)

        min=[3,Day]
        min.sort()
        lc_search_sql = server.query_lc_org_search.sql(CF=x_Bus_Day_ago(min[0]), NIC=x_Bus_Day_ago(Day),lbd=x_Bus_Day_ago(1))
        lc_search = server.query.query('DWWorking', lc_search_sql, 'Last Call Check')
        log.df_len(lc_search)
        
        rolled, list_add = pipeline.rolled_inventory.check_lc(load_balanced_inv,lc_search, Day, tomorrow_str)
        log.df_len(rolled)

        clean_for_score = pipeline.sprint_schedule.map_priotiy(rolled, Day, names)
        log.df_len(clean_for_score)
    time_check(startTime_1, 'Sprint Schedule')
    
    df_scored = pipeline.score.split_drop_score(clean_for_score)
    log.df_len(df_scored)
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')
    
    orignal_inv = tables('pull','NA',"assignment_map.csv")
    log.df_len(orignal_inv)

    NewID = pipeline.new_inventory.append_new_inventory(df_scored)
    log.df_len(NewID)

    orignal_inv = tables('pull','NA',"assignment_map.csv")
    add_inv = orignal_inv.append(NewID)
    log.df_len(add_inv)
    time_check(startTime_1, 'Add NewIDs to list')

    def Save():
        zipfiles('push',df_scored, tomorrow_str)
        # tables('push',  add_inv,            'assignment_map.csv')
        tables('push',  list_add,           'Missed_ORGs.csv')
        time_check(startTime_1, 'Save files')
        ###################################

    # Run the File
    if Master_List == 0:
        daily_piv(df_scored)
        time_check(startTime_1, 'Create Pivot Table')
        ####################################
        if test0 == 'Fail':
            pass
        else:
            Save()
            server.insert.batch_insert(x_Bus_Day_ago(10).strftime("%Y-%m-%d"),tomorrow_str, df_scored)
        time_check(startTime_1, 'batch_insert')
 
    ###calculate ever 2 weeks
    if Master_List == 1:
        set1, set2 = pipeline.etc.data_list_split(B10, 2)
        df_key = pipeline.sprint_schedule.Assign_Map(df_scored,B10,set1,set2)
        tables('push', df_key, str(f'../assignment_map/{tomorrow_str}.csv'))
        tables('push', df_key, 'assignment_map.csv')
        dt = pipeline.sprint_schedule.static_schedule(B10)
        tables('push', dt, 'start.csv')
        full_campaign_file()

if __name__ == "__main__":
    full_campaign_file()