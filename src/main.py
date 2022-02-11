import pandas as pd
import numpy as np
from datetime import date
import time

import pipeline.clean
import pipeline.score
import pipeline.sprint_schedule
import pipeline.skills
from pipeline.etc import daily_piv, time_check, next_business_day, x_Bus_Day_ago, Next_N_BD, date_list_split
from pipeline.tables import tables, zipfiles, count_phone

import server.insert
import server.secret
import server.query
import server.queries.reschedule
import server.queries.mastersiteID
import log.log as log

servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
startTime_1 = time.time()
today = date.today()
tomorrow = next_business_day(today)
tomorrow_str = tomorrow.strftime(date_format)

B10 = Next_N_BD(today, 10)

def main():
    ### load & transform
    filename = str(f'Call_Campaign_v4_{today.strftime("%m%d")}*')
    load = zipfiles('pull', 'NA', filename)
    log.df_len('load', load)

    # test last call date = today
    tested, test = pipeline.clean.check_load(load, today)
    log.df_len('tested', tested)
    time_check(startTime_1, f'File Load \t{test}')
    
    ### Master Site ID
    mastersite_sql = server.queries.mastersiteID.sql()
    mastersite = server.query.query(servername, database,  mastersite_sql, 'Add mastersiteID')
    mapped = pd.merge(tested, mastersite, how='left', on='OutreachID')
    log.df_len('mastersiteID', mapped)
    time_check(startTime_1, 'msid map')

    ## Reschedules
    # reschedule_sql = server.queries.reschedule.sql()
    # reSchedule = server.query.query(servername, database,  reschedule_sql, 'Add reschedules')
    # log.df_len('reSchedule', reSchedule)

    # df_full0 = load.append(reSchedule, ignore_index = True)
    # log.df_len('df_full', df_full0)

    # fix & add columns
    clean = pipeline.clean.clean(mapped, tomorrow_str)
    log.df_len('clean', clean)
    time_check(startTime_1, 'clean')

    # reskill inventory
    skilled = pipeline.skills.complex_skills(clean)
    log.df_len('skilled', skilled)
    skilled['PhoneNumber'] = skilled['PhoneNumber'].astype(str)
    time_check(startTime_1, 'skill')

    # score inventory per skill
    scored = pipeline.score.split_drop_score(skilled)
    log.df_len('scored', scored)
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')
    
    # get column name & types ~ collect unique phone script
    column_types = scored.dtypes.reset_index()
    uniq_num_count = count_phone(scored)

    def Save():
        # save files
        zipfiles('push',scored, tomorrow_str)
        tables('push',  uniq_num_count,     'unique_phone_count.csv')
        tables('push',  column_types,       'columns.csv')
        time_check(startTime_1, 'Save files')
        # insert into server
        server.insert.batch_insert(servername, database, x_Bus_Day_ago(10).strftime(date_format), tomorrow_str, scored)
        time_check(startTime_1, 'batch_insert')

    # create campaign pivot
    daily_piv(scored)
    time_check(startTime_1, 'Create Pivot Table')

    if test == 'Pass': Save() 

if __name__ == "__main__":
    main()