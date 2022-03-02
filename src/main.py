from datetime import date
import pandas as pd
import time

import pipeline.clean
import pipeline.score
import pipeline.sprint_schedule
import pipeline.skills
from pipeline.etc import daily_piv, time_check, next_business_day, x_Bus_Day_ago
from pipeline.tables import tables, zipfiles, contact_counts

import server.query, server.insert, server.secret
import server.queries.MasterSiteId
import log.log as log

servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
today, startTime_1 = date.today(), time.time()
tomorrow_str = next_business_day(today).strftime(date_format)

def main(msid='yes', sample='no'):
    ### load & transform
    filename = str(f'Call_Campaign_v4_{today.strftime("%m%d")}*')
    load = zipfiles('pull', 'NA', filename)
    log.df_len('load', load)

    ### test last call date = today
    tested, test = pipeline.clean.check_load(load, today)
    log.df_len('tested', tested)
    time_check(startTime_1, f'File Load \t{test}')
    
    ### Master Site ID
    if msid == 'yes':
        mastersite_sql = server.queries.MasterSiteId.sql()
        mastersite = server.query.query(servername, database,  mastersite_sql, 'Add MasterSiteId')
        tables('push',  mastersite,     'mastersite.csv')

    mastersite = tables('pull',  'na',     'mastersite.csv')
    mapped = pd.merge(tested, mastersite, how='left', on='OutreachID')
    log.df_len('MasterSiteId', mapped)
    time_check(startTime_1, 'msid map')

    ### fix & add columns
    clean = pipeline.clean.clean(mapped, tomorrow_str)
    log.df_len('clean', clean)
    time_check(startTime_1, 'clean')

    ### reskill inventory
    skilled = pipeline.skills.complex_skills(clean)
    log.df_len('skilled', skilled)
    skilled['PhoneNumber'] = skilled['PhoneNumber'].astype(str)
    time_check(startTime_1, 'skill')

    ### score inventory per skill
    scored = pipeline.score.split(skilled)
    log.df_len('scored', scored)
    time_check(startTime_1, 'Split, Score, & Parent/Child Relationship')
    
    def Save():
        ### save file
        zipfiles('push', scored, tomorrow_str)
        ### get column name & types ~ collect unique phone script
        tables('push',  scored.dtypes.reset_index(), 'columns.csv')
        ### reporting
        contact_counts(scored)
        time_check(startTime_1, 'Save files')
        ### insert into server ###
        server.insert.batch_insert(servername, database, x_Bus_Day_ago(10).strftime(date_format), tomorrow_str, scored)
        time_check(startTime_1, 'batch_insert')

    ### create campaign pivot
    daily_piv(scored)
    time_check(startTime_1, 'Create Pivot Table')

    if sample == 'yes':
        zipfiles('push', scored, tomorrow_str)

    if test == 'Pass': Save() 

if __name__ == "__main__":
    main(msid='no', sample='no')