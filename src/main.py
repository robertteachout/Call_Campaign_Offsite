from datetime import date
import pandas as pd
import time

import pipeline.clean
import pipeline.score
import pipeline.sprint_schedule
import pipeline.skills
from pipeline.etc import daily_piv, time_check, x_Bus_Day_ago, dates
from pipeline.tables import tables, zipfiles, contact_counts

import server.query, server.insert, server.secret
import server.queries.MasterSiteId
import server.queries.reschedule
import log.log as log

servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
BusinessDay = dates(date_format)

def main(test='n', msid='n', sample='n'):
    ### load & transform
    if test == 'y':
        filename = str(f'Call_Campaign_v4_{BusinessDay.yesterday.strftime("%m%d")}*')
    else:
        filename = str(f'Call_Campaign_v4_{BusinessDay.today.strftime("%m%d")}*')

    load = zipfiles('pull', 'NA', filename)
    log.df_len('load', load)

    ### test last call date = today
    tested, test = pipeline.clean.check_load(load, BusinessDay.today)
    log.df_len('tested', tested)
    time_check(BusinessDay.now, f'File Load \t{test}')
    
    # ## Reschedules
    # reschedule_sql = server.queries.reschedule.sql()
    # reSchedule = server.query.query(servername, database,  reschedule_sql, 'Add reschedules')
    # log.df_len('reSchedule', reSchedule)

    # df_full0 = tested.append(reSchedule, ignore_index = True)
    # log.df_len('df_full', df_full0)

    ### Master Site ID
    if msid == 'y':
        mastersite_sql = server.queries.MasterSiteId.sql()
        mastersite = server.query.query(servername, database,  mastersite_sql, 'Add MasterSiteId')
        tables('push',  mastersite,     'mastersite.csv')

    mastersite = tables('pull',  'na',     'mastersite.csv')
    mapped = pd.merge(tested, mastersite, how='left', on='OutreachID')
    log.df_len('MasterSiteId', mapped)
    time_check(BusinessDay.now, 'msid map')

    ### fix & add columns
    clean = pipeline.clean.clean(mapped, BusinessDay.tomorrow_str)
    log.df_len('clean', clean)
    time_check(BusinessDay.now, 'clean')

    ### reskill inventory
    skilled = pipeline.skills.complex_skills(clean)
    log.df_len('skilled', skilled)
    skilled['PhoneNumber'] = skilled['PhoneNumber'].astype(str)
    time_check(BusinessDay.now, 'skill')

    ### score inventory per skill
    scored = pipeline.score.split(skilled)
    log.df_len('scored', scored)
    time_check(BusinessDay.now, 'Split, Score, & Parent/Child Relationship')
    
    def Save():
        ### save file
        zipfiles('push', scored, BusinessDay.tomorrow_str)
        ### get column name & types ~ collect unique phone script
        tables('push',  scored.dtypes.reset_index(), 'columns.csv')
        ### reporting
        contact_counts(scored)
        time_check(BusinessDay.now, 'Save files')
        ### insert into server ###
        server.insert.batch_insert(servername, x_Bus_Day_ago(10).strftime(date_format), BusinessDay.tomorrow_str, scored)
        time_check(BusinessDay.now, 'batch_insert')

    ### create campaign pivot
    daily_piv(scored)
    time_check(BusinessDay.now, 'Create Pivot Table')

    if sample == 'y':
        zipfiles('push', scored, BusinessDay.tomorrow_str)

    if test == 'Pass': Save() 

if __name__ == "__main__":
    def question(q):
        return input(f"\n{q}(y/n): ")

    if question('questions') == 'y':
        test=question('test')
        msid=question('msid')
        sample=question('save')
        main(test,msid,sample)
    else:
        main()