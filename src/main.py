import pandas as pd
import numpy as np

import pipeline.clean
import pipeline.score
import pipeline.sprint_schedule
import pipeline.skills
from pipeline.etc import daily_piv, time_check, x_Bus_Day_ago, Business_Days
from pipeline.tables import tables, zipfiles, contact_counts

import server.connections, server.insert, server.secret
import server.queries.MasterSiteId
import server.queries.reschedule
import server.queries.optum_assigned
import server.queries.call_campaign_insert
from server.insert import clean_for_insert, before_insert, sql_insert

import log.log as log

servername  = server.secret.servername
database    = server.secret.database

date_format = '%Y-%m-%d'
BusinessDay = Business_Days

def main(test='n', msid='n', sample='n'):
    server_name = 'EUS1PCFSNAPDB01'
    database    = 'DWWorking'
    table       = 'Call_Campaign'
    dwworking   = server.connections.MSSQL(server_name, database)
    dw_engine   = dwworking.create_engine()

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
    # reSchedule = pd.read_sql(reschedule_sql, dw_engine)
    # log.df_len('reSchedule', reSchedule)

    # df_full0 = tested.append(reSchedule, ignore_index = True)
    # log.df_len('df_full', df_full0)

    ### Master Site ID
    if msid == 'y':
        mastersite_sql = server.queries.MasterSiteId.sql()
        mastersite = pd.read_sql(mastersite_sql, dw_engine)
        tables('push',  mastersite,     'mastersite.csv')

    mastersite = tables('pull',  'na',     'mastersite.csv')
    mapped = pd.merge(tested, mastersite, how='left', on='OutreachID')
    log.df_len('MasterSiteId', mapped)
    time_check(BusinessDay.now, 'msid map')

    ### fix & add columns
    clean = pipeline.clean.clean(mapped, BusinessDay.tomorrow_str)
    log.df_len('clean', clean)
    time_check(BusinessDay.now, 'clean')

    # optum assigned inventory
    # optum_assigned_sql = server.queries.optum_assigned.sql()
    # optum_assigned =  pd.read_sql(optum_assigned_sql, dw_engine)
    # log.df_len('optum_assigned', optum_assigned)
    # time_check(BusinessDay.now, 'optum_assigned')
    # # add filter with above query
    # org_list = optum_assigned.OutreachID.to_list()
    # f1 = clean.OutreachID.isin(org_list)
    # clean['optum_assigned'] = np.where(f1, 1, 0)

    # dft = pd.read_csv('data/table_drop/table.csv')
    # f1 = clean.OutreachID.isin(dft.OutreachID.to_list())
    # # f2 = clean.Retrieval_Team  == 'Genpact Offshore'
    # clean['temp_jen'] = np.where(f1, 1, 0)
    
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
        time_check(BusinessDay.now, 'Save files')
        ### insert into server ###
        load = clean_for_insert(scored)
        load_date = ''.join(scored.Load_Date.unique())
        remove, lookup = server.queries.call_campaign_insert.sql(x_Bus_Day_ago(10), load_date)
        before_insert(dw_engine, remove, lookup)
        sql_insert(load, dw_engine, table)

        contact_counts(scored)
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