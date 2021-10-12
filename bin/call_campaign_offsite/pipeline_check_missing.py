import pandas as pd
from etc_function import x_Bus_Day_ago
from dbo_org_search import cd_last_call
from data_config import table_drops


def pull_list():
    df = cd_last_call()
    df['Load_Date'] = pd.to_datetime(df['Load_Date']).dt.date
    df['CF_Last_Call'] = pd.to_datetime(df['CF_Last_Call']).dt.date
    df['Daily_Groups'] = pd.to_datetime(df['Daily_Groups']).dt.date
    df['NIC_Last_Call'] = pd.to_datetime(df['NIC_Last_Call']).dt.date
    filter0 = (df['Load_Date'] == x_Bus_Day_ago(1))
    filter1 = (df['Daily_Groups'] == x_Bus_Day_ago(1))#.strftime("%x"))
    filter2 = (df['NIC_Last_Call'].isnull())
    filter3 = (df['NIC_Last_Call'] < x_Bus_Day_ago(3))
    filter4 = (df['Skill'] != 'CC_Genpact_Scheduling')
    list_add = df[ filter0 & filter1 & ( filter2 | filter3 ) & filter4]    
    table_drops('push', list_add, 'Missed_ORGs.csv')
    return list_add['OutreachID']

