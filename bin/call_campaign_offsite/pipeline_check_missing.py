import pandas as pd
from etc_function import x_Bus_Day_ago
import dbo_query
from data_config import tables


def pull_list():
    df = dbo_query.lc_org_search()
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
    return list_add

if __name__ == "__main__":
    print(pull_list())

