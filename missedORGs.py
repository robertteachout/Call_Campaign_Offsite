from datetime import datetime
import pandas as pd
import pyodbc
import sys
import numpy as np
import time
from datetime import date, timedelta, datetime
import holidays
import matplotlib.pylab as plt

from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath, last_business_day
from dbo_Query import Query
today1 = date.today()
today = today1.strftime("%x")
ONE_DAY = timedelta(days=1)
HOLIDAYS_US = holidays.US()
startTime_1 = time.time()
def last_business_day(start):
    next_day = start - ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day -= ONE_DAY
    return next_day#.strftime("%x")
def lbd3(start):
    return last_business_day(last_business_day(last_business_day(start))).strftime("%x")
def pull_list():
    query = ('''
            SELECT
            c.[Skill]
            ,c.[OutreachID]
            ,c.[PhoneNumber]
            ,c.[Daily_Groups]
            ,n.[Start_Date] As NIC_Last_Call
            ,cf.[LastCallDateId] AS CF_Last_Call
            ,c.[Unique_Phone]

            FROM [DWWorking].[dbo].[Call_Campaign] AS c
            LEFT JOIN [DW_Operations].[dbo].[DimOutreach] AS cf
                ON c.OutreachID = cf.OutreachId
            LEFT JOIN (
                SELECT
                [Skill_Name],
                Contact_Name,
                MAX([Start_Date]) AS [Start_Date]
                FROM [DWWorking].[Prod].[nicagentdata]
                GROUP BY Skill_Name, Contact_Name
                ) AS n
                    ON c.[PhoneNumber] = n.[Contact_Name]
                    AND c.[Skill] = n.[Skill_Name]
            ''')
    
    df = Query('DWWorking', query)
    df['CF_Last_Call'] = pd.to_datetime(df['CF_Last_Call'].astype(str), format='%Y%m%d', errors='coerce')
    df['Daily_Groups'] = pd.to_datetime(df['Daily_Groups'], errors='coerce')
    df['NIC_Last_Call'] = pd.to_datetime(df['NIC_Last_Call'], errors='coerce')
    filter1 = (df['Daily_Groups'] == last_business_day(today1).strftime("%x"))
    filter2 = (df['NIC_Last_Call'].isnull())
    filter3 = (df['NIC_Last_Call'] <= lbd3(today1))
    filter4 = (df['Skill'] != 'CC_Genpact_Scheduling ')
    list_add = df[ filter1 & ( filter2 | filter3 ) & filter4]
    path = newPath('Table_Drop','')
    # old_list = pd.read_csv(path + 'Missed_ORGs' +  '.csv')
    # old_list['Daily_Groups'] = pd.to_datetime(old_list['Daily_Groups'])
    # # new_list = old_list[old_list['Daily_Groups'] != today]
    # new_list = old_list.append(list_add)
    # new_list.to_csv(path + 'Missed_ORGs' +  '.csv', index=False)
    list_add.to_csv(path + 'Missed_ORGs' +  '.csv', index=False)
    return list_add['OutreachID']

# df = pull_list()
# print(df)
# print(last_business_day(today1))
if __name__ == "__main__":

    executionTime_1 = (time.time() - startTime_1)
    print("-----------------------------------------------")
    print('Time: ' + str(executionTime_1))
    print("-----------------------------------------------")