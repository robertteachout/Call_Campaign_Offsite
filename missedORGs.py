from datetime import datetime
import pandas as pd
import pyodbc
import sys
import numpy as np
import time
from datetime import date, timedelta, datetime
today1 = date.today()
import holidays

from Bus_day_calc import next_business_day, x_Bus_Day_ago, Next_N_BD, map_piv, daily_piv, newPath, last_business_day
from dbo_Query import Query
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
        	,CAST (od.lastcalldate AS DATE) as 'CF_Last_Call'
            ,c.[Unique_Phone]
            ,c.[Load_Date]

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
            LEFT JOIN Chartfinder_Snap.dbo.OutreachDates as od
	            ON c.OutreachID = od.OutreachID
            WHERE c.[Daily_Groups] = CAST(DATEADD(day, -1, GETDATE()) AS DATE)
            ''')
    
    df = Query('DWWorking', query, 'Last Call Check')
    df['Load_Date'] = pd.to_datetime(df['Load_Date'])
    df['CF_Last_Call'] = pd.to_datetime(df['CF_Last_Call'])
    df['Daily_Groups'] = pd.to_datetime(df['Daily_Groups'])
    df['NIC_Last_Call'] = pd.to_datetime(df['NIC_Last_Call'])
    filter0 = (df['Load_Date'] == x_Bus_Day_ago(1).strftime("%x"))
    filter1 = (df['Daily_Groups'] == last_business_day(today1).strftime("%x"))
    filter2 = (df['NIC_Last_Call'].isnull())
    filter3 = (df['NIC_Last_Call'] < lbd3(today1))
    filter4 = (df['Skill'] != 'CC_Genpact_Scheduling')
    list_add = df[ filter0 & filter1 & ( filter2 | filter3 ) & filter4]
    path = newPath('Table_Drop','')
    # old_list = pd.read_csv(path + 'Missed_ORGs' +  '.csv')
    # old_list['Daily_Groups'] = pd.to_datetime(old_list['Daily_Groups'])
    # old_list['NIC_Last_Call'] = pd.to_datetime(old_list['NIC_Last_Call'])
    # old_list['CF_Last_Call'] = pd.to_datetime(old_list['CF_Last_Call'])
    # new_list = old_list.append(list_add)
    list_add.to_csv(path + 'Missed_ORGs' +  '.csv', index=False)
    # list_add.to_csv(path + 'Missed_ORGs' +  '.csv', index=False)
    return list_add['OutreachID']

# df = pull_list()
# print(df)
if __name__ == "__main__":

    executionTime_1 = (time.time() - startTime_1)
    print("-----------------------------------------------")
    print('Time: ' + str(executionTime_1))
    print("-----------------------------------------------")