from datetime import datetime
import pandas as pd
import pyodbc
import sys
import numpy as np
import time
from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath

startTime_1 = time.time()

### Server Location ###
servername = 'HOME\SQLSERVER2019'
database = 'test_campaign_file'
# # servername = 'EUS1PCFSNAPDB01'
# # database = 'DWWorking'

DB ={
    'servername': servername,
    'database'  : database
    }


if __name__ == '__main__':
    conn_str = (
        'DRIVER={SQL Server}; SERVER=' + DB['servername'] + '; DATABASE=' + DB['database'] + '; Trusted_Connection=yes'
    )
    cnxn = pyodbc.connect(conn_str, autocommit=True)
    crsr = cnxn.cursor()

    crsr.execute('''
        SELECT
        C.OutreachID,
        C.PhoneNumber,
        C.Score,
        C.Skill,
        C.Unique_Phone,
        C.Daily_Groups
        N.DateOfCall As NIC_Last_Call,
        CF.[Last Call] AS CF_Last_Call,
        FROM dbo.Campaign AS C
        LEFT JOIN dbo.api_nic_listinventory AS N
            ON C.PhoneNumber = N.PhoneNumber
        LEFT JOIN dbo.ChartFinder ????
            ON C.PhoneNumber = N.PhoneNumber

        ''')
