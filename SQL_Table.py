from datetime import datetime
import pandas as pd
import pyodbc
import sys
import numpy as np
import time
from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath

startTime_1 = time.time()
Dpath = newPath('Table_Drop','')
### Load file ###
df = pd.read_csv(Dpath + 'Group_Rank.csv', sep=',',low_memory=False)
### Clean ###
df = df[['OutreachID', 'PhoneNumber', 'Last_Call', 'Score', 'Skill', 'OutreachID_Count', 'Daily_Groups']]
df['Last_Call'] = df['Last_Call'].fillna(value= pd.to_datetime('10/10/2020'))
df[['OutreachID', 'Score', 'PhoneNumber', 'OutreachID_Count']] = df[['OutreachID', 'Score', 'PhoneNumber', 'OutreachID_Count']].astype(np.int64)
df[['Last_Call', 'Daily_Groups']] = df[['Last_Call', 'Daily_Groups']].astype('datetime64[ns]')
### Server Location ###
servername = 'HOME\SQLSERVER2019'
database = 'test_campaign_file'
# # servername = 'EUS1PCFSNAPDB01'
# # database = 'DWWorking'

DB ={
    'servername': servername,
    'database'  : database
    }

class MyDfInsert:
    def __init__(self, cnxn, sql_stub, data_frame, rows_per_batch=1000):
        # NB: hard limit is 1000 for SQL Server table value constructor
        self._rows_per_batch = 1000 if rows_per_batch > 1000 else rows_per_batch

        self._cnxn = cnxn
        self._sql_stub = sql_stub
        self._num_columns = None
        self._row_placeholders = None
        self._num_rows_previous = None
        self._all_placeholders = None
        self._sql = None

        row_count = 0
        param_list = list()
        for df_row in data_frame.itertuples():
            param_list.append(tuple(df_row[1:]))  # omit zero-based row index
            row_count += 1
            if row_count >= self._rows_per_batch:
                self._send_insert(param_list)  # send a full batch
                row_count = 0
                param_list = list()
        self._send_insert(param_list)  # send any remaining rows

    def _send_insert(self, param_list):
        if len(param_list) > 0:
            if self._num_columns is None:
                # print('[DEBUG] (building items that depend on the number of columns ...)')
                # this only happens once
                self._num_columns = len(param_list[0])
                self._row_placeholders = ','.join(['?' for x in range(self._num_columns)])
                # e.g. '?,?'
            num_rows = len(param_list)
            if num_rows != self._num_rows_previous:
                # print('[DEBUG] (building items that depend on the number of rows ...)')
                self._all_placeholders = '({})'.format('),('.join([self._row_placeholders for x in range(num_rows)]))
                # e.g. '(?,?),(?,?),(?,?)'
                self._sql = f'{self._sql_stub} VALUES {self._all_placeholders}'
                self._num_rows_previous = num_rows
            params = [int(element) if isinstance(element, np.int64) else element
                      for row_tup in param_list for element in row_tup]
            # print('[DEBUG]    sql: ' + repr(self._sql))
            # print('[DEBUG] params: ' + repr(params))
            crsr = self._cnxn.cursor()
            crsr.execute(self._sql, params)

if __name__ == '__main__':
    conn_str = (
        'DRIVER={SQL Server}; SERVER=' + DB['servername'] + '; DATABASE=' + DB['database'] + '; Trusted_Connection=yes'
    )
    cnxn = pyodbc.connect(conn_str, autocommit=True)
    crsr = cnxn.cursor()
    ### Create Table ###
    # crsr.execute("""
    #                 CREATE TABLE Campaign (
    #                 OutreachID int, PhoneNumber BIGINT, Last_Call date, 
    #                 Score int, Skill varchar(50), Daily_Groups date, OutreachID_Count int
    #                 )
    #                 """)

    t0 = time.time()
    ### Remove yesterday's file ###
    crsr.execute('''DELETE FROM dbo.Campaign''')
    ### Add today's file ###
    MyDfInsert(cnxn, """
                    INSERT INTO test_campaign_file.dbo.Campaign (
                        OutreachID, PhoneNumber, Last_Call, Score, Skill, OutreachID_Count, Daily_Groups) 
                    """, df, rows_per_batch=275)

    print()
    print(f'Inserts completed in {time.time() - t0:.2f} seconds.')

    cnxn.close()

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")