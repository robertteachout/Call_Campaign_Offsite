import pandas as pd
import sys, os
import pyodbc
import numpy as np
import time

from datetime import date
from pathlib import Path

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

def connection(servername):
    ### Server Location ###
    return  pyodbc.connect(f"""
                            DRIVER={{SQL Server}};
                            SERVER={servername}; 
                            DATABASE=DWWorking; 
                            Trusted_Connection=yes""", 
                            autocommit=True)

def clean_for_insert(load):
    load.rename({'parent':'Unique_Phone'}, axis=1, inplace=True)
    df = load[['OutreachID', 'PhoneNumber', 'Score', 'Skill','Unique_Phone','Load_Date','MasterSiteId']].copy()
    df['Daily_Groups'] = '2022-02-14'
    df['PhoneNumber'] = df['PhoneNumber'].astype(str).str.replace('\.0', '', regex=True).str[:10]

    col_fill = {'MasterSiteId':1000838, 'Unique_Phone':0}
    for col, fill in col_fill.items():
        df[col] = df[col].fillna(fill).apply(lambda x: int(x))

    int_cols    = ['OutreachID', 'Score', 'Unique_Phone']
    int_key     = dict.fromkeys(int_cols, np.int64)
    date_cols   = ['Daily_Groups', 'Load_Date']
    date_key    = dict.fromkeys(date_cols, 'datetime64[ns]')
    return df.astype(dict(int_key, **date_key))

def batch_insert(servername, campaign_history, load_date, load):
    cnxn = connection(servername)

    crsr = cnxn.cursor()
    ### Remove campaign_history's file ###
    remove=f'''
            DELETE
            FROM [DWWorking].[dbo].[Call_Campaign]
            WHERE Load_Date < '{campaign_history}'
            OR Load_Date = '{load_date}'
            '''
    ### Remove yesterday's file ###
    crsr.execute(remove)

    # pull whats in the server
    lookup = '''
            SELECT
                [Load_Date] 
                ,count([Load_Date]) AS Count
            FROM [DWWorking].[dbo].[Call_Campaign]
            GROUP BY [Load_Date]
            ORDER BY [Load_Date];
            '''
    print(pd.read_sql(lookup, cnxn))

    df = clean_for_insert(load)
    print(df)

    # ask to go forward with insert
    if input("Enter(y/n): ") == 'y':
        pass
    else:
        raise SystemExit
    

    ### Load file ###


    add = f"INSERT INTO [DWWorking].[dbo].[Call_Campaign] ({','.join([x for x in df.columns])})"

    t0 = time.time()
    ### Add today's file #
    MyDfInsert(cnxn, add, df, rows_per_batch=250)

    print()
    print(f'Inserts completed in {time.time() - t0:.2f} seconds.')
    cnxn.close()

if __name__ == "__main__":
    import secret
    from zipfile import ZipFile
    import pyarrow.csv as csv

    servername  = secret.servername
    file = Path(__file__).resolve()  
    package_root_directory = file.parents[1]  
    sys.path.append(str(package_root_directory))

    from pipeline.tables import tables
    from pipeline.etc import next_business_day, x_Bus_Day_ago
    date_format = '%Y-%m-%d'
    today = date.today()
    today_str = today.strftime(date_format)
    yesterday = x_Bus_Day_ago(1).strftime(date_format)
    tomorrow = next_business_day(today)
    tomorrow_str = tomorrow.strftime(date_format)
    extract = Path('data/load')
    file = extract / f'{today_str}.zip'
    # file = extract / f'{tomorrow_str}.zip'
    # file = f'{today_str}.zip'

    with ZipFile(file, 'r') as zips:
        zips.extractall(extract)
        file = extract / zips.namelist()[0]
        df = csv.read_csv(file).to_pandas()
        os.remove(file)

    # df = tables('pull','na', file, Path('data/load'))
    batch_insert(servername,
                x_Bus_Day_ago(10).strftime(date_format),
                tomorrow_str,
                df)