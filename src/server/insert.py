from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd
import sys, os
import numpy as np
import time
import sqlalchemy

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


class Server(ABC):
    @abstractmethod
    def create_engine():
        pass

@dataclass
class MSSQL:
    server  : str
    database: str
    driver  : str = "ODBC Driver 17 for SQL Server"
    engine  : sqlalchemy.engine = field(init=False)

    def __post_init__(self) -> None:
        self.engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://{self.server}/{self.database}?driver={self.driver}", fast_executemany=True)

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

def before_insert(server:MSSQL, remove, lookup) -> None:
    server.engine.execute(remove)
    print(pd.read_sql(lookup, dwworking.engine))

def sql_insert(load, server: MSSQL, table):
    # ask to go forward with insert
    if input("Enter(y/n): ") == 'y':
        pass
    else:
        raise SystemExit
    ### Load file ###
    t0 = time.time()
    ### Add today's file #
    load.to_sql(table, server.engine, index=False, if_exists="append")
    print(f'Inserts completed in {time.time() - t0:.2f} seconds.')

if __name__ == "__main__":
    file = Path(__file__).resolve()  
    package_root_directory = file.parents[1]  
    sys.path.append(str(package_root_directory))
    from pipeline.etc import Business_Days, x_Bus_Day_ago
    from server.queries.call_campaign_insert import sql
    buzday = Business_Days()
    
    day = buzday.tomorrow_str
    
    extract = Path('data/load')
    file = extract / f'{day}.zip'
    df = pd.read_csv(file)

    server      = 'EUS1PCFSNAPDB01'
    database    = 'DWWorking'
    table       = 'Call_Campaign'

    dwworking   = MSSQL(server, database)

    load = clean_for_insert(df)
    load_date = ''.join(df.Load_Date.unique())
    remove, lookup = sql(x_Bus_Day_ago(10), load_date)
    before_insert(dwworking, remove, lookup)
    sql_insert(load, dwworking, table)
