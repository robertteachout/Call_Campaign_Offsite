from pathlib import Path
import os, sys
import pyarrow as pa
import pyarrow.csv as csv

paths = Path(__file__).parent.absolute().parent.absolute().parent.absolute()
from datetime import date
today = date.today()

from glob import glob
import pandas as pd
import os

table_path   = paths / "data/table_drop"
extract_path = paths / "data/extract"
load         = paths / "data/load"

### Input/output static tables ###
def tables(push_pull, table, name, path=table_path):
    if push_pull == 'pull':
        # return csv.read_csv(paths / path / name)
        return pd.read_csv(paths / path / name, sep=',',on_bad_lines='warn',engine="python",)
    else:
        table.to_csv(table_path / name, sep=',', index=False)

### push_pull zip file ###
def zipfiles(push_pull, table, filename, extract=extract_path):
    if push_pull == 'pull':
        Extract_path = list(extract.glob(filename))[0]
        return pd.read_csv(Extract_path, sep='|', on_bad_lines='warn',engine="python",quoting=3)
    else:
        compression_options = dict(method='zip', archive_name=f'{filename}.csv')
        table.to_csv(load / f'{filename}.zip', compression=compression_options, sep=',',index=False)

def count_phone(df):
    df0 = tables('pull', 'NA', 'unique_phone_count.csv')
    gb = df.groupby(['Load_Date'])['Unique_Phone'].count().reset_index()
    gb['Total'] = len(df)
    gb['%'] = round(gb['Unique_Phone'] / gb['Total'], 2)
    return df0.append(gb, ignore_index=True)

def append_column(df, location, index=list(),join='left'):
    # get orignal table or create one
    try:
        # set index so you don't need to remove it before add new df
        old = pd.read_csv(location, index_col=index)
    except:
        if input('create new table (y/n): ') == 'y':
            return df.to_csv(location)
        else:
            return print('check index input')
    # left join orignal with new
    new = pd.merge(old, df, how=join, left_index=True, right_index=True)
    new.to_csv(location)

if __name__ == "__main__":
    df= pd.DataFrame({'test':[1,2,3,4]})
    print(df)
    # zipfiles('push', df, 'test')
