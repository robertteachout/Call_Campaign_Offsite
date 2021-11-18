from pathlib import Path
from glob import glob
import pandas as pd
import os
from zipfile import ZipFile
import zipfile
import shutil

### Input/output static tables ###
def tables(push_pull, table, name):
    table_path = Path("data/table_drop/")
    if push_pull == 'push':
        table.to_csv(table_path / name, sep=',',index=False)
    else:
        return pd.read_csv(table_path / name, sep=',',low_memory=False)

### push_pull zip file ###
def zipfiles(push_pull, df, filename):
    if push_pull == 'pull':
        path = Path("data/extract")
        Extract_path = list(path.glob(filename))[0]
        return pd.read_csv(Extract_path, sep='|', on_bad_lines='warn',engine="python",quoting=3)
    else:
        compression_options = dict(method='zip', archive_name=f'{filename}.csv')
        p = Path("data/load")
        df.to_csv(p / f'{filename}.zip', compression=compression_options, sep=',',index=False)

def count_phone(df):
    df0 = tables('pull', 'NA', 'unique_phone_count.csv')
    gb = df.groupby(['Load_Date'])['Unique_Phone'].count().reset_index()
    gb['Total'] = len(df)
    gb['%'] = round(gb['Unique_Phone'] / gb['Total'], 2)
    return df0.append(gb, ignore_index=True)



