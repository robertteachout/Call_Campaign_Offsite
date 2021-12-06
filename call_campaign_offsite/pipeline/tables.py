from pathlib import Path
import os, sys

paths = Path(__file__).parent.absolute().parent.absolute().parent.absolute()
from datetime import date
today = date.today()

from glob import glob
import pandas as pd
import os

table_path  = paths / "data/table_drop"
extract     = paths / "data/extract"
load        = paths / "data/load"

### Input/output static tables ###
def tables(push_pull, table, name, path=table_path):
    if push_pull == 'pull':
        return pd.read_csv(paths / path / name, sep=',',on_bad_lines='warn',engine="python",)
    else:
        table.to_csv(table_path / name, sep=',', index=False)

### push_pull zip file ###
def zipfiles(push_pull, table, filename):
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

if __name__ == "__main__":
    df= pd.DataFrame({'test':[1,2,3,4]})
    print(df)
    zipfiles('push', df, 'test')
