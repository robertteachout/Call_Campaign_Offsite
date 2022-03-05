from pathlib import Path
import os, sys
# import pyarrow as pa
import pyarrow.csv as csv
from zipfile import ZipFile


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
        return pd.read_csv(paths / path / name, sep=',', on_bad_lines='warn', low_memory=False)#, engine="python",)
    else:
        table.to_csv(table_path / name, sep=',', index=False)

### push_pull zip file ###
def zipfiles(push_pull, table, filename, extract=extract_path):
    if push_pull == 'pull':
        Extract_path = list(extract.glob(filename))[0]
        # if re.search("copy", Extract_path): raise SystemExit
        with ZipFile(Extract_path, 'r') as zip:
                zip.extractall(Path(extract))
                file = extract / zip.namelist()[0]
                try:
                    df = csv.read_csv(file, parse_options=csv.ParseOptions(delimiter='|'))
                    os.remove(file)
                except:
                    os.remove(file)

        return df.to_pandas()
    else:
        compression_options = dict(method='zip', archive_name=f'{filename}.csv')
        table.to_csv(load / f'{filename}.zip', compression=compression_options, sep=',',index=False)

def contact_counts(df):
    final = tables('pull', 'NA', 'monthly_average_contacts.csv')
    temp = pd.DataFrame()
    try:
        cf = df[(df.MasterSiteId == 1000838) | (df.MasterSiteId.isna())]
        msid = df[(df.MasterSiteId != 1000838) & (df.MasterSiteId.notna())]
        temp['date'] = df['Load_Date'].head(1)
        temp['ChartFinder'] = len(cf.PhoneNumber.unique())
        temp['MSID'] = len(msid.MasterSiteId.unique())
        temp['Total'] = temp.ChartFinder + temp.MSID
        temp.date = pd.to_datetime(temp.date)
        temp['months'] = temp.date.apply(lambda x:x.strftime('%m'))
        final = final.append(temp, ignore_index=True)

        avgs = final.groupby('months')['Total'].mean().astype(int).to_dict()
        final['monthly_avg'] = final.months.map(avgs)
    except:
        print('contact_count doesnt work')
    tables('push', final, 'monthly_average_contacts.csv')

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
