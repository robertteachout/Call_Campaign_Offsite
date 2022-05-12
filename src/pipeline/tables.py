import re
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv

from .utils import Business_Days

paths = Path(__file__).parent.absolute().parent.absolute().parent.absolute()


CONFIG_PATH = paths / "src/config"

TABLE_PATH = paths / "data/table_drop"
LOAD_PATH = paths / "data/load"

Bus_day = Business_Days()

def asm_fall_out(df, file):
    with ZipFile(Path("data/extract") / file, "r") as zip:
        file_name = zip.namelist()[0]
        with zip.open(file_name) as raw_file:
            all_orgs = set([int(line[:8]) for line in raw_file if line[:8] != b'Outreach'])
    missing_orgs = list( all_orgs.difference(set(df.OutreachID)) )
    df_missing = pd.DataFrame(list(missing_orgs), columns=['OutreachID'])
    tables('push', df_missing, f'missing_orgs.csv')

def save_locally(scored, log_contact='y'):
    ### save file
    compressed_files(f"{Bus_day.tomorrow_str}.zip", table=scored)
    # compressed_files(f"{Bus_day.tomorrow_str}.csv.gz", table=scored)
    ### get column name & types ~ collect unique phone script
    tables("push", scored.dtypes.reset_index(), "columns.csv")
    ### insert into server ###
    if log_contact == 'y':
        contact_counts(scored)

def extract_file_name(test):
    extract = Path("data/extract")
    if test == "y":
        file_search = str(f'Call_Campaign_v4_{Bus_day.yesterday.strftime("%m%d")}*')
    else:
        file_search = str(f'Call_Campaign_v4_{Bus_day.today.strftime("%m%d")}*')

    file_match = list(extract.glob(file_search))[0]
    file_name = re.split(r"\\|\/",str(file_match))[-1]
    return extract, file_name


### Input/output static tables ###
def tables(push_pull, table, name, path=Path("data/table_drop")):
    if push_pull == "pull":
        # return csv.read_csv(paths / path / name)
        return pd.read_csv(paths / path / name, sep=",", on_bad_lines="warn", engine="python")
    else:
        table.to_csv(TABLE_PATH / name, sep=",", index=False)


def read_compressed(file_path, sep):
    match str(file_path).split(".")[-1]:
        case "zip":
            with ZipFile(file_path, "r") as zip:
                file_name = zip.namelist()[0]
                with zip.open(file_name, "r") as file:
                    return csv.read_csv(
                        file, parse_options=csv.ParseOptions(delimiter=sep, quote_char=False)
                    ).to_pandas()
        case "gz":
            return csv.read_csv(file_path).to_pandas()


def write_compressed(file_path, table):
    match str(file_path).split(".")[-1]:
        case "zip":
            filename = str(file_path).split("\\")[-1][:-4]
            compression_options = dict(method="zip", archive_name=f"{filename}.csv")
            table.to_csv(
                file_path, compression=compression_options, sep=",", index=False
            )
        case "gz":
            try:
                pa_table = pa.Table.from_pandas(table)
            except Exception as e:
                print(e)
            with pa.CompressedOutputStream(file_path, "gzip") as out:
                csv.write_csv(pa_table, out)


### push_pull zip file ###
def compressed_files(filename, path=Path(LOAD_PATH), table="read", sep=","):
    extract_path = path / filename
    if isinstance(table, str):
        try:
            return read_compressed(extract_path, sep)
        except:
            print("slow")
            return pd.read_csv(extract_path, sep=sep, engine="python", quoting=3)

    elif isinstance(table, pd.DataFrame):
        try:
            write_compressed(extract_path, table)
        except:
            print("slow")
            table.to_csv(extract_path, index=False)


def contact_counts(df):
    final = tables("pull", "NA", "monthly_average_contacts.csv")
    temp = pd.DataFrame()
    try:
        cf = df[(df.MasterSiteId == 1000838) | (df.MasterSiteId.isna())]
        msid = df[(df.MasterSiteId != 1000838) & (df.MasterSiteId.notna())]
        temp["date"] = df["Load_Date"].head(1)
        temp["ChartFinder"] = len(cf.PhoneNumber.unique())
        temp["MSID"] = len(msid.MasterSiteId.unique())
        temp["Total"] = temp.ChartFinder + temp.MSID
        temp.date = pd.to_datetime(temp.date)
        temp["months"] = temp.date.apply(lambda x: x.strftime("%m"))
        final = final.append(temp, ignore_index=True).drop_duplicates(subset="date")

        avgs = final.groupby("months")["Total"].mean().astype(int).to_dict()
        final["monthly_avg"] = final.months.map(avgs)
    except:
        print("contact_count doesnt work")
    tables("push", final, "monthly_average_contacts.csv")


def append_column(df, location, index=list(), join="left"):
    # get orignal table or create one
    try:
        # set index so you don't need to remove it before add new df
        old = pd.read_csv(location, index_col=index)
    except:
        if input("create new table (y/n): ") == "y":
            return df.to_csv(location)
        else:
            return print("check index input")
    # left join orignal with new
    new = pd.merge(old, df, how=join, left_index=True, right_index=True)
    new.to_csv(location)


if __name__ == "__main__":
    extract_file_name("y")
