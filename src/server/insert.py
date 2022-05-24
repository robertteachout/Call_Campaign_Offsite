import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy

from .queries.call_campaign_insert import sql as call_campaign_insert_sql


def server_insert(scored, table_name, sql_engine, remove_date):
    load = clean_for_insert(scored)
    load_date = "".join(scored.Load_Date.unique())
    remove, lookup = call_campaign_insert_sql(
        remove_date, load_date
    )
    before_insert(sql_engine, remove, lookup)
    sql_insert(load, sql_engine, table_name)

def clean_for_insert(load):
    load.rename({"parent": "Unique_Phone"}, axis=1, inplace=True)
    df = load[
        [
            "OutreachID",
            "PhoneNumber",
            "Score",
            "Skill",
            "Unique_Phone",
            "Load_Date",
            "MasterSiteId",
        ]
    ].copy()
    df["Daily_Groups"] = "2022-02-14"
    df["PhoneNumber"] = (
        df["PhoneNumber"].astype(str).str.replace("\.0", "", regex=True).str[:10]
    )

    col_fill = {"MasterSiteId": 1000838, "Unique_Phone": 0}
    for col, fill in col_fill.items():
        df[col] = df[col].fillna(fill).apply(lambda x: int(x))

    int_cols = ["OutreachID", "Score", "Unique_Phone"]
    int_key = dict.fromkeys(int_cols, np.int64)
    date_cols = ["Daily_Groups", "Load_Date"]
    date_key = dict.fromkeys(date_cols, "datetime64[ns]")
    return df.astype(dict(int_key, **date_key))


def before_insert(engine: sqlalchemy.engine, remove, lookup) -> None:
    engine.execute(remove)
    print(pd.read_sql(lookup, engine))


def sql_insert(load, engine: sqlalchemy.engine, table):
    # ask to go forward with insert
    if input("Enter(y/n): ") == "y":
        pass
    else:
        raise SystemExit
    ### Load file ###
    t0 = time.time()
    ### Add today's file #
    load.to_sql(table, engine, index=False, if_exists="append", schema='dbo')
    print(f"Inserts completed in {time.time() - t0:.2f} seconds.")


if __name__ == "__main__":
    file = Path(__file__).resolve()
    package_root_directory = file.parents[1]
    sys.path.append(str(package_root_directory))
    from pipeline.utils import Business_Days, x_Bus_Day_ago

    from server.connections import MSSQL
    from server.queries.call_campaign_insert import sql

    buzday = Business_Days()

    day = buzday.tomorrow_str

    extract = Path("data/load")
    file = extract / f"{day}.zip"
    df = pd.read_csv(file)

    server = "EUS1PCFSNAPDB01"
    database = "DWWorking"
    table = "Call_Campaign"

    dwworking = MSSQL(server, database)
    dw_engine = dwworking.create_engine()

    load = clean_for_insert(df)
    load_date = "".join(df.Load_Date.unique())
    remove, lookup = sql(x_Bus_Day_ago(10), load_date)
    before_insert(dw_engine, remove, lookup)
    sql_insert(load, dw_engine, table)
