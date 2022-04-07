from glob import glob
from pathlib import Path

import pandas as pd

from src.pipeline.etc import *
from src.pipeline.tables import tables
from src.server.query import query

yesterday = x_Bus_Day_ago(1)


def nic(start, end):
    sql = f"""
        SELECT 
        Contact_Name,
        [Start_Date]
        FROM [DWWorking].[Prod].[nicagentdata]
        WHERE [Start_Date] >= '{start}'
        AND [Start_Date] <= '{end}' """
    df = query("DWWorking", sql, "")
    return df


target = pd.DataFrame()


def new_func(batch):
    z = Path(f"data/batch/batch_{batch}")
    b = list(z.glob("*.zip"))
    if not b:
        print("Check Path, list empty")
    dub = pd.DataFrame()
    dt = list()
    for i in b:
        df = tables("pull", "na", i, "")
        df["Daily_Groups"] = pd.to_datetime(df["Daily_Groups"]).dt.date
        df = df[df["Daily_Groups"] <= yesterday]
        df["PhoneNumber"] = pd.to_numeric(
            df["PhoneNumber"], downcast="integer"
        )  # .astype(str).str[:-2]
        col = df[["PhoneNumber"]]
        col_clean = col.drop_duplicates(
            subset=["PhoneNumber"], keep="last"
        ).reset_index(drop=True)
        try:
            dub = dub.append(col_clean)
        except:
            dub = col_clean
        st = str(i).split("\\")[-1][:-4]
        print(st)
        dt.append(st)
    return z, dub, dt


batch = 14

z, dub, dt = new_func(batch)

col1 = "Total Delivered #s"
col2 = "Total Unique #s"
col3 = "Unique Delivered #s"
col4 = "Missed #s"
col5 = "% Unique #s Dialed"

s_dt = f"{dt[0]}"
e_dt = f"{dt[-1]}"

clean_dub = dub.drop_duplicates(subset=["PhoneNumber"], keep="last").reset_index(
    drop=True
)

n = nic(s_dt, e_dt)
n = n.rename(columns={"Contact_Name": "PhoneNumber"}).astype(str)
n["count"] = col1

clean_dub["PhoneNumber"] = clean_dub["PhoneNumber"].astype(str)
join = (
    pd.merge(clean_dub, n, how="left", on=["PhoneNumber"])
    .fillna(col4)
    .rename(columns={"PhoneNumber": s_dt})
)

Unique_Delivery = join[join["count"] == col1]

clean_uni = Unique_Delivery.drop_duplicates(
    subset=[f"{s_dt}"], keep="last"
).reset_index(drop=True)

piv = join.pivot_table(index=["count"], values=[f"{s_dt}"], aggfunc=["count"])
piv.columns = piv.columns.droplevel(0)
piv.loc[col2] = len(clean_dub)
piv.loc[col3] = len(clean_uni)
piv.loc[col5] = round(piv.loc[col3] / piv.loc[col2], 2)

z = Path(f"data/batch/obeya")

df = tables("pull", "na", "batch_final.csv", z)
final = pd.merge(df, piv, how="left", on=["count"])
final.to_csv(z / f"batch_final.csv")
