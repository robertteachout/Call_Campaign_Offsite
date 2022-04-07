import os

import pandas as pd
import pyarrow as pa
import pyarrow.csv as csv

ls = [x for x in os.listdir("data/daily_priority") if x[0] == "2"]
final = pd.DataFrame()
# for index,i in enumerate(ls):
#     table = csv.read_csv(f'data/daily_priority/{i}')
#     df = table.to_pandas()
#     # orgs = set(df.OutreachID)
#     orgs = df[['Skill', 'OutreachID']].set_index('OutreachID').rename(columns={'Skill':f'Skill_{i[5:10]}'})
#     if index != 0:
#         # fin = sub.intersection(orgs)
#         fin = pd.merge(sub,orgs, how='inner', left_index=True, right_index=True).reset_index()
#         sub = orgs
#         piv = fin.groupby(f'Skill_{i[5:10]}')[f'Skill_{i[5:10]}'].count()
#         piv.index.rename('index',inplace=True)
#         if index > 1:
#             final = pd.merge(final, piv, how='left',left_index=True, right_index=True)
#         else:
#             final = piv
#     else:
#         sub = orgs
# print(final)
for index, i in enumerate(ls):
    print(i[:-4])
    table = pd.read_csv(f"data/load/{i[:-4]}.zip")
    df = table  # .to_pandas()
    # orgs = set(df.OutreachID)
    # orgs = df[['Skill', 'OutreachID']]#.set_index('OutreachID').rename(columns={'Skill':f'Skill_{i[5:10]}'})
    df = df[
        (df.Project_Type == "ACA-PhysicianCR")
        & (df.Last_Call.isna())
        & (df.Unique_Phone == 1)
    ].copy()
    piv = df.groupby("Skill").OutreachID.count().to_frame()
    piv = piv.rename(columns={"OutreachID": f"{i[5:10]}"})
    print(piv)
    if index != 0:
        final = pd.merge(final, piv, how="left", left_index=True, right_index=True)
    else:
        final = piv

print(final)
