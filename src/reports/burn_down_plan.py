# first try == need refactor
projects = ['UHC HEDIS', 'HEDIS','ACA-PhysicianCR'] #'Chart Sync','HCR', 'ACA-PhysicianCR','ACA-HospitalCR','UHC HEDIS', 'HEDIS', 'Chart Review'
df = df0[df0.Project_Type.isin(projects)]
cf = df[(df.mastersiteID == 1000838) | (df.mastersiteID.isna())]
msid = df[(df.mastersiteID != 1000838) & (df.mastersiteID.notna())]

piv = msid.pivot_table(index='mastersiteID',columns='Project_Type',values='OutreachID',aggfunc='count')
# uhc_piv = piv[piv[project] == 1]
col = piv.columns.to_list()

solo = piv.copy()
solo['test'] = solo.values.tolist()
solo['test2'] = solo['test'].apply(lambda x: [i for i in x if pd.isna(i) == False])
solo['solo'] = solo['test2'].apply(lambda x: len(x) == 1)
add_col = []
for i in range(len(col)):
    add_col.append(i)
    solo[i] = solo['test'].apply(lambda x: pd.isna(x[i]) == False)

solo_ls = solo[solo.solo == True].index.tolist()

# print(piv[piv.index.isin(solo_ls)].sum())
solo['group'] = solo[add_col].values.tolist()
solo.pivot_table(index=['group','mastersiteID'], )
if __name__ == "__main__":
    projects = ['UHC HEDIS', 'HEDIS','ACA-PhysicianCR'] #'Chart Sync','HCR', 'ACA-PhysicianCR','ACA-HospitalCR','UHC HEDIS', 'HEDIS', 'Chart Review'
    cf, msid = sites(df0, projects)