import pandas as pd

def Number_stats(df):
    audit_sort = {'RADV':0, 'Medicaid Risk':1, 'HEDIS':2, 'Specialty':3,  'ACA':4, 'Medicare Risk':5}
    name_sort = {'Unscheduled':0, 'Escalated':2, 'PNP Released':1,'Past Due':3,'Scheduled':4}
    rm_sort = {'EMR Remote': 0, 'HIH - Other': 2, 'Onsite':1,'Offsite':3}
    age_sort = {21: 0, 0: 1, 20:2, 15:3, 10:4, 5:5}

    # new = ['status_sort',       'rm_sort',          'age_sort',     'audit_sort']
    # old = ['Outreach_Status',   'Retrieval_Group',  'age_catergory','Audit_Type']
    # map = [name_sort,           rm_sort,            age_sort,       audit_sort]
    # for i, j, m in zip(new, old, map):
    #     df[f'{i}'] = df[f'{j}'].map(m)
    df0=df
    df0['status_sort'] = df0['Outreach_Status'].map(name_sort)
    df0['rm_sort'] = df0['Retrieval_Group'].map(rm_sort)
    df0['age_sort'] = df0['age_category'].map(age_sort)
    df0['audit_sort'] = df0['Audit_Type'].map(audit_sort)
    df3 = df0
    if not 'Daily_Priority' in df3.columns:
        df3['Daily_Priority'] = 0
    return df

def split(df, sk):
        df0 = df[df['Skill'] == sk]
        df1 = df0.join(pd.get_dummies(df0['Outreach_Status']))
        df1['Unique_Phone'] = 0
        d1 = dict.fromkeys(df0['Outreach_Status'].unique(), 'sum')
        col = {'TotalCharts':'sum','Cluster':'mean',**d1}
        ### Unique Numbers count and status
        df2 = df1.groupby(['PhoneNumber']).agg(col).rename(columns={'TotalCharts':'TotalChartsAgg','Cluster':'Cluster_Avg'}).reset_index()
        ### Add info to main line and reskill
        df3 = pd.merge(df0,df2, on='PhoneNumber')

        ### put Unscheduleed as parent
        df3 = df3.sort_values(by= ['PhoneNumber', 'status_sort']).reset_index(drop = True)
        df4 = df3.drop_duplicates(['PhoneNumber']).reset_index(drop = True)
        ### re-rank after breaking it with status sort
        if not 'rolled' in df4.columns:
            df4['rolled'] = 0
        df4 = df4.sort_values(by = ['Daily_Priority','rolled','audit_sort','age_sort'], ascending=[True, False, True, True]).reset_index(drop = True)
        df4['Unique_Phone'] = 1
        ### add score column
        df_skill = df4
        df_skill['Score'] = range(0,len(df_skill))
        # Add Unique ORGs to Rank list 
        df5 = df_skill.append(df3)
        df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
        ### Piped ORGs attached to phone numbers
        df6['OutreachID'] = df6['OutreachID'].astype(str)
        df6['Matches'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
        return df6
        
def drop_dup(df):
    df3 = Number_stats(df)
    df_score_split = pd.DataFrame()
    ### Sort Order and drop Dups
    for i in df3['Skill'].unique():
        df_score_split = df_score_split.append(split(df3, i))
    return df_score_split

def split_drop_score(df):
    return drop_dup(df)