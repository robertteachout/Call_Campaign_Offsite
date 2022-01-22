import pandas as pd
import numpy as np

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

    # df['status_sort'] = df['Outreach_Status'].map(name_sort)
    # df['rm_sort'] = df['Retrieval_Group'].map(rm_sort)
    df['age_sort'] = df['age_category'].map(age_sort)
    df['audit_sort'] = df['Audit_Type'].map(audit_sort)
    if not 'Daily_Priority' in df.columns:
        df['Daily_Priority'] = 0
    return df

def split(df, sk):
        df0 = df[df['Skill'] == sk].copy()
        # df1 = df0.join(pd.get_dummies(df0['Outreach_Status']))
        # df1['Unique_Phone'] = 0
        # d1 = dict.fromkeys(df0['Outreach_Status'].unique(), 'sum')
        # col = {**d1}
        ### Unique Numbers count and status
        # df2 = df1.groupby(['PhoneNumber']).agg(col).reset_index()
        ### Add info to main line and reskill
        # df3 = pd.merge(df0,df2, on='PhoneNumber')

        ### put Unscheduleed as parent
        # df3 = df3.sort_values(by= ['PhoneNumber', 'status_sort']).reset_index(drop = True)
        f1 = df0.audit_sort <=2
        df0['sla'] = np.where(f1, 5, 10)

        if not 'meet_sla' in df0.columns:
            f1 = df0.sla >= df0.age
            df0['meet_sla'] = np.where(f1, 1,0)

        ### re-rank after breaking it with status sort
        if not 'rolled' in df0.columns:
            df0['rolled'] = 0

        if not 'togo_bin' in df0.columns:
            bucket_amount = 5
            labels = list(reversed([x for x in range(bucket_amount)]))
            df0['togo_bin'] = pd.cut(df0.ToGoCharts, bins=bucket_amount, labels=labels)
            df0.togo_bin = df0.togo_bin.astype(int)

        scored = df0.sort_values(
                            by = ['rolled','meet_sla','togo_bin', 'age'], 
                            ascending=[False, True,True, False]
                            ).reset_index(drop = True)

        non_dup = scored.drop_duplicates(['PhoneNumber']).reset_index(drop = True)

        df_skill = non_dup.sort_values(
                            by = ['rolled','meet_sla','togo_bin','age'], 
                            ascending=[False, True,True, False]
                            ).reset_index(drop = True)
        
        ### re-rank skill according to meeting sla requirments and tilt towards togo charts
        # f1 = (df4.audit_sort <= 2) & (df4.age_category <= 5)# & (df4.age_category != 0)
        # f2 = (df4.audit_sort > 2) & (df4.age_category <= 10)# & (df4.age_category != 0)
        # df4['meets_sla'] = np.where(f1 | f2, 1, 0)
        
        # if sk == 'Tier1' or 'Tier2':
        #     df4 = df4.sort_values(by =['meets_sla','ToGoCharts'], ascending=[True,False]).reset_index(drop=True)

        df_skill['Unique_Phone'] = 1
        ### add score column
        df_skill['Score'] = range(len(df_skill)) #[x for x , i in enumerate(df_skill.OutreachID)]
        # Add Unique ORGs to Rank list 
        df5 = df_skill.append(scored)
        df6 = df5.drop_duplicates(['OutreachID']).reset_index(drop= True)
        ### Piped ORGs attached to phone numbers
        df6['OutreachID'] = df6['OutreachID'].astype(str)
        # df6['Matchees'] = df6.groupby(['PhoneNumber'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
        return df6
        
def split_drop_score(df):
    df3 = Number_stats(df).fillna(0)
    df_score_split = pd.DataFrame()
    ### Sort Order and drop Dups
    for i in df3['Skill'].unique():
        df_score_split = df_score_split.append(split(df3, i))
    return df_score_split