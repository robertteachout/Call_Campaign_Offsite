
def grouping(df, match):
    piv = df.pivot_table(index=match 
                        ,columns='Project_Type'
                        ,values='OutreachID'
                        ,aggfunc='count')

    col = piv.columns.to_list()

    t2 = piv.reset_index(drop=False) \
            .melt(id_vars=[match], value_vars=col) \
            .dropna(subset=['value'])
            
    t2['groups'] = t2.groupby(match)['Project_Type'].transform(lambda x: '|'.join(x))
    return t2.groupby('groups').value.sum()

def sites(df0, projects):
    df = df0[df0.Project_Type.isin(projects)]

    cf = df[(df.MasterSiteId == 1000838) | (df.MasterSiteId.isna())]
    msid = df[(df.MasterSiteId != 1000838) & (df.MasterSiteId.notna())]

    t1 = grouping(cf, 'PhoneNumber')
    t2 = grouping(msid, 'MasterSiteId')
    return t1, t2

if __name__ == "__main__":
    projects = ['UHC HEDIS', 'HEDIS','ACA-PhysicianCR'] #'Chart Sync','HCR', 'ACA-PhysicianCR','ACA-HospitalCR','UHC HEDIS', 'HEDIS', 'Chart Review'
    cf, msid = sites(df0, projects)