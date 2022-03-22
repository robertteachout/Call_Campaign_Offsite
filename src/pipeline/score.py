import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class business_lines:
    projects: list
    capacity: int
    system:str
    skill:str

def rank(df=pd.DataFrame, new_col=str, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns, ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def project_rank(df, buz_line, rank):
    f1 = df.Project_Type.isin(buz_line.projects) # 'ACA-PhysicianCR'
    df[f'temp_rank{rank}'] = np.where(f1, 1, 0)
    return df

def stack_inventory(df, grouping):
    uhc     = business_lines(['UHC HEDIS'],3650,'CC_ChartFinder','CC_Adhoc3')
    hedis   = business_lines(['HEDIS'], 2450,'CC_ChartFinder','CC_Adhoc4')
    aca     = business_lines(['ACA-PhysicianCR'], 3550,'CC_ChartFinder','CC_Adhoc5')
    medicaid= business_lines(['Chart Review','Clinical Review MCaid PhyCR'], 2500,'CC_ChartFinder','CC_Adhoc6')
    radv    = business_lines(['RADV'], 2500,'CC_ChartFinder','CC_Adhoc2')
    aetna    = business_lines(['Aetna Commercial','Aetna Medicare'], 2500,'CC_Genpact_Scheduling','CC_Genpact_Scheduling')
    # commerical_ls = ['Centene ACA','Centene HEDIS','Oscar','WellCare HEDIS','Highmark ACA','Advantasure ACA','Anthem ACA','HealthSpring HEDIS','OptimaHealth HEDIS','Med Mutual of Ohio HEDIS','Inovalon','BCBS TN ACA','Cigna HEDIS','Anthem Hedis','Anthem Comm HEDIS','Devoted Health HEDIS','Aetna HEDIS','Advantasure_HEDIS_WA','IBX Hedis','Molina HEDIS Region 6-FL-SC','Aetna MEDICAID HEDIS','Inovalon Hedis','Advantmed HEDIS','Excellus CRA','Oscar HF ACA','Priority Health ACA','Molina HEDIS Region 5-IL-MI-WI','Advantasure_HEDIS_NE','Aetna Commercial','Gateway HEDIS','Molina HEDIS Region 4-NY-OH','Advantasure_HEDIS_VT','Arizona BlueCross BlueShield','Med Mutual of Ohio ACA','Highmark NY ACA','Molina HEDIS Region 3-MS-NM-TX','Advantasure_HEDIS_ND','Advantasure_HEDIS_OOA_Anthem','Optima Health Commercial','Highmark HEDIS','Centauri','BCBSTN HEDIS','Molina HEDIS Supplemental Region 5- IL-MI-WI','Molina HEDIS Region 2-ID-UT-WA','Premera','Humana HEDIS','Molina HEDIS Supplemental Region 4-NY-OH','Molina HEDIS Supplemental Region 6-FL-SC','ABCBS','Molina HEDIS Region 1-CA','Reveleer HEDIS','Centene HEDIS-WI','Molina HEDIS Supplemental Region 3-MS-NM-TX','Change Healthcare','Alliant Health Plans HEDIS','BCBS TN HEDIS OOA','Humana']
    commerical_ls = ['Centene ACA', 'Centene HEDIS', 'WellCare HEDIS', 'Highmark ACA', 'Advantasure ACA', 'Anthem ACA', 'HealthSpring HEDIS', 'OptimaHealth HEDIS', 'Med Mutual of Ohio HEDIS', 'BCBS TN ACA', 'Cigna HEDIS', 'Anthem Comm HEDIS', 'Devoted Health HEDIS', 'Aetna HEDIS', 'Advantasure_HEDIS_WA', 'Molina HEDIS Region 6-FL-SC', 'Aetna MEDICAID HEDIS', 'Advantmed HEDIS', 'Oscar HF ACA', 'Priority Health ACA', 'Molina HEDIS Region 5-IL-MI-WI', 'Advantasure_HEDIS_NE', 'Gateway HEDIS', 'Molina HEDIS Region 4-NY-OH', 'Advantasure_HEDIS_VT', 'Med Mutual of Ohio ACA', 'Highmark NY ACA', 'Molina HEDIS Region 3-MS-NM-TX', 'Advantasure_HEDIS_ND', 'Advantasure_HEDIS_OOA_Anthem', 'Highmark HEDIS', 'BCBSTN HEDIS', 'Molina HEDIS Supplemental Region 5- IL-MI-WI', 'Molina HEDIS Region 2-ID-UT-WA', 'Humana HEDIS', 'Molina HEDIS Supplemental Region 4-NY-OH', 'Molina HEDIS Supplemental Region 6-FL-SC', 'Molina HEDIS Region 1-CA', 'Reveleer HEDIS', 'Centene HEDIS-WI', 'Molina HEDIS Supplemental Region 3-MS-NM-TX', 'Alliant Health Plans HEDIS', 'BCBS TN HEDIS OOA']    
    commerical = business_lines(commerical_ls, 2500,'CC_ChartFinder','CC_ChartFinder')
    business = [uhc, hedis, aca, medicaid, radv, commerical, aetna]

    temp = {}
    for index, buz_line in enumerate(business):
        # create column on dataframe
        df = project_rank(df, buz_line, index)
        # create dict for scoring
        temp[f'temp_rank{index}'] = False

    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False,'age':False, 'togo_bin':False} 
    # group by phone number or msid & rank highest value org
    df = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    df = rank(df,'Score', ['Skill','parent'], rank_cols)
    
    if grouping == 'PhoneNumber':
        for buz_line in business:
            if buz_line.system == 'CC_ChartFinder':
                f0 = df.Skill == buz_line.system
                f1 = df.Project_Type.isin(buz_line.projects)
                df.Skill = np.where(f0 & f1, buz_line.skill, df.Skill)

    rank_final = {'meet_target_sla':True, 'timezone_sort':True, 'no_call':False, **temp, 'age_sort':True,'togo_bin':False} 
    df = rank(df,'Score', ['Skill','parent'], rank_final)
    df['Matchees'] = df.groupby(['Skill', grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    df['OutreachID'] = df['OutreachID'].astype(str)
   
    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid, 'PhoneNumber')
    msid_scored = stack_inventory(msid, 'MasterSiteId')

    dubs = scored.append(msid_scored)
    unique = dubs.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    unique['Score'] = np.where(f0, 1000000, unique.Score)
    return unique
        
def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)