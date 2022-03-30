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

def ciox_business_lines():
    uhc         = business_lines(['UHC HEDIS'],3650,'CC_ChartFinder','CC_Adhoc3')
    hedis       = business_lines(['HEDIS'], 2450,'CC_ChartFinder','CC_Adhoc4')
    aca         = business_lines(['ACA-PhysicianCR'], 3550,'CC_ChartFinder','CC_Adhoc5')
    medicaid    = business_lines(['Chart Review','Clinical Review MCaid PhyCR'], 2500,'CC_ChartFinder','CC_Adhoc6')
    radv        = business_lines(['RADV'], 2500,'CC_ChartFinder','CC_Adhoc2')
    aetna       = business_lines(['Aetna Commercial','Aetna Medicare'], 2500,'CC_Genpact_Scheduling','CC_Genpact_Scheduling')
    # commerical_ls = ['Centene ACA','Centene HEDIS','Oscar','WellCare HEDIS','Highmark ACA','Advantasure ACA','Anthem ACA','HealthSpring HEDIS','OptimaHealth HEDIS','Med Mutual of Ohio HEDIS','Inovalon','BCBS TN ACA','Cigna HEDIS','Anthem Hedis','Anthem Comm HEDIS','Devoted Health HEDIS','Aetna HEDIS','Advantasure_HEDIS_WA','IBX Hedis','Molina HEDIS Region 6-FL-SC','Aetna MEDICAID HEDIS','Inovalon Hedis','Advantmed HEDIS','Excellus CRA','Oscar HF ACA','Priority Health ACA','Molina HEDIS Region 5-IL-MI-WI','Advantasure_HEDIS_NE','Aetna Commercial','Gateway HEDIS','Molina HEDIS Region 4-NY-OH','Advantasure_HEDIS_VT','Arizona BlueCross BlueShield','Med Mutual of Ohio ACA','Highmark NY ACA','Molina HEDIS Region 3-MS-NM-TX','Advantasure_HEDIS_ND','Advantasure_HEDIS_OOA_Anthem','Optima Health Commercial','Highmark HEDIS','Centauri','BCBSTN HEDIS','Molina HEDIS Supplemental Region 5- IL-MI-WI','Molina HEDIS Region 2-ID-UT-WA','Premera','Humana HEDIS','Molina HEDIS Supplemental Region 4-NY-OH','Molina HEDIS Supplemental Region 6-FL-SC','ABCBS','Molina HEDIS Region 1-CA','Reveleer HEDIS','Centene HEDIS-WI','Molina HEDIS Supplemental Region 3-MS-NM-TX','Change Healthcare','Alliant Health Plans HEDIS','BCBS TN HEDIS OOA','Humana']
    # commerical_ls = ['Centene ACA', 'Centene HEDIS', 'WellCare HEDIS', 'Highmark ACA', 'Advantasure ACA', 'Anthem ACA', 'HealthSpring HEDIS', 'OptimaHealth HEDIS', 'Med Mutual of Ohio HEDIS', 'BCBS TN ACA', 'Cigna HEDIS', 'Anthem Comm HEDIS', 'Devoted Health HEDIS', 'Aetna HEDIS', 'Advantasure_HEDIS_WA', 'Molina HEDIS Region 6-FL-SC', 'Aetna MEDICAID HEDIS', 'Advantmed HEDIS', 'Oscar HF ACA', 'Priority Health ACA', 'Molina HEDIS Region 5-IL-MI-WI', 'Advantasure_HEDIS_NE', 'Gateway HEDIS', 'Molina HEDIS Region 4-NY-OH', 'Advantasure_HEDIS_VT', 'Med Mutual of Ohio ACA', 'Highmark NY ACA', 'Molina HEDIS Region 3-MS-NM-TX', 'Advantasure_HEDIS_ND', 'Advantasure_HEDIS_OOA_Anthem', 'Highmark HEDIS', 'BCBSTN HEDIS', 'Molina HEDIS Supplemental Region 5- IL-MI-WI', 'Molina HEDIS Region 2-ID-UT-WA', 'Humana HEDIS', 'Molina HEDIS Supplemental Region 4-NY-OH', 'Molina HEDIS Supplemental Region 6-FL-SC', 'Molina HEDIS Region 1-CA', 'Reveleer HEDIS', 'Centene HEDIS-WI', 'Molina HEDIS Supplemental Region 3-MS-NM-TX', 'Alliant Health Plans HEDIS', 'BCBS TN HEDIS OOA']    
    commerical_aca = business_lines(['Centene ACA',
        'Highmark ACA',
        'BCBS TN ACA',
        'Oscar HF ACA',
        'Priority Health ACA',
        'Med Mutual of Ohio ACA',
        'Highmark NY ACA'],2500, 'CC_ChartFinder', 'CC_Adhoc7')
    # commerical_aca = business_lines(['Advantasure ACA','Anthem ACA'],2500, 'CC_ChartFinder', 'CC_Adhoc7')
    # commerical_hedis = business_lines(['Centene HEDIS', 'WellCare HEDIS', 'Med Mutual of Ohio HEDIS','Anthem HEDIS','Anthem Comm HEDIS','Cigna HEDIS','HealthSpring HEDIS'],2500, 'CC_ChartFinder', 'CC_Adhoc8')
    commerical_hedis = business_lines(
        ['OptimaHealth HEDIS',
        'Devoted Health HEDIS',
        'Aetna HEDIS',
        'Advantasure_HEDIS_WA',
        'Molina HEDIS Region 6-FL-SC',
        'Aetna MEDICAID HEDIS',
        'Advantmed HEDIS',
        'Molina HEDIS Region 5-IL-MI-WI',
        'Advantasure_HEDIS_NE',
        'Gateway HEDIS',
        'Molina HEDIS Region 4-NY-OH',
        'Advantasure_HEDIS_VT',
        'Molina HEDIS Region 3-MS-NM-TX',
        'Advantasure_HEDIS_ND',
        'Advantasure_HEDIS_OOA_Anthem',
        'Highmark HEDIS',
        'BCBSTN HEDIS',
        'Molina HEDIS Supplemental Region 5- IL-MI-WI',
        'Molina HEDIS Region 2-ID-UT-WA',
        'Humana HEDIS',
        'Molina HEDIS Supplemental Region 4-NY-OH',
        'Molina HEDIS Supplemental Region 6-FL-SC',
        'Molina HEDIS Region 1-CA',
        'Reveleer HEDIS',
        'Centene HEDIS-WI',
        'Molina HEDIS Supplemental Region 3-MS-NM-TX',
        'Alliant Health Plans HEDIS',
        'BCBS TN HEDIS OOA'],2500, 'CC_ChartFinder', 'CC_Adhoc8')
    return [uhc, hedis, aca, medicaid, radv, commerical_hedis,commerical_aca, aetna]

def stack_inventory(df, grouping):
    business = ciox_business_lines()
    temp = {}
    for index, buz_line in enumerate(business):
        # create column on dataframe
        df = project_rank(df, buz_line, index)
        # create dict for scoring
        temp[f'temp_rank{index}'] = False

    rank_cols = {'meet_target_sla':True, **temp, 'no_call':False,'age':False, 'togo_bin':False} 
    # group by phone number or msid & rank highest value org
    find_parent = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = find_parent.overall_rank == 1
    find_parent['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    rank_parent = rank(find_parent,'Score', ['Skill','parent'], rank_cols)
    
    if grouping == 'PhoneNumber':
        for buz_line in business:
            if buz_line.system == 'CC_ChartFinder':
                f0 = rank_parent.Skill == buz_line.system
                f1 = rank_parent.Project_Type.isin(buz_line.projects)
                rank_parent.Skill = np.where(f0 & f1, buz_line.skill, rank_parent.Skill)


    rank_final = {'meet_target_sla':True, **temp,  'no_call':False, 'age_sort':False, 'togo_bin':False} 
    full_rank = rank(rank_parent,'Score', ['Skill','parent'], rank_final)
    full_rank.OutreachID = full_rank.OutreachID.apply(lambda x: str(x))
    full_rank['Matchees'] = full_rank.groupby(['Skill', grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return full_rank

def skill_score(df, skill, skill_rank):
    skill = df[df.Skill == skill].copy()
    dump = rank(skill,'Score', ['Skill','parent'], skill_rank)
    return dump.append(df).drop_duplicates(['OutreachID']).reset_index(drop= True)

def split(df):
    df['Outreach ID'] = df['OutreachID'].astype(str)
   
    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid, 'PhoneNumber')
    msid_scored = stack_inventory(msid, 'MasterSiteId')
    unique =  scored.append(msid_scored).drop_duplicates(['OutreachID']).reset_index(drop= True)

    ### skille that need special treatment
    skill_rank = {'meet_target_sla':True, 'no_call':False, 'ToGoCharts':False} 
    unique = skill_score(unique, 'CC_Adhoc7', skill_rank)

    skill_rank = {'meet_target_sla':True, 'no_call':False, 'ToGoCharts':False} 
    unique = skill_score(unique, 'CC_Adhoc8', skill_rank)

    # f1 = unique.Skill    == 'CC_Adhoc3'
    # f2 = unique.Outreach_Status == 'Scheduled'
    # f3 = unique.meet_target_sla == 1
    # f4 = unique.ScheduleDate    == '2022-03-31'
    # f4 = unique.meet_sla        == 0
    status_map = {'Unscheduled':1, 'Past Due':2, 'Scheduled':3, 'PNP Released':0,'Escalated':0}
    unique['status_map'] = unique.Outreach_Status.map(status_map)
    # unique.advance_sla_rank = np.where(f1 & f2 & f4 & ~f3, 2, unique.advance_sla_rank)
    # unique.advance_sla_rank = np.where(f1 & f2 & f4, 3, unique.advance_sla_rank)
    # unique.advance_sla_rank = np.where(f1 & ~f2 & ~f3, 4, unique.advance_sla_rank)
    skill_rank = {'meet_target_sla':True,'status_map':True, 'no_call':False, 'age':False} 
    unique = skill_score(unique, 'CC_Adhoc3', skill_rank)
    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(['Chart Sync']) # 'ACA-PhysicianCR'
    unique['Score'] = np.where(f0, 1000000, unique.Score)
    return unique

def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)