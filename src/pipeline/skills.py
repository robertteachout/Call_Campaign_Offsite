import numpy as np
import pandas as pd

def special_handling(df):
    f1 = df['Agent'] == 'Special Handling'
    df['Skill'] = np.where(f1, 'PC_Special_Handling', df['Skill'])
    return df

def genpact(df):
    filter2 = df['OutreachID_Count'] == 1
    filter3 = df['Retrieval_Team'] == 'Genpact Offshore'
    df['Skill'] = np.where(filter2 &filter3, 'CC_Genpact_Scheduling', df['Skill'])
    return df

def fire_flag(df, skill_name):
    filer1 = df['Score'].str[:1]
    df['Skill'] = np.where(filer1, skill_name, df['Skill'])
    return df

def CC_Pend_Eligible(df):
    filter1 = df['CallCount'] >= 5
    filter2 = df['Outreach_Status'] == 'Unscheduled'
    df['Skill'] = np.where(filter1 & filter2, 'CC_Pend_Eligible', df['Skill'])
    return df

def genpactPRV_priority(df):
    f1 = df['Outreach_Status'] == 'Unscheduled'
    f2 = df['Project_Type'] == 'Cigna - IFP RADV'
    f3 = df['Project_Type'] == 'Med Mutual of Ohio'
    f5 = df['Project_Type'] == 'Clover Health MRA'

    f4 = df['Age'] > 2
    f6 = df['Age'] == 0
    f7 = df['CallCount'] < 5

    df['Skill'] = np.where(f1 & f2 &  f4, 'CC_GenpactPRV_Priority', df['Skill'])
    df['Skill'] = np.where(f1 & (f4 | f6) & f7 & ( f3 | f5), 'CC_GenpactPRV_Priority', df['Skill'])
    return df

def wellmed_schedule(df):
    filter1 = df['Skill'] == 'CC_Wellmed_Sub15_UNS'
    filter4 = df['Outreach_Status'] == 'Scheduled'

    filter2 = df['Age'] <= 2
    filter3 = df['Age'] > 0
    df['Skill'] = np.where(filter1 & (filter2 | filter3) & filter4, 'Child_ORG', df['Skill'])
    return df

def emr_rm(df):
    f1 = df['Retrieval_Group'] == 'EMR Remote'
    df['Skill'] = np.where(f1, 'EMR_Remote_removed', df['Skill'])
    return df

def Osprey(df):
    f1 = df['Project_Type'] == 'Osprey'
    f2 = df['Outreach_Status'] != 'Scheduled'
    f3 = df['Outreach_Status'] != 'Escalated'
    # removeing inventory
    df['Skill'] = np.where(f1 & f2 & f3, 'CC_Osprey_Outbound', df['Skill'])
    return df

def rm_schedule(df):
    f1 = df['Outreach_Status'] == 'Scheduled'
    df['Skill'] = np.where(f1, 'schedule_pull', df['Skill'])
    return df

def adhoc2(df):
    projects = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']
    f1 = df['Project_Type'].isin(projects)
    f2 = df['Last_Call'].isna()
    f3 = df['CallCount'] >= 8

    df['Skill'] = np.where(f1 & (f2 | f3), 'CC_Adhoc2', df['Skill'])
    return df

def aetna_commercial(df): 
    f1 = df['Project_Type'] == 'Aetna Commercial'
    f2 = df['CallCount'] == 0
    df['Skill'] = np.where(f1 & f2, 'CC_Adhoc1', df['Skill'])
    return df

def research_pull(df):
    f1 = df['PhoneNumber'] == 9999999999
    df['Skill'] = np.where(f1, 'Research_Pull ', df['Skill'])
    return df

def last_call(df,nbd):
    # convert CF last call date to child org / child ORG's won't be affected
    df['Last_Call'] = pd.to_datetime(df['Last_Call'], errors='coerce').dt.date
    filter1 = df['Last_Call'] >= nbd #x_Bus_Day_ago(3)
    filter2 = df['Skill'] != 'CC_Pend_Eligible'
    df['Skill'] = np.where(filter1 & filter2, 'Child_ORG', df['Skill'])
    return df

def Re_Skill_Tier(df):
    df_local = df
    filter1 = df_local['OutreachID_Count'] ==1
    filter2 = df_local['OutreachID_Count'] >=1
    filter3 = df_local['OutreachID_Count'] <=4
    filter4 = df_local['OutreachID_Count'] >=5 #### ????
    
    filter5 = (df_local['Outreach_Status'] == 'Unscheduled') 
    filter6 = (df_local['Outreach_Team'] == 'Sub 15') 

    df_local['Skill'] = np.where(filter4, 'CC_Tier1', df_local['Skill'])
    df_local['Skill'] = np.where(filter2 & filter3, 'CC_Tier2', df_local['Skill'])
    df_local['Skill'] = np.where(filter5 & filter6 & filter1, 'CC_Tier3', df_local['Skill'])
    return df_local

def fill(df):
    f1 = df['Skill'].isnull()
    f2 = df['Skill'] == 'NaN'
    df['Skill'] = np.where(f1 | f2, 'CC_Tier2', df['Skill'])
    return df

def escalations(df):
    f1 = df['Outreach_Status'] == 'Escalated'
    f2 = df['Outreach_Status'] == 'PNP Released'
    df['Skill'] = np.where(f1 | f2, 'CC_Escalation', df['Skill'])
    return df

def wellmed(df):
    f1 = df['Project_Type'] == 'WellMed'
    df['Skill'] = np.where(f1, 'CC_Wellmed_Sub15_UNS', df['Skill'])
    return df

def complex_skills(df, nbd):
    f = df 
    f = Re_Skill_Tier(f)
    f = wellmed(f)
    f = escalations(f)
    
    f = genpact(f)
    f = genpactPRV_priority(f)
    f = wellmed_schedule(f)
    f = last_call(f, nbd)
    # f = CC_Pend_Eligible(f)
    f = research_pull(f)
    f = rm_schedule(f)
    f = adhoc2(f)
    f = aetna_commercial(f)
    f = Osprey(f)
    f = fill(f)
    f = emr_rm(f)
    return f
