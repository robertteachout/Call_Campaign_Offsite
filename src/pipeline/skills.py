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

    f4 = df['Outreach_Status'] == 'Escalated'
    f5 = df['Outreach_Status'] == 'PNP Released'
    # removeing inventory
    df['Skill'] = np.where(f1 & f2 & f3, 'CC_Osprey_Outbound', df['Skill'])
    df['Skill'] = np.where(f1 & (f4 | f5), 'Osprey_Escalation', df['Skill'])
    return df

def rm_schedule(df):
    f1 = df['Outreach_Status'] == 'Scheduled'
    df['Skill'] = np.where(f1, 'schedule_pull', df['Skill'])
    return df

def adhoc2(df):
    projects = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']
    f1 = df['Project_Type'].isin(projects)
    f2 = df['Last_Call'].isna()
    f3 = df['age_category'] >= 10
    f4 = df['Outreach_Status'] != 'Scheduled'

    df['Skill'] = np.where(f1 & (f2 | f3) & f4, 'CC_Adhoc2', df['Skill'])
    return df

def adhoc1(df, advantasure): 
    f1 = df['Project_Type'] == 'Aetna Commercial'
    f2 = df['CallCount'] == 0

    f6 = df['OutreachID'].isin(advantasure)
    df['Skill'] = np.where((f1 & f2) | f6, 'CC_Adhoc1', df['Skill'])
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
    f1 = df['OutreachID_Count'] ==1
    f2 = df['OutreachID_Count'] >=1
    f3 = df['OutreachID_Count'] <=4
    f4 = df['OutreachID_Count'] >=5 #### ?

    df['Skill'] = np.where(f4, 'CC_Tier1', df['Skill'])
    df['Skill'] = np.where(f2 & f3, 'CC_Tier2', df['Skill'])
    return df

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

def mastersiteID(df):
    f1 = df['mastersiteID'].notna()
    f2 = df['mastersiteID'] != 1000838
    df['Skill'] = np.where(f1 & f2, 'mastersite_inventory', df['Skill'])
    return df

def complex_skills(df, nbd, advantasure=list()):
    f = df 
    f = Re_Skill_Tier(f)
    # f = wellmed(f)
    f = escalations(f)
    
    # f = genpact(f)
    # f = genpactPRV_priority(f)
    # f = wellmed_schedule(f)
    f = last_call(f, nbd)
    # f = CC_Pend_Eligible(f)
    f = rm_schedule(f)
    # mass filter 
    f = mastersiteID(f)

    f = adhoc2(f)
    f = adhoc1(f, advantasure)
    f = Osprey(f)
    f = fill(f)
    f = emr_rm(f)
    f = research_pull(f)
    return f
