import numpy as np
import pandas as pd

def special_handling(df):
    f1 = df['Agent'] == 'Special Handling'
    df['Skill'] = np.where(f1, 'PC_Special_Handling', df['Skill'])
    return df

def CC_Genpact_Scheduling(df):
    msi_table = df[df.Skill =='CC_Cross_Reference'].groupby('mastersiteID')['OutreachID'].count().reset_index()
    cf_table = df[df.Skill == 'CC_Chartfinder'].groupby('PhoneNumber')['OutreachID'].count().reset_index()
    unique_msid = msi_table[msi_table.OutreachID == 1]['mastersiteID'].astype(int).to_list()
    unique_cf = cf_table[cf_table.OutreachID == 1]['PhoneNumber'].astype(int).to_list()
    # chart = df.TotalCharts <= 15
    f0 = df.PhoneNumber.isin(unique_cf)
    f1 = df.mastersiteID.isin(unique_msid) 
    f2 = df.Retrieval_Team  == 'Genpact Offshore'
    df['Skill'] = np.where( (f0 | f1) & f2, 'CC_Genpact_Scheduling', df['Skill'])
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
    df['Skill'] = np.where(f1, 'CC_Osprey_Outbound', df['Skill'])
    df['Skill'] = np.where(f1 & (f4 | f5), 'Osprey_Escalation', df['Skill'])
    return df

def rm_schedule(df):
    f1 = df['Outreach_Status'] == 'Scheduled'
    f2 = df['Last_Call'].notna()
    df['Skill'] = np.where(f1 & f2, 'schedule_pull', df['Skill'])
    return df

def adhoc2(df):
    projects = ['AHN','CDQI HCR','NAMMCA','OC-AZ','OC-NV','OCN-WA','OC-UT','Reliant','Riverside', 'WellMed']
    f1 = df['Project_Type'].isin(projects)
    f2 = df['Last_Call'].isna()
    f3 = df['age_category'] >= 10
    f4 = df['Outreach_Status'] != 'Scheduled'

    df['Skill'] = np.where(f1 & (f2 | f3) & f4, 'CC_Adhoc1', df['Skill'])
    return df

def adhoc1(df, advantasure): 
    f1 = df['Project_Type'] == 'Aetna Commercial'
    f2 = df['CallCount'] == 0

    f6 = df['OutreachID'].isin(advantasure)
    df['Skill'] = np.where((f1 & f2) | f6, 'CC_Adhoc1', df['Skill'])
    return df

def research_pull(df):
    f0 = df['Project_Type'] == 'Osprey'
    f1 = df['PhoneNumber'] == '9999999999'
    df['Skill'] = np.where(f1, 'Research_Pull ', df['Skill'])
    df['Skill'] = np.where(f1 & f0, 'Osprey_research', df['Skill'])
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
    df['Skill'] = np.where(f1 & f2, 'CC_Cross_Reference', df['Skill'])
    return df

def quicklist(df):
    nocall_list = pd.read_csv('data/table_drop/nocalllist.csv')
    f1 = nocall_list.OutreachID.tolist()
    f2 = df.OutreachID.isin(f1) 
    df['Skill'] = np.where(f2, 'CC_Tier2', df['Skill'])
    df['quicklist'] = np.where(f2, 1,0)
    return df

def chartfinder(df):
    df['Skill'] = 'CC_ChartFinder'
    return df

def Cross_Reference_SPI(df):
    f1 = df['SPI'] == 'TRUE'
    df['Skill'] = np.where(f1, 'CC_Cross_Reference_SPI', df['Skill'])
    return df

def UHC_HEDIS(df):
    f1 = df['Project_Type'] == 'UHC HEDIS'
    df['Skill'] = np.where(f1, 'CC_ChartFinder', df['Skill'])
    return df

def complex_skills(df):
    f = df 
    f = chartfinder(f)
    f = mastersiteID(f)
    
    f = CC_Genpact_Scheduling(f)
    f = UHC_HEDIS(f)
    f = rm_schedule(f)
    f = escalations(f)    
    f = Osprey(f)
    f = emr_rm(f)
    f = research_pull(f)
    f = Cross_Reference_SPI(f)
    return f
