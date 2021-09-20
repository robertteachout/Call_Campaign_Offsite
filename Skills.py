import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath, time_check

today = date.today()
B10 = Next_N_BD(today, 10)
tomorrow = next_business_day(today)
D2 = next_business_day(tomorrow)
# from FileLoad import Final_Load

def F_Status(df, Status):
    df_local = df
    if Status != 'NA':
        filter = df_local['Outreach_Status'] == Status
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_Audit(df, Audit):
    df_local = df
    if Audit != 'NA':
        filter = df_local['Audit_Type'] == Audit
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_ClientOptumCategory(df, Category):
    df_local = df
    if Category != 'NA':
        filter = df_local['Client / Optum Category'] == Category
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter
def F_Retrieval_Method(df, Method):
    df_local = df
    if Method != 'NA':
        filter = df_local['Retrieval Method'] == Method
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_ToGoCharts(df, Start, End):
    df_local = df
    if Start != 'NA':
        filter1 = df_local['TotalCharts'] >= Start
        filter2 = df_local['TotalCharts'] <= End
    else:
        filter1 = df_local['TotalCharts'] != 'NA' 
        filter2 = df_local['TotalCharts'] != 'NA'
    return filter1, filter2 

def F_DueDate(df, DueDate):
    df_local = df
    if DueDate != 'NA':
        filter = df_local['Project_Due_Date'] == DueDate
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_SchedualDate(df, SchedualDate):
    df_local = df
    if SchedualDate != 'NA':
        filter = df_local['Recommended_Schedule_Date'] == SchedualDate
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_ProjectType(df, Project_Type):
    df_local = df
    if Project_Type != 'NA':
        filter = df_local['Project_Type'] == Project_Type
    else:
        filter = df_local['Outreach_Status'] != 'NA'
    return filter

def F_Status_only(df):
    df_local = df
    filter = (df_local['Project_Status'] == 'Unscheduled') | (df_local['Project_Status'] == 'Past Due') | (df_local['Project_Status'] == 'Scheduled')
    return filter

def Re_Skill_Client_Category(df, Status, Category, Method, Method2, Start, End, DueDate, Skill_Name):
    df_local = df
    filter1 = F_Status(df_local, Status)
    filter2 = F_ClientOptumCategory(df_local, Category)
    filter3 = F_Retrieval_Method(df_local, Method)                  
    filter4 = F_Retrieval_Method(df_local, Method2)                  
    filter5, filter6 = F_ToGoCharts(df_local, Start, End)
    filter7 = F_DueDate(df_local, DueDate)                 
    df_local['Skill'] = np.where(filter1 & filter2 & (filter3 | filter4) & (filter5 & filter6) & filter7, Skill_Name, df_local['Skill'])
    return df_local

def Re_Skill_Project(df, Status, Project_Type, Start, End,Skill_Name):
    df_local = df
    filter1 = F_Status(df_local, Status)
    filter2 = F_ProjectType(df_local, Project_Type)
    filter3, filter4 = F_ToGoCharts(df_local, Start, End)

    df_local['Skill'] = np.where(filter1 & filter2 & (filter3 & filter4), Skill_Name, df_local['Skill'])
    return df_local

def Re_Skill_Adhoc(df, Status,  Start, End, Skill_Name):
    df_local = df
    filter1 = F_Status(df_local, Status)
    filter2, filter3 = F_ToGoCharts(df_local, Start, End)
    df_local['Skill'] = np.where(filter1 & (filter2 & filter3), Skill_Name, df_local['Skill'])
    return df_local

def Re_Skill_Retrieval(df, Status, Method, Method2, Skill_Name):
    df_local = df
    filter1 = F_Status(df_local, Status)
    filter2 = F_Retrieval_Method(df_local, Method)                  
    filter3 = F_Retrieval_Method(df_local, Method2)   
    df_local['Skill'] = np.where(filter1 & (filter2 | filter3), Skill_Name, df_local['Skill'])
    return df_local

def Re_Skill_Audit(df, Status, Audit, Audit2, Start, End, Skill_Name):
    df_local = df
    filter1 = F_Status(df_local, Status)
    filter2 = F_Audit(df_local, Audit)                  
    filter3 = F_Audit(df_local, Audit2)
    filter4, filter5 = F_ToGoCharts(df_local, Start, End)
    df_local['Skill'] = np.where(filter1 & (filter2 | filter3) & (filter4 & filter5), Skill_Name, df_local['Skill'])
    return df_local

def Re_Skill_Agent(df):
    df_local = df
    filter3 = df_local['Agent'] == 'Special Handling'
    df_local['Skill'] = np.where(filter3, 'PC_Special_Handling', df_local['Skill'])
    return df_local

def Re_Skill_Genpact(df):
    df_local = df
    filter2 = df_local['OutreachID_Count'] == 1
    filter3 = df_local['Retrieval_Team'] == 'Genpact Offshore'
    df_local['Skill'] = np.where(filter2 &filter3, 'CC_Genpact_Scheduling', df_local['Skill'])
    return df_local

def Re_Skill_status(df, status, skill_name):
    filter1 = F_Status(df, status)
    df['Skill'] = np.where(filter1, skill_name, df['Skill'])
    return df

def random_skill(df):
    filter1 = F_Status(df, 'Unscheduled')
    filter11 = F_Status(df, 'Past Due')
    filter2 = F_ProjectType(df, 'Cigna - IFP RADV')
    filter3 = F_ProjectType(df, 'Med Mutual of Ohio')
    filter5 = F_ProjectType(df, 'Clover Health MRA')
    filter4 = df['Age'] > 2
    filter6 = df['Age'] == 0
    filter7 = df['CallCount'] < 5

    df['Skill'] = np.where(filter1 & filter2 & filter4, 'CC_GenpactPRV_Priority', df['Skill'])
    df['Skill'] = np.where((filter1 | filter11) & filter3 & (filter4 | filter6) & filter7, 'CC_GenpactPRV_Priority', df['Skill'])
    df['Skill'] = np.where((filter1 | filter11) & filter5 & (filter4 | filter6) & filter7, 'CC_GenpactPRV_Priority', df['Skill'])
    ### Dave gave this to specical team and request to pull out tell friday
    # df['Skill'] = np.where(filter2, 'Child_ORG', df['Skill'])
    return df

def wellmed_schedual(df):
    filter1 = df['Skill'] == 'CC_Wellmed_Sub15_UNS'
    filter4 = df['Outreach_Status'] == 'Scheduled'

    filter2 = df['Age'] <= 2
    filter3 = df['Age'] > 0
    df['Skill'] = np.where(filter1 & (filter2 | filter3) & filter4, 'Child_ORG', df['Skill'])
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

def Schedual_Child(df):
    filter1 = (df['Outreach_Status'] == 'Scheduled')
    filter2 = (df['ScheduleDate'] != tomorrow)
    df['Skill'] = np.where(filter1 & filter2, 'Child_ORG', df['Skill'])
    return df

def convert(df):
    filter1 = (df['Daily_Groups'] >= tomorrow)
    # filter2 = (df['Daily_Groups'] == D2)
    filter3 = (df['Age'] == 0)
    filter4 = (df['Skill'] == 'CC_Tier2')
    filter5 = (df['Skill'] == 'CC_Tier1')
    filter6 = (df['Unique_Phone'] != 1)
    df['Skill'] = np.where(filter3 & (filter1) & filter4, 'Child_ORG', df['Skill'])
    df['Skill'] = np.where(filter3 & (filter1) & filter5, 'Child_ORG', df['Skill'])
    df['Skill'] = np.where(filter6, 'Child_ORG', df['Skill'])
    return df

def complex_skills(df):
    f = df 
    f = Re_Skill_Tier(f)
    # f = Re_Skill_Project(f, 'Unscheduled', 'WellMed', 16, 300,'CC_Wellmed_Plus15_UNS')
    f = Re_Skill_Project(f, 'NA', 'WellMed', 1, 300,'CC_Wellmed_Sub15_UNS')
    # f = Re_Skill_Project(f, 'Past Due', 'WellMed', 16, 300,'CC_Wellmed_Plus15_PD')
    # f = Re_Skill_Project(f, 'Past Due', 'WellMed', 1, 15,'CC_Wellmed_Sub15_PD')
    f = Re_Skill_status(f, 'Escalated', 'CC_Escalation')
    f = Re_Skill_status(f, 'PNP Released', 'CC_Escalation')
    
    f = Re_Skill_Genpact(f)
    f = random_skill(f)
    f = wellmed_schedual(f)
    # f = convert(f)
    # f = Schedual_Child(f)  #### flip with Matts aproval 
    return f

def Final_Skill(df):
    df_local = df
    f = Re_Skill_Project(df_local, 'Unscheduled', 'NA', 1, 15, 'PC_Sub15_UNS')
    f = Re_Skill_Project(f, 'Unscheduled', 'NA', 16, 49, 'PC_16-49_UNS')
    f = Re_Skill_Project(f, 'Unscheduled', 'NA', 50, 300, 'PC_Plus50_UNS')
    f = Re_Skill_Project(f, 'Past Due', 'NA', 1, 15, 'PC_Sub15_PD')
    f = Re_Skill_Project(f, 'Past Due', 'NA', 16, 49, 'PC_16-49_PD')
    f = Re_Skill_Project(f, 'Past Due', 'NA', 50, 300, 'PC_Plus50_PD')

    f = Re_Skill_Retrieval(f, 'Unscheduled', 'On Site Pending', 'EMR - Remote', 'PC_Adhoc2')

    ## WellMed
    f = Re_Skill_Project(f, 'Unscheduled', 'WellMed', 16, 300,'PC_Wellmed_Plus15_UNS')
    f = Re_Skill_Project(f, 'Unscheduled', 'WellMed', 1, 15,'PC_Wellmed_Sub15_UNS')
    f = Re_Skill_Project(f, 'Past Due', 'WellMed', 16, 300,'PC_Wellmed_Plus15_PD')
    f = Re_Skill_Project(f, 'Past Due', 'WellMed', 1, 15,'PC_Wellmed_Sub15_PD')

    f= Re_Skill_Audit(f, 'Unscheduled', 'RADV', 'Medicaid Risk', 16, 299, 'PC_RADVMCAID_Plus15_UNS')
    f= Re_Skill_Audit(f, 'Unscheduled', 'RADV', 'Medicaid Risk', 1, 15, 'PC_RADVMCAID_Sub15_UNS')
    f= Re_Skill_Audit(f, 'Past Due', 'RADV', 'Medicaid Risk', 16, 299, 'PC_RADVMCAID_Plus15_PD')
    f= Re_Skill_Audit(f, 'Past Due', 'RADV', 'Medicaid Risk', 1, 15, 'PC_RADVMCAID_Sub15_PD')
    f= Re_Skill_Audit(f, 'Scheduled', 'RADV', 'Medicaid Risk', 16, 299, 'PC_RADVMCAID_Plus15_PreEmp')
    f= Re_Skill_Audit(f, 'Scheduled', 'RADV', 'Medicaid Risk', 1, 15, 'PC_RADVMCAID_Sub15_PreEmp')
    
    f = Re_Skill_Project(f, 'NA', 'Oscar RADV', 'NA','NA', 'PC_Pilot1')

    f = Re_Skill_Agent(f)
    f = Re_Skill_Genpact(f, 'Genpact')
    return f


if __name__ == "__main__":
    print("Skills")
