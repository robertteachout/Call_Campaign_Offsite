# from numpy.lib.arraysetops import unique
import pandas as pd
import numpy as np
from FileLoad import Final_Load


def F_Status(df, Status):
    df_local = df
    if Status != 'NA':
        filter = df_local['Outreach Status'] == Status
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter

def F_Audit(df, Audit):
    df_local = df
    if Audit != 'NA':
        filter = df_local['Audit Type'] == Audit
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter

def F_ClientOptumCategory(df, Category):
    df_local = df
    if Category != 'NA':
        filter = df_local['Client / Optum Category'] == Category
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter
def F_Retrieval_Method(df, Method):
    df_local = df
    if Method != 'NA':
        filter = df_local['Retrieval Method'] == Method
    else:
        filter = df_local['Outreach Status'] != 'NA'
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
        filter = df_local['Project Due Date'] == DueDate
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter

def F_SchedualDate(df, SchedualDate):
    df_local = df
    if SchedualDate != 'NA':
        filter = df_local['Recommended Schedule Date'] == SchedualDate
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter

def F_ProjectType(df, Project_Type):
    df_local = df
    if Project_Type != 'NA':
        filter = df_local['Project Type'] == Project_Type
    else:
        filter = df_local['Outreach Status'] != 'NA'
    return filter

def F_Status_only(df):
    df_local = df
    filter = (df_local['Project Status'] == 'Unscheduled') | (df_local['Project Status'] == 'Past Due') | (df_local['Project Status'] == 'Scheduled')
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
    filter3 = df_local['Retrieval Team'] == 'Genpact Offshore'
    df_local['Skill'] = np.where(filter3, 'Genpact', df_local['Skill'])
    return df_local

def Re_Skill_status(df, status, skill_name):
    filter1 = F_Status(df, status)
    df['Skill'] = np.where(filter1, skill_name, df['Skill'])
    return df

def Re_Skill_Tier(df):
    df_local = df
    filter2 = df_local['OutreachID Count'] >=1
    filter3 = df_local['OutreachID Count'] <=4
    filter4 = df_local['OutreachID Count'] >=5
    
    filter5 = (df_local['Outreach Status'] == 'Unscheduled') 
    filter6 = (df_local['Outreach Team'] == 'Sub 15') 
    filter7 = (df_local['OutreachID Count'] == 1)

    df_local['Skill'] = np.where(filter2 & filter3, 'CC_Tier2', df_local['Skill'])
    
    df['Skill'] = np.where(filter5 & filter6 & filter7, 'CC_Tier3', df['Skill'])
    df_local = df_local.copy()

    df_local['Skill'] = np.where(filter4, 'CC_Tier1', df_local['Skill'])
    
    return df_local

def complex_skills(df):
    f = df
    f = Re_Skill_Tier(f)

    f = Re_Skill_Project(f, 'Unscheduled', 'WellMed', 16, 300,'CC_Wellmed_Plus15_UNS')
    f = Re_Skill_Project(f, 'Unscheduled', 'WellMed', 1, 15,'CC_Wellmed_Sub15_UNS')
    f = Re_Skill_Project(f, 'Past Due', 'WellMed', 16, 300,'CC_Wellmed_Plus15_PD')
    f = Re_Skill_Project(f, 'Past Due', 'WellMed', 1, 15,'CC_Wellmed_Sub15_PD')
    f = Re_Skill_status(f, 'Escalated', 'CC_Escalation')
    
    f = Re_Skill_Genpact(f)
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

# test = Final_Skill(Final_Load())
# print(test.groupby(['Skill']).agg({'OutreachID': 'count', 'ToGoCharts':'sum' }).sort_values(by= 'OutreachID',ascending=False))
# print(test[test['Skill'] == 'PC_Adhoc2'])
if __name__ == "__main__":
    print("Skills")
