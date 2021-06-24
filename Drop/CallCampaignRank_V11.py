import pandas as pd
import numpy as np
import time
from datetime import date, timedelta, datetime
from FileLoad import Final_Load, Format
from Skills import Final_Skill
from Cluster import Load_Cluster, Adoc3, Badhoc7, Cluster, Clusters, Adoc3_Skill
from Rank import Last_Call
from Priority_Tracking import Project_Rank

startTime_1 = time.time()
## Load Date ##
today = date.today()
RAD4 = (today + timedelta(days = 4)).strftime("%m/%d/%Y")
print(RAD4)

tomorrow = (today + timedelta(days = 1)).strftime("%m/%d/%Y")
def normalizeDate(date):
    newDate = '/'.join(str.zfill(elem,2) for elem in date.split('-'))
    date_obj = datetime.strptime(newDate, '%m/%d/%Y').date()
    return date_obj
tomorrow = normalizeDate(tomorrow)



def Call_Campaign():
    df = Final_Load()

    Skilled_set = Final_Skill(df)
    Skilled_set['Cluster'] = 100

    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, CNA = Load_Cluster()

    ClusterGroup = Clusters(Skilled_set, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10)

    Final_Filtered = Format(ClusterGroup)
    Final_Filtered['Pacing Priority'] = 0
    def Retrieval_Method(RM, Group_Age):
        df = Final_Filtered[Final_Filtered['Group Number'] == Group_Age]
        Projects = list(Project_Rank(RM)['Project Type'])
        df = df[df['Retrieval Group'] == RM]
        x = 1
        for i in Projects:
            filter = df['Project Type'] == i
            df['Pacing Priority'] = np.where(filter, x, df['Pacing Priority'])
            x += 1
        G00 = df.sort_values('Pacing Priority').reset_index(drop=True)
        End = (G00['OutreachID'].count())
        Range = list(range(1, End))
        df = pd.DataFrame(Range,columns= ['Score'])
        G00['Score'] = df['Score']
        Final_Rank = G00.dropna(subset=['OutreachID'])
        return Final_Rank

    def Add_RM():
        def conat():
            Retrieval_Method()
        d1 = Retrieval_Method('Onsite')
        d2 = Retrieval_Method('Offsite')
        d3 = Retrieval_Method('EMR Remote')
        d4 = Retrieval_Method('HIH - Other')
        Group = pd.concat([d1, d2, d3, d4]).sort_values(['Score'])
        return Group.reset_index(drop=True)
    
    # print(Retrieval_Mid_Rank('Offsite', 0, 5)[['Retrieval Group','Project Type', 'Score','Group Number', 'Pacing Priority','Skill']])
    Final_Rank = Add_RM()
    
    return df
# print(Call_Campaign()[['Score', 'Skill','Load Date']])
# print(Call_Campaign().groupby(['Group Number']).agg({'OutreachID': 'count', 'ToGoCharts':'sum' }))
# print(Call_Campaign().groupby(['Skill']).agg({'OutreachID': 'count', 'ToGoCharts':'sum' }).sort_values(by= 'OutreachID',ascending=False))
df = Call_Campaign()
filter1 = (df['Outreach Status'] != 'Scheduled') & (df['Outreach Status'] != 'Escalated')
filter2 = df['Project Type'] == 'RADV'
df['Recommended Schedule Date'] = np.where(filter1 & filter2, str(RAD4), df['Recommended Schedule Date'])

print(df[['Outreach Status', 'Project Type', 'Recommended Schedule Date']])
# print(df['Outreach Status'].unique())
# # print(df['Recommended Schedule Date'].unique())

# # # # ## Send to CSV
# path = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\dump\\Group_Rank\\'
path2 = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\'
# df.to_csv(path + str(tomorrow) + '.csv', index=False) #str(tomorrow) +

df.to_csv(path2 + 'test.csv', index=False)

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")
