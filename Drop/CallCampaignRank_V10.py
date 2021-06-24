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
RAD4 = (today + timedelta(days = 7))#.strftime("%m/%d/%Y")
print(RAD4)

tomorrow = (today + timedelta(days = 3)).strftime("%Y-%m-%d")

def Call_Campaign():
    df = Final_Load()

    Skilled_set = Final_Skill(df)
    Skilled_set['Cluster'] = 100

    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, CNA = Load_Cluster()

    ClusterGroup = Clusters(Skilled_set, C1, C2, C3, C4, C5, C6, C7, C8, C9, C10)

    Final_Filtered = Format(ClusterGroup)

    def cluster_list(Priority):
        cluster_list = []
        for i in range(1, 11):
            cluster_list.append(i)
        cluster_list.insert(Priority, 100)

    def Retrival_Group(Group):
        Retrieval = Final_Filtered.loc[Final_Filtered['Retrieval Group'] ==  Group].copy()
        return Retrieval

    def Retrieval_Mid_Rank(Retrival, Days, Order):
        G00 = Last_Call(Retrival_Group(Retrival), Days)
        G00 = G00.sort_values(['Age'], ascending=False).reset_index(drop=True)
        # G00['Group Number'] = Days
        ## Add project tracking
        Projects = list(Project_Rank(Retrival)['Project Type'])
        G00['Pacing Priority'] = 0
        x= 1
        for i in Projects:
            filter = G00['Project Type'] == i
            G00['Pacing Priority'] = np.where(filter, x, G00['Pacing Priority'])
            x += 1
        G00 = G00.sort_values(['Pacing Priority']).reset_index(drop=True)
        End = (G00['OutreachID'].count()) * Order
        Range = list(range(1, End, Order))
        df = pd.DataFrame(Range,columns= ['Score'])
        G00['Score'] = df['Score']
        return G00.dropna(subset=['OutreachID'])



    ######## Rank Call Age #################################
    Calls_00 = 1
    Calls_05 = 5
    Calls_10 = 4
    Calls_15 = 3
    Calls_20 = 2
    Calls_21 = 1
    ########################################################
    def Concat_1(Retrieval):
        G00 = Retrieval_Mid_Rank(Retrieval, 0, Calls_00)
        G01 = Retrieval_Mid_Rank(Retrieval, 5, Calls_05)
        G05 = Retrieval_Mid_Rank(Retrieval, 10, Calls_10)
        G10 = Retrieval_Mid_Rank(Retrieval, 15, Calls_15)
        G15 = Retrieval_Mid_Rank(Retrieval, 20, Calls_20)
        G20 = Retrieval_Mid_Rank(Retrieval, 21, Calls_21)
        Group = pd.concat([G00,G01,G05,G10,G15,G20]).sort_values(['Score'])
        return Group.reset_index(drop=True)

    def Retrieval_Final_Rank(Retrival, Order):
        Group = Concat_1(Retrival)
        Group = Group.loc[Group['OutreachID'].notnull()]
        End = (Group['Score'].count()) * Order
        Range = list(range(1, End, Order))
        Retrieval = pd.DataFrame(Range,columns= ['Score'])
        Group['Score'] = Retrieval['Score']
        return Group.sort_values(['Score'])

    ######### Rank Retrieval Type ##########################
    Onsite = 1
    Offsite = 1
    EMR_Remote = 1
    HIH_Other = 1
    ########################################################
    def Concat_2():
        G00 = Retrieval_Final_Rank('Onsite', Onsite)
        G05 = Retrieval_Final_Rank('Offsite', Offsite)
        G10 = Retrieval_Final_Rank('EMR Remote', EMR_Remote)
        G15 = Retrieval_Final_Rank('HIH - Other', HIH_Other)
        Group = pd.concat([G00, G05,G10,G15]).sort_values(['Score']).reset_index(drop=True)
        Group = Group.loc[Group['OutreachID'].notnull()]
        End = (Group['Score'].count())
        Range = list(range(1, End, 1))
        Retrieval = pd.DataFrame(Range,columns= ['Score'])
        Group['Score'] = Retrieval['Score']
        return Group

    Final_Rank = Concat_2()
    return Final_Rank

df = Call_Campaign()
filter1 = (df['Outreach Status'] != 'Scheduled') & (df['Outreach Status'] != 'Escalated')
filter2 = df['Project Type'] == 'RADV'
df['Recommended Schedule Date'] = np.where(filter1 & filter2, str(RAD4), df['Recommended Schedule Date'])
# print(df[['Age', 'Last Call', 'Group Number']])

# # # # ## Send to CSV
path = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\dump\\Group_Rank\\'
df.to_csv(path + str(tomorrow) + '.csv', index=False) #str(tomorrow) +

path2 = 'C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\'
df.to_csv(path2 + 'Group_Rank.csv', index=False)

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")
