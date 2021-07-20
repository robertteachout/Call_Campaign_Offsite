import pandas as pd
import pyodbc
import sys
import numpy as np
import time

startTime_1 = time.time()

df = pd.read_csv('C:\\Users\\roeth\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drop\\Group_Rank.csv', sep=',',low_memory=False)
df.columns = df.columns.str.replace('/ ','')
df = df.rename(columns=lambda x: x.replace(' ', "_")).drop(columns='Matches')
df.iloc[:, 1:] = df.iloc[:, 1:].astype(str)
# print(df)

database = 'test_campaign_file'

DB = {'servername': 'HOME\SQLSERVER2019',
    'database': database}
# create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')

cursor = conn.cursor()

# # Create Table
cursor.execute('''DELETE FROM dbo.Campaign''')

# cursor.execute('CREATE TABLE Campaign (OutreachID int, Outreach_Team nvarchar(50), Client_Optum_Category nvarchar(50), Recommended_Schedule_Date nvarchar(50), PhoneNumber nvarchar(50), Address nvarchar(50), City nvarchar(50), State nvarchar(50), Zip nvarchar(50), Agent nvarchar(50), Market nvarchar(50), Project_Due_Date nvarchar(50), TimeZone nvarchar(50), TimeZoneKey nvarchar(50), Site_Clean_Id nvarchar(50), Project_Type nvarchar(50), Audit_Type nvarchar(50), PNP_Code nvarchar(50), ToGoCharts nvarchar(50), TotalCharts nvarchar(50), RetrievedCharts nvarchar(50), CallCount nvarchar(50), Retrieval_Method nvarchar(50), Retrieval_Group nvarchar(50), ScheduleDate nvarchar(50), Age nvarchar(50), Retrieval_Team nvarchar(50), Last_Call nvarchar(50), DaysSinceCreation nvarchar(50), Outreach_Status nvarchar(50), Score nvarchar(50), Skill nvarchar(50), Group_Number nvarchar(50), Cluster nvarchar(50), Load_Date nvarchar(50), Daily_Groups nvarchar(50), OutreachID_Count nvarchar(50), Daily_Priority nvarchar(50), status_sort nvarchar(50), rm_sort nvarchar(50), age_sort nvarchar(50), audit_sort nvarchar(50), TotalChartsAgg nvarchar(50), Cluster_Avg nvarchar(50), Past_Due nvarchar(50), Scheduled nvarchar(50), Unscheduled nvarchar(50), Escalated nvarchar(50), PNP_Released nvarchar(50), Unique_Phone nvarchar(50))')

# # Insert DataFrame to Table
for row in df.itertuples():
    cursor.execute('''
                    INSERT INTO test_campaign_file.dbo.Campaign
                    (OutreachID, Outreach_Team, Client_Optum_Category, Recommended_Schedule_Date, PhoneNumber, Address, City, State, Zip, 
                    Agent, Market, Project_Due_Date, TimeZone, TimeZoneKey, Site_Clean_Id, Project_Type, Audit_Type, PNP_Code, ToGoCharts, 
                    TotalCharts, RetrievedCharts, CallCount, Retrieval_Method, Retrieval_Group, ScheduleDate, Age, Retrieval_Team, Last_Call, 
                    DaysSinceCreation, Outreach_Status, Score, Skill, Group_Number, Cluster, Load_Date, Daily_Groups, OutreachID_Count, 
                    Daily_Priority, status_sort, rm_sort, age_sort, audit_sort, TotalChartsAgg, Cluster_Avg, Past_Due, Scheduled, Unscheduled, 
                    Escalated, PNP_Released, Unique_Phone)  
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''', 
                    row.OutreachID, row.Outreach_Team, row.Client_Optum_Category, row.Recommended_Schedule_Date, row.PhoneNumber, 
                    row.Address, row.City, row.State, row.Zip, row.Agent, row.Market, row.Project_Due_Date, row.TimeZone, row.TimeZoneKey, 
                    row.Site_Clean_Id, row.Project_Type, row.Audit_Type, row.PNP_Code, row.ToGoCharts, row.TotalCharts, row.RetrievedCharts, 
                    row.CallCount, row.Retrieval_Method, row.Retrieval_Group, row.ScheduleDate, row.Age, row.Retrieval_Team, row.Last_Call, 
                    row.DaysSinceCreation, row.Outreach_Status, row.Score, row.Skill, row.Group_Number, row.Cluster, row.Load_Date, 
                    row.Daily_Groups, row.OutreachID_Count, row.Daily_Priority, row.status_sort, row.rm_sort, row.age_sort, row.audit_sort, 
                    row.TotalChartsAgg, row.Cluster_Avg, row.Past_Due, row.Scheduled, row.Unscheduled, row.Escalated, row.PNP_Released, row.Unique_Phone)

conn.commit()

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")