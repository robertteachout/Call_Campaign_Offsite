import pandas as pd
import pyodbc
import sys
from Bus_day_calc import next_business_day, Next_N_BD, map_piv, daily_piv, newPath, date_list_split
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
today = date.today() #-timedelta(days=1)

# print('Python: ' + sys.version.split('|')[0])
# print('Pandas: ' + pd.__version__)
# print('pyODBC: ' + pyodbc.version)


DB = {'servername': 'EUS1PCFSNAPDB01',
      'database': 'DWWorking'}

# create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')

# query db
sql = """
SELECT A.[Project Year]
     , A.[Audit Type]
     , A.[Client Category]
     , A.[Client / Optum Category]
     , A.[Client Project]
     , A.[Project ID]
     , A.[Project Type]
     , A.[Project Start Due]
     , A.[Project Due Date]
     , A.[SVP]
     , A.[Account Manager]
     , A.[Project Status]
     , A.[Total Charts]
     , A.[Net charts]
     , A.[Forecasted Netcharts]
     , A.[QA Completed]
     , A.[Targeted charts]
     , A.[Today's Targeted charts]
     , A.[Retrievable Charts]
     , A.[Next Week Charts]
     , A.[Production Charts]
     , A.[Time Percent]
     , A.[Tolerance Adjustment]
     , A.[Baseline Tolerance]
     , A.[Tolerance Charts]
     , A.[Next 7 Days]
     , A.[Next 14 Days]
     , A.[Charts Retrived in 1days]
     , A.[Charts Retrived in 5days]
     , A.[Charts Retrived in 10days]
     , A.[Avaialable]
     , A.[Unscheduled]
     , A.[Scheduled]
     , A.[Past Due]
     , A.[PNP Released]
     , A.[Rejected in QA]
     , A.[QA Work load]
     , A.[PNP]
     , A.[CNA]
     , A.Limbo
     , A.[SB.Digital Direct (E)]
     , A.[SB.Digital Direct (NE)]
     , A.[SB.Embedded Remote]
     , A.[SB.Embedded ROI]
     , A.[SB.EMR Remote]
     , A.[SB.HIH]
     , A.[SB.Offsite]
     , A.[SB.Onsite]
     , A.[Total Surplus Backlog]
     , A.[AV.Digital Direct (E)]
     , A.[AV.Digital Direct (NE)]
     , A.[AV.Embedded Remote]
     , A.[AV.Embedded ROI]
     , A.[AV.EMR Remote]
     , A.[AV.HIH]
     , A.[AV.Offsite]
     , A.[AV.Onsite]
     , A.[N.Digital Direct (E)]
     , A.[N.Digital Direct (NE)]
     , A.[N.Embedded Remote]
     , A.[N.Embedded ROI]
     , A.[N.HIH]
     , A.[N.Offsite]
     , A.[N.Onsite]
     , A.[N.EMR Remote]
     , A.[Reporting Category]
     , A.[Insert Date]
	 , B.YieldType
	 , A.[Prod Plan per Day (5BD)]
FROM DWWorking.Prod.Project_Tracking_Report_V2 AS A
LEFT JOIN (SELECT ProjectId, YieldType FROM DW_Operations.dbo.DimProject GROUP BY ProjectId, YieldType) AS B
ON A.[Project ID] = B.ProjectId
WHERE A.[Insert Date] = CAST(GETDATE() AS DATE)
      AND [Project Status] IN ('New', 'In Process')
      AND A.[Project Due Date] >= CAST(GETDATE() AS DATE)
	  and A.[Net Charts] > 0
--AND [Client Category] NOT IN ( 'Optum Insight' )
--AND [Project Type] NOT IN ( 'RADV' )
--AND A.[Project ID] = '2003200537_0627'

"""
def Cluster_Query():
    df = pd.read_sql(sql, conn)
    return df
path2 = newPath('Table_Drops','')
print(Cluster_Query())
# Cluster_Query().to_csv(path2 + 'Project Tracking.csv', index=False)
# Cluster_Query().to_csv(path2 + today + '.csv', index=False)
