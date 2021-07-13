import pandas as pd
import time
from datetime import date, timedelta, datetime

from dbo_Query import Query
from Bus_day_calc import newPath
startTime_1 = time.time()

today = date.today()

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

df = Query('DWWorking', sql)

def save_ptr():
      path1 = newPath('dump','Project_Tracking')
      path2 = newPath('Table_Drop','')
      df.to_csv(path2 + 'Project_Tracking.csv', index=False)
      df.to_csv(path1 + str(today) + '.csv', index=False)

save_ptr()

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")