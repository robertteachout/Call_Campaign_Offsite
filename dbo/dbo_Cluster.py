import pandas as pd
import time
from datetime import date, timedelta, datetime

from dbo_Query import Query
startTime_1 = time.time()


sql = """

SELECT [Provider_num]
      ,[Cluster]
      ,[FactChart_OutreachId]
FROM [DWWorking].[dbo].[2019_2020_cluster]
WHERE [FactChart_OutreachId] IS NOT NULL

"""
def q_cluster():
      return Query('DWWorking', sql)

executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")