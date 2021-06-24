import pandas as pd
import pyodbc
import sys

# print('Python: ' + sys.version.split('|')[0])
# print('Pandas: ' + pd.__version__)
# print('pyODBC: ' + pyodbc.version)


DB = {'servername': 'EUS1PCFSNAPDB01',
      'database': 'DWWorking'}

# create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')

# query db
sql = """

SELECT [Provider_num]
      ,[Cluster]
      ,[FactChart_OutreachId]
FROM [DWWorking].[dbo].[2019_2020_cluster]
WHERE [FactChart_OutreachId] IS NOT NULL

"""
def Cluster_Query():
    df = pd.read_sql(sql, conn)
    return df
print(Cluster_Query())