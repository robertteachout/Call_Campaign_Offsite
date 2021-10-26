import pandas as pd
import pyodbc
import sys
import query_lc_org_search
import query_reschedule

def Query(database, sql, query_name):
      DB = {'servername': 'EUS1PCFSNAPDB01',
            'database': database}
      # create the connection
      try:
            conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes') 
      except pyodbc.OperationalError:
            print("""Not connected to server""")
            sys.exit(1)
      print('''Connected to Server \t {}'''.format(query_name))
      df = pd.read_sql(sql, conn)
      return df

def lc_org_search():
      return Query('DWWorking', query_lc_org_search.sql(), 'Last Call Check')
      
def reSchedule():
      return Query('DWWorking', query_reschedule.sql(), 'Add reschedules')