import pandas as pd
import pyodbc
import sys
import query_lc_org_search
import query_reschedule
from etc_function import x_Bus_Day_ago

def Query(database, sql, query_name):
      servername =  'EUS1PCFSNAPDB01'
      # create the connection
      try:
            conn = pyodbc.connect(f"""DRIVER={{SQL Server}};SERVER={servername};DATABASE={database};Trusted_Connection=yes""") 
      except pyodbc.OperationalError:
            print("""Not connected to server""")
            sys.exit(1)
      print('''Connected to Server \t {}'''.format(query_name))
      df = pd.read_sql(sql, conn)
      return df

def lc_search(CF, NIC):
      return Query('DWWorking', query_lc_org_search.sql(x_Bus_Day_ago(CF), x_Bus_Day_ago(NIC),x_Bus_Day_ago(1)), 'Last Call Check')
      
def reSchedule():
      return Query('DWWorking', query_reschedule.sql(), 'Add reschedules')

if __name__ == "__main__":
      df = lc_search(1, 5)
      print(df)