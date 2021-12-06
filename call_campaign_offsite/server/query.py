import pandas as pd
import pyodbc
import sys

def query(database, sql, query_name):
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
