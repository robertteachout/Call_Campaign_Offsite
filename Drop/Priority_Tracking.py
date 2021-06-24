import pandas as pd
import numpy as np
import glob
from datetime import date, timedelta, datetime

import time
import csv
startTime_1 = time.time()
csv.field_size_limit(1000)

today = date.today()
F_today = today.strftime("%m.%d.%Y")
F_today = str('Project Tracking v3.0 ' + F_today +'.xlsx')

Dpath = 'C:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/Call Campaign Automation/dump/Project Tracking/'
for file in glob.glob(Dpath + F_today):
    filename = file

def Load_PR(Sheet):
    df = pd.read_excel(filename, sheet_name=Sheet, header=1)
    df.dropna(how='all', axis=1, inplace=True)
    df = df[(df['Project ID'] != 'Total') & (df['Project Year'] != 'Total')]
    df = df[(df['Project Health'] == 'Behind - Available') | (df['Project Health'] == 'Ahead - Available')]
    df = df[df['Available'] > 0]
    return df

def Agg(sheet):
    df = Load_PR(sheet)
    df = df.groupby(['Project Type','Client Project','Project ID']).agg({'Pacing %': 'mean', 'Charts to Target': 'sum'})
    df = df.groupby(['Project Type','Client Project']).agg({'Pacing %': 'mean', 'Charts to Target': 'sum'})
    df = df.groupby(['Project Type']).agg({'Pacing %': 'mean', 'Charts to Target': 'sum'}).sort_values(by= 'Pacing %', ascending=True)
    df['Pacing %'] = df['Pacing %'].round(3)
    df['Charts to Target'] = df['Charts to Target'].round(0)

    return df.reset_index()
 
def Project_Rank(sheet):
    if  sheet == 'HIH - Other':
        Summary = Agg('Project Tracking Summary')
    else:
        Summary = Agg(sheet)
    return Summary

# test = Project_Rank('HIH - Other')

# print(test)#[test['Client Project'] == 'WellCare MRA'].head(50))#,'Forecasted Netcharts'))


executionTime_1 = (time.time() - startTime_1)
print("-----------------------------------------------")
print('Time: ' + str(executionTime_1))
print("-----------------------------------------------")

if __name__ == "__main__":
    print("File will load")