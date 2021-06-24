import pandas as pd
import os
import numpy as np
from datetime import date, timedelta, datetime

def Group(df, RM):
        dfg = df[df['Retrieval Group'] == RM].groupby('Outreach Status').agg({'OutreachID':'count','ToGoCharts':'sum'})
        dfg = dfg.reset_index()
        dfg['RM'] = RM
        return dfg
def Final(df, date):
    d1 = Group(df,'Onsite')
    d2 = Group(df,'Offsite')    
    d3 = Group(df,'EMR Remote')    
    d4 = Group(df,'HIH - Other')
    fd = d1.append(d2).append(d3).append(d4)
    fd = fd.groupby(['RM','Outreach Status']).agg({'OutreachID':'sum','ToGoCharts':'sum'}).reset_index()
    fd['Date'] = date
    return fd

test = pd.DataFrame(columns = ['RM', 'Outreach Status', 'OutreachID', 'ToGoCharts', 'Date'])
path = 'C:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/Call Campaign Automation/dump/Group_Rank/'
files = os.listdir(path)

for i in files:
    df = pd.read_csv(path + i,sep=',',low_memory=False)
    test = test.append(Final(df, i))
test['Date'] = test['Date'].str.slice(start=0, stop=10).copy()   
print(test)
# path = 'C:/Users/roeth/OneDrive - CIOX Health/Aaron/Projects/Call Campaign Automation/Table_Drops/'
# test.to_csv(path + 'Group Outreach Status.csv', index=False)