import pandas as pd
import os
import glob
import numpy as np
from datetime import date, timedelta, datetime

today = date.today()
F_today = today.strftime("%m%d")

F_today = str('Call_Campaign_v4_' + F_today +'*.txt')

Dpath = 'C:/Users/ARoethe/OneDrive - CIOX Health/Aaron/Projects/Call Campaign Automation/dump/Call_Campaign/'
for file in glob.glob(Dpath + F_today):
    print(file)
# print(today)
# print(F_today)

# Project Tracking v3.0 06.14.2021

# today = date.today()
# tomorrow = (today + timedelta(days = 3)).strftime("%m/%d/%Y")

# print(tomorrow.replace("/", "-"))
df0 = pd.read_csv(file, sep='|', error_bad_lines=False, engine="python")

df_dummy = pd.get_dummies(df0['Outreach Status'])

print(df0.join(df_dummy))

