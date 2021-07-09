from datetime import date, timedelta, datetime
import holidays
import pandas as pd
import numpy as np
import time
today = date.today() #-timedelta(days=1)
FivDay = today + timedelta(days=7)
### Get Next Business day
ONE_DAY = timedelta(days=1)
HOLIDAYS_US = holidays.US()

def next_business_day(start):
    next_day = start + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day += ONE_DAY
    return next_day
def Next_N_BD(start, N):
    B10 = []
    seen = set(B10)
    i = 0

    while len(B10) < N:
        def test(day):
            d = start + timedelta(days=day)
            return next_business_day(d)
        item = test(i).strftime("%m/%d/%y")
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i += 1
    return B10
# print(Next_N_BD(today, 10))
def daily_piv(df):
    # df_piv = df.drop_duplicates('PhoneNumber')
    u = df.pivot_table(index =['Daily_Groups','Daily_Priority'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'])
    return print(u.reset_index().sort_values('Daily_Priority'))
def map_piv(df):
    # df_piv = df.drop_duplicates('PhoneNumber')
    u = df.pivot_table(index =['Daily_Groups'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'])
    return print(u)
# print(Next_N_BD(today, 10))

import os
import sys

def newPath(Parent, Look):
    absolutepath = os.path.abspath(__file__)
    fileDirectory = os.path.dirname(absolutepath)
    parentDirectory = os.path.dirname(fileDirectory)
    newPath = os.path.join(parentDirectory, str(Parent) + '\\'+ str(Look) +'\\')
    return newPath
# newPath('dump','')

def date_list_split(ls, numSplit):
    splits = np.array_split(ls, numSplit)
    return splits

# num1 , num2 = date_list_split(Next_N_BD(today, 10), 2)
# print(num2)
