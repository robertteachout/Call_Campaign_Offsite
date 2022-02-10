from datetime import date, timedelta, datetime
import holidays
import numpy as np
import time
startTime_1 = time.time()

def time_check(start, comment):
    executionTime_1 = round(time.time() - start, 2)
    print("-----------------------------------------------")
    print(comment + '\n' +'Time: ' + str(executionTime_1))
    print("-----------------------------------------------")

def daily_piv(df):
    def piv(name):
        print(f""" _-_-_-_-_-_-_-_-_-_- {name} -_-_-_-_-_-_-_-_-_-_-_ """)
        try:
            df1 = df[df[str(name)] == 1]
            print(df1.pivot_table(index ='Skill', values ='OutreachID', aggfunc = ['count']))
        except:
            print(f'{name}: Null')

    piv('Unique_Phone')
    
def date_list_split(ls, numSplit):
    splits = np.array_split(ls, numSplit)
    return splits

### CIOX Business Calender
today = date.today()
HOLIDAYS_US = holidays.US(years= today.year)
HOLIDAYS_CIOX = dict(zip(HOLIDAYS_US.values(), HOLIDAYS_US.keys()))
del_list = ("Washington\'s Birthday", 'Juneteenth National Independence Day','Columbus Day','Veterans Day')
for i in del_list:
    HOLIDAYS_CIOX.pop(i)

ONE_DAY = timedelta(days=1)

def next_business_day(start):
    next_day = start + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_CIOX.values():
        next_day += ONE_DAY
    return next_day

def last_business_day(start):
    next_day = start - ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_CIOX.values():
        next_day -= ONE_DAY
    return next_day

def x_Bus_Day_ago(N):
    B10 = []
    seen = set(B10)
    i = today

    while len(B10) < N:
        item = last_business_day(i)
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i -= timedelta(days=1)
    return B10[-1]

def Next_N_BD(start, N):
    B10 = []
    seen = set(B10)
    i = 0

    while len(B10) < N:
        def test(day):
            d = start + timedelta(days=day)
            return next_business_day(d)
        item = test(i).strftime("%Y-%m-%d")
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i += 1
    return B10
