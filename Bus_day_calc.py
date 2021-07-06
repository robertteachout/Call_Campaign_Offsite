from datetime import date, timedelta, datetime
import holidays
import time
today = date.today() #-timedelta(days=1)

### Get Next Business day
ONE_DAY = timedelta(days=1)
HOLIDAYS_US = holidays.US()

def next_business_day(start):
    next_day = start + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day += ONE_DAY
    return next_day
def Next_N_BD():
    B10 = []
    seen = set(B10)
    i = 0

    while len(B10) < 10:
        def test(day):
            d = today + timedelta(days=day)
            return next_business_day(d)
        item = test(i).strftime("%m/%d")
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i += 1
    return B10



def daily_piv(df):
    df_piv = df.drop_duplicates('PhoneNumber')
    u = df_piv.pivot_table(index =['Daily_Groups','Daily_Priority'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'])
    return print(u.reset_index().sort_values('Daily_Priority'))
def map_piv(df):
    df_piv = df.drop_duplicates('PhoneNumber')
    u = df_piv.pivot_table(index =['Daily_Groups'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'])
    return print(u)
# print(Next_N_BD())