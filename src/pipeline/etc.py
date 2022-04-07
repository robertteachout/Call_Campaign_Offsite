import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np
from pandas.tseries.holiday import (AbstractHolidayCalendar, Holiday,
                                    USLaborDay, USMartinLutherKingJr,
                                    USMemorialDay, USPresidentsDay,
                                    USThanksgivingDay, nearest_workday)

startTime_1 = time.time()


def time_check(start, comment):
    executionTime_1 = round(time.time() - start, 2)
    print("-----------------------------------------------")
    print(comment + "\n" + "Time: " + str(executionTime_1))
    print("-----------------------------------------------")


def daily_piv(df):
    def piv(name):
        print("_-" * 10 + name + "-_" * 10)
        try:
            df1 = df[df[str(name)] == 1]
            print(
                df1.pivot_table(index="Skill", values="OutreachID", aggfunc=["count"])
            )
            work = {
                "CC_ChartFinder": 12000,
                "CC_Cross_Reference": 2400,
                # 'CC_Adhoc2':1000
            }
            # get top x in each skill
            filters = "|".join(
                [
                    f"(Skill == '{i}' & Score < {j} & parent == 1)"
                    for i, j in work.items()
                ]
            )
            table = df.query(filters)  # .PhoneNumber.tolist()
            # pivot
            called = table.pivot_table(
                index=[
                    "Skill",
                    "Project_Type"
                    # ,'no_call'
                ],
                values="OutreachID",
                aggfunc=["count"],
                margins=True,
            )  # .sort_values([('count','OutreachID')])
            called.to_clipboard()
            print(called)
        except:
            print(f"{name}: Null")

    piv("parent")


def date_list_split(ls, numSplit):
    splits = np.array_split(ls, numSplit)
    return splits


### CIOX Business Calender


class CioxHoliday(AbstractHolidayCalendar):
    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        USMemorialDay,
        Holiday("USIndependenceDay", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


ciox_holidays = CioxHoliday()
today = date.today()
ONE_DAY = timedelta(days=1)


def next_business_day(start):
    next_day = start + ONE_DAY
    holidays = ciox_holidays.holidays(today, today + timedelta(days=1 * 365)).values
    while next_day.weekday() >= 5 or next_day in holidays:
        next_day += ONE_DAY
    return next_day


def last_business_day(start):
    next_day = start - ONE_DAY
    holidays = ciox_holidays.holidays(today, today + timedelta(days=1 * 365)).values
    while next_day.weekday() >= 5 or next_day in holidays:
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


@dataclass
class Business_Days:
    date_format: str = "%Y-%m-%d"
    yesterday: datetime = x_Bus_Day_ago(1)
    yesterday_str: str = x_Bus_Day_ago(1).strftime(date_format)
    today: datetime = date.today()
    today_str: str = date.today().strftime(date_format)
    now: datetime = time.time()
    tomorrow: datetime = next_business_day(today)
    tomorrow_str: str = next_business_day(today).strftime(date_format)
