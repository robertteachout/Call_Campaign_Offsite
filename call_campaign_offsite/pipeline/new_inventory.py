import pandas as pd
from datetime import date
import numpy as np

today = date.today()

def NewID_sprint_load_balance(df,tomorrow_str,remaining_bd):
    f1 = df['Daily_Groups'] == 0
    df['NewID'] = np.where(f1, 1, 0)
    df['OutreachID'] = df['OutreachID'].astype(int) ## flag
    df_new = df[df['NewID'] == 1].reset_index(drop= True)
    if len(df_new) <= 10:
        df_new['Daily_Groups'] = tomorrow_str #tomorrow.strftime('%Y-%m-%d')
        p = df_new.append(df).drop_duplicates(['OutreachID']).reset_index(drop= True)
        return p
    ### split total NewIds into groups ###
    BusDay = remaining_bd
    group_size = ( len(df_new) // len(BusDay) ) * BusDay
    Daily_Priority = pd.DataFrame(group_size, columns=['Daily_Groups'])
    add_back = len(df_new) - len(Daily_Priority)
    Daily_Priority = Daily_Priority.append(Daily_Priority.iloc[[-1]*add_back]).reset_index(drop=True)
    df_new['Daily_Groups']  = Daily_Priority['Daily_Groups'].reset_index(drop=True)
    p = df_new.append(df).drop_duplicates(['OutreachID']).reset_index(drop= True)
    p['Daily_Groups'] = pd.to_datetime(p['Daily_Groups']).dt.date
    return p

def append_new_inventory(df):
    if 'NewID' in df.columns:
        NewID = df[df['NewID'] == 1]
        NewID = NewID[['PhoneNumber', 'Skill', 'Daily_Groups', 'NewID']].reset_index(drop=True)
        NewID['Daily_Groups'] = pd.to_datetime(NewID['Daily_Groups']).dt.strftime('%Y-%m-%d')
        return NewID
    else:
        print("check newid columm")
    