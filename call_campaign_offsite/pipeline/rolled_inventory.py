import pandas as pd
import numpy as np

def check_lc(df, lc_search, Day, tomorrow_str):
    if Day != 0:
        ### Add yesterdays daily group that was missed
        list_add = lc_search
        filter0 = df['OutreachID'].isin(list_add['OutreachID'].squeeze())
        df['Daily_Groups'] = np.where(filter0, tomorrow_str, df['Daily_Groups'])
        df['rolled'] = np.where(filter0, 1, 0)
    else:
        list_add = pd.DataFrame()
    return df, list_add