from pathlib import Path
from glob import glob

import pandas as pd
from src.pipeline.tables import tables

def work(i):
    df = tables('pull', 'na', i, '')
    df['PhoneNumber'] = pd.to_numeric(df['PhoneNumber'], downcast='integer')#.astype(str).str[:-2]
    col = df[['PhoneNumber']]

    col_clean = col.drop_duplicates(
        subset= ['PhoneNumber'],
        keep='last').reset_index(drop=True)
    return set(col_clean['PhoneNumber'])

def new_func(batch):
    z = Path(f'data/batch/batch_{batch}')
    b = list(z.glob('*.csv'))
    if not b:
        print('Check Path, list empty')
        
    final = pd.DataFrame(columns=['Date', 'count'])
    for i in range(0, len(b)):
        f = pd.DataFrame(columns=['Date', 'count'])
        ls = b[i:]
        jj = 0 
        for j in ls:
            col_clean = work(j)
            if jj == 0:
                # first = col_clean
                dub = col_clean
            else:
                dub = dub.intersection(col_clean)
                # dub = first.intersection(col_clean)

            st = (str(j).split('\\')[-1][:-4])
            jj += 1
            f = f.append({'Date':st,'count':len(dub)},ignore_index = True)
        name = str(ls[0]).split('\\')[-1][:-4]
        f = f.rename(columns={'count':name}).set_index(['Date'])
        if i == 0:
            final = f
        else:
            final = final.join(f)
    return final

batch = 8

final = new_func(batch)
# final.pct_change().cumsum().round(4).T.fillna('') *100
final.T.fillna('')

# final.T.fillna('').to_csv(f'fallout.csv')



