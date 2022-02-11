import pandas as pd
import numpy as np

def rank(df, group, new_col):
    df.sort_values(['Skill', group,'meet_sla','temp_rank', 'no_call', 'togo_bin', 'age']
               ,ascending=[True, True, True, False, False, False, False], inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(['Skill', group])[new_col].cumsum()
    return df

def stack_inventory(df, grouping='PhoneNumber'):
    # group by phone number or msid & rank highest value org
    df = rank(df, grouping  , 'overall_rank')

    # top overall per group = parent
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    df = rank(df, 'parent'  , 'Score')
    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

def split(df):
    df['OutreachID'] = df['OutreachID'].astype(str)
    f0 = df.Project_Type.isin(['UHC HEDIS','HEDIS']) # 'ACA-PhysicianCR'
    df['temp_rank'] = np.where(f0, 1, 0)

    split = 'CC_Cross_Reference'
    notmsid = df[df.Skill != split].copy()
    msid    = df[df.Skill == split].copy()

    scored      = stack_inventory(notmsid)
    msid_scored = stack_inventory(msid, grouping='mastersiteID')

    dubs = scored.append(msid_scored)
    unique = dubs.drop_duplicates(['OutreachID']).reset_index(drop= True)
    ### Piped ORGs attached to phone numbers
    return unique
        
def split_drop_score(df):
    ### Sort Order and drop Dups
    return split(df)