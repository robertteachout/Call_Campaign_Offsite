# from FileLoad import Final_Load
import pandas as pd
import numpy as np
import string

# df = Final_Load()
def Load_Assignment():
    path = "C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\"
    Cluster = pd.read_csv(path + "Assignment_Map.csv", sep=',', error_bad_lines=False, engine="python")
    Cluster = Cluster.join(pd.get_dummies(Cluster['Daily_Groups']))
    return Cluster

# print(Load_Assignment().head(20))
def sort(i):
    df = Load_Assignment()
    df0 = df[df[i] == 1]['PhoneNumber']
    return df0

def Cluster(df, Add_Cluster):
    df_local = df
    filter0 = df_local['PhoneNumber'].isin(sort(Add_Cluster).squeeze())
    df_local['Daily_Groups'] = np.where(filter0, Add_Cluster, df_local['Daily_Groups'])
    return df_local

def Daily_Maping(df):
    f = df
    Sprint = Load_Assignment().nunique()['Daily_Groups']
    letters = list(string.ascii_uppercase)[:Sprint]
    for i in letters:
        f = Cluster(f,i)
    f['Daily_Groups'] = f['Daily_Groups'].fillna(method='ffill')
    return f

# print(Daily_Maping(df).groupby('Daily_Groups')['PhoneNumber'].count())
# print(Daily_Maping(df)['Daily_Groups'])

if __name__ == "__main__":
    print("Clusters")