# from FileLoad import Final_Load
import pandas as pd
import numpy as np

# df = Final_Load()

def Load_Cluster():
    path = "C:\\Users\\ARoethe\\OneDrive - CIOX Health\\Aaron\\Projects\\Call Campaign Automation\\Table_Drops\\"
    Cluster = pd.read_csv(path + "Cluster.csv", sep=',', error_bad_lines=False, engine="python")
    Cluster = Cluster.join(pd.get_dummies(Cluster['Clusters']))
    return Cluster

def sort(i):
    df = Load_Cluster()
    df0 = df[df[i] == 1]['Site Clean Id']
    return df0

def Cluster(df, Add_Cluster):
    df_local = df
    filter0 = df_local['Site Clean Id'].isin(sort(Add_Cluster).squeeze())
    df_local['Cluster'] = np.where(filter0, Add_Cluster, df_local['Cluster'])
    return df_local

def Clusters(df):
    f = Cluster(df,0)
    for i in range(1,11):
        f = Cluster(f,i)
    return f

# print(Clusters(df).groupby('Cluster')['Cluster'].count())

if __name__ == "__main__":
    print("Clusters")