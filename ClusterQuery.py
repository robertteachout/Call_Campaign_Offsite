# from FileLoad import Final_Load
import pandas as pd
import numpy as np
from dbo import Cluster_Query
from FileLoad import Final_Load

# df = Final_Load()

def Load_Cluster():
    Clusters = Cluster_Query()
    Cluster = Clusters.join(pd.get_dummies(Clusters['Cluster']))
    return Cluster
# print(Load_Cluster())
def sort(i):
    df = Load_Cluster()
    df0 = df[df[str(i)] == 1]['FactChart_OutreachId']
    return df0
# print(sort(1))

def Cluster(df, Add_Cluster):
    df_local = df
    filter0 = df_local['OutreachID'].isin(sort(Add_Cluster).squeeze())
    df_local['Cluster'] = np.where(filter0, Add_Cluster, df_local['Cluster'])
    return df_local

def Clusters(df):
    f = Cluster(df,1)
    for i in range(2,11):
        f = Cluster(f,i)
    return f
df = Clusters(Final_Load())
print(df.groupby('Cluster')['OutreachID'].count())
# print(Clusters(df).groupby('Cluster')['Cluster'].count())

if __name__ == "__main__":
    print("Clusters")