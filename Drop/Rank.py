import pandas as pd
import numpy as np

from FileLoad import Final_Load

def Retrival_Group(df, Group):
    Retrieval = df.loc[df['Retrieval Group'] ==  Group].copy()
    return Retrieval

def Last_Call(Retrieval, Days):
    Group = Retrieval.loc[Retrieval['Last Call'].notnull()].reset_index(drop=True).copy()
    # print(Group[['Age', 'Last Call']])
    DateDiff = Group.copy()
    # print(DateDiff[['Age', 'Last Call']])
    DateDiff['New'] = pd.to_datetime('today').date()
    DateDiff['DateDiff'] = DateDiff['New'] - DateDiff['Last Call']
    # print(DateDiff[['New','Last Call','DateDiff']].sort_values('DateDiff', ascending=False).head(50))

    Group['Age'] = (DateDiff['DateDiff']/np.timedelta64(1,'D')).astype(int)
    DateDiff['Age'] = (DateDiff['DateDiff']/np.timedelta64(1,'D')).astype(int)

    def Rank(Group_R):
        Group_R['Score'] = DateDiff['Age'].rank(method= 'first', ascending=False).copy()
        return Group_R
    def Rank_Zero_Charts(Group_R):
        Group_R['Age'] = 0
        Group_R['Score'] = Group_R['ToGoCharts'].rank(method= 'first', ascending=True).copy() #Charts that haven't been called with 1 chart are left behind
        return Group_R
    if Days == 0:
        Group0 = Retrieval[Retrieval['Last Call'].isnull()].copy()
        Group = Rank_Zero_Charts(Group0)
    elif Days == 5:
        Group1 = Group.loc[(Group['Age'] >= 0) & (Group['Age'] <= 5)].copy()
        Group = Rank(Group1)
    elif Days == 10:
        Group5 = Group.loc[(Group['Age'] >= 6) & (Group['Age'] <= 10)].copy()
        Group = Rank(Group5)
    elif Days == 15:
        Group10 = Group.loc[(Group['Age'] >= 11) & (Group['Age'] <= 15)].copy()
        Group = Rank(Group10)
    elif Days == 20:
        Group15 = Group.loc[(Group['Age'] >= 16) & (Group['Age'] <= 20)].copy()
        Group = Rank(Group15)
    elif Days == 21:
        Group20 = Group.loc[(Group['Age'] >= 21)].copy()
        Group = Rank(Group20)
    else:
        print("error")
    return Group
# print(Last_Call(Final_Load(), 0)[['Age','Last Call', 'ToGoCharts', 'Score']])

if __name__ == "__main__":
    print("Rank")