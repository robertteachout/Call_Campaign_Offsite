import pandas as pd

### Create static two week sprint ###
def static_schedule(B10):
    dt = pd.DataFrame(
            {'startdate'    :B10,
                'Number'    :range(10)} )
    dt['startdate'] = pd.to_datetime(dt['startdate']) 
    return dt

def check_day(start, nbd):
    try:
        day = start[start['startdate'] == nbd]['Number'].to_list()[0]
        Master_List = 0
    except IndexError:
        day = 1
        Master_List = 1
    return day, Master_List

def daily_maping(df, assignment, tomorrow_str):
    ### get full list two two date to one number and cut down to future date and remove dubs
    assignment['PhoneNumber'] = assignment['PhoneNumber'].astype(str).str[:10]
    
    assignment['Daily_Groups'] = pd.to_datetime(assignment['Daily_Groups'], format='%Y-%m-%d')
    start = assignment[assignment['Daily_Groups'] >= tomorrow_str]
    end =  assignment[assignment['Daily_Groups'] < tomorrow_str]
    assignment = start.append(end).drop_duplicates(subset='PhoneNumber').sort_values('Daily_Groups').reset_index(drop=True)
    assignment['Daily_Groups'] = pd.to_datetime(assignment['Daily_Groups'], format='%Y-%m-%d').dt.date

    mapping = dict(zip(assignment.PhoneNumber, assignment.Daily_Groups))
    mapped = df
    mapped['PhoneNumber'] = mapped['PhoneNumber'].astype(str).str[:10]
    mapped.Daily_Groups = mapped.PhoneNumber.map(mapping).fillna(0)
    names = list(assignment['Daily_Groups'].unique())
    return mapped, names

    ## Sprint Schedulual Day
def map_priotiy(df, Day, names):
    Sprint = len(names)
    ### Map and Sort
    Sprint_schedule = list(range(0,Sprint))
    Category = names
    Sprint_schedule = Sprint_schedule[-Day:] + Sprint_schedule
    Daily_sort = dict(zip(Category,Sprint_schedule))
    df['Daily_Priority'] = df['Daily_Groups'].map(Daily_sort).fillna(Sprint_schedule[-1])
    return df

### Create file with assigned categories to ORG
def Assign_Map(df,B10,num1,num2):
    skills = df['Skill'].unique()
    df_clean = df.drop_duplicates('PhoneNumber')
    df_key = pd.DataFrame()
    def assign_skill(sk, j, BusDay):
        df_skill = df_clean[df_clean['Skill'] == sk].reset_index(drop = True)
        if j == 5:
            df_skill = df_skill[df_skill['audit_sort'] <= 2].reset_index(drop = True)
        else:
            df_skill = df_skill[df_skill['audit_sort'] > 2].reset_index(drop = True)
        #### INPUT BY DAY ####
        Sprint = j
        ######################
        df_len = len(df_skill)
        group_size = df_len // Sprint 
        ## What day for what number ##
        listDay = BusDay * group_size
        listDay.sort()
        ## Create Same len list of letters as len of df
        Daily_Priority = pd.DataFrame(listDay, columns=['Daily_Groups'])
        add_back = df_len - len(Daily_Priority)
        Daily_Priority = Daily_Priority.append(Daily_Priority.iloc[[-1]*add_back]).reset_index(drop=True)
        df_skill['Daily_Groups'] = Daily_Priority['Daily_Groups']
        return df_skill[['PhoneNumber', 'Skill','Daily_Groups']]
    def assign_audit(sk):
        D5_1 = assign_skill(sk, 5, list(num1))
        D5_2 = assign_skill(sk, 5, list(num2))
        D10  = assign_skill(sk, 10, B10)
        final = D5_1.append(D5_2).append(D10)
        return final
    ## Add together all skills with uniquely broken out sprints
    for i in skills:
            df_key = df_key.append(assign_audit(i))
    df_key['NewID'] = 0
    return df_key