import numpy as np

from .utils import query_df


# json parse input filters
def json_to_str_filters(dict_filter):
    def filter_types(key, value):
        if key == 'general':
            return " & ".join(value).replace("& |", "|")
        else:
            return f"{key} in {value}"

    ls_filters = [filter_types(k,v) 
                    for k,v in dict_filter.items() 
                    if v != []]

    return " & ".join(ls_filters)
    
def load_custom_skills(df, user_input):
    for custom_skill in user_input:
        filters = json_to_str_filters(custom_skill.filters)
        
        if filters:
            df.Skill = np.where(
                query_df(df, filters), 
                custom_skill.skill, 
                df.Skill)    
    return df

# score new skills
def check_custom_score(df, skill, column_rankers):
    # check if skill is active & ranking columns exists
    columns = set(column_rankers)
    if df.Skill.isin([skill]).any() and columns.issubset(df.columns):
        pass        
    else:
        print(f"Check Skill: {skill} | validate columns: {columns}")
        raise SystemExit

def create_score_column(df, name):
    f1 = df.Project_Type == name
    df[name] = np.where(f1, 1, 0)
    return df
