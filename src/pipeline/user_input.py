import numpy as np
from .business_prioirty import ciox_busines_lines, Business_Line
from .utils import query_df, join_tables

business = ciox_busines_lines()

# json parse input filters
def json_to_str_filters(dict_filter):
    def filter_types(key, value):
        return ' & '.join([f"{item} == 1" for item in value]) \
        if key == "equal_1" \
        else f"{key} in {value}"
    
    ls_filters = [filter_types(k,v) 
                    for k,v in dict_filter.items() 
                    if v != ["default"]]

    return " & ".join(ls_filters)
    
def load_custom_skills(df, custom_skill:Business_Line):
    for custom_skill in business:
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
        print(f"Check Skill: {skill} | validate columns: {columns)}")
        raise SystemExit

def custom_skills(table, business:list[Business_Line]):
    skill = df[df.Skill == skill].copy()
    scored_skill = rank(skill, "Score", ["Skill", "parent"], skill_rank)
    return join_tables(scored_skill, df)
    for line in business:
        table = score_skill(table, line.skill, line.scoring)
    return table

def create_score_column(df, name):
    f1 = df.Project_Type == name
    df[name] = np.where(f1, 1, 0)
    return df

def custom()
    for skill in business:
        if skill.new_columns != ["default"]:
            for name in skill.new_columns:
                unique = create_score_column(unique, name)

    ### skills that need special treatment
    if business != None:
        score_business = [line for line in business 
                            if list(line.scoring.keys()) != ["default"]]
        unique = custom_skills(unique, score_business)

    ### Piped ORGs attached to phone numbers
    f0 = unique.Project_Type.isin(["Chart Sync"])  # 'ACA-PhysicianCR'
    unique["Score"] = np.where(f0, 1000000, unique.Score)
    return unique