import streamlit as st
import pandas as pd
import json
import log.log as log
from pipeline.business_prioirty import ciox_busines_lines, Business_Line
import pipeline.clean
import pipeline.score
import pipeline.skills
import pipeline.sprint_schedule
import server.connections
import server.insert
import server.queries.call_campaign_insert
import server.queries.MasterSiteId
import server.queries.optum_assigned
import server.queries.reschedule
from pipeline.utils import Business_Days, daily_piv, time_check, x_Bus_Day_ago
from pipeline.tables import (compressed_files, contact_counts,
                             extract_file_name, tables)
from server.insert import before_insert, clean_for_insert, sql_insert

Bus_day = Business_Days()

extract, filename = extract_file_name('y')
print(filename)
load = compressed_files(filename, path=extract, sep="|")

inputs = ciox_busines_lines()
new_skill = Business_Line()
inputs.append(new_skill)

ls_skill = [line.skill for line in inputs]
projects = load['Project Type'].unique()
columns  = load.columns


# with st.form("my_form"):
add_selectbox = st.sidebar.selectbox(
    "Pick a Custom Skill to custmize",
    ls_skill
)

selected_projects = st.multiselect("All Projects", projects)
selected_columns  = st.multiselect("All columns", columns)

# selected_skill = st.selectbox("Custom Skills", ls_skill)
def df_print(name):
    for i in inputs:
        if i.skill == name:
            st.text(json.dumps(i.as_dict(),indent=4))
            # st.metric(label="skill", value=i.skill)
            # st.metric(label="filters", value=str(i.filters))
            # st.metric(label="new_columns", value=str(i.new_columns))
            # st.metric(label="scoring", value=str(i.scoring))


            # st.dataframe( pd.DataFrame(i.as_dict()).drop('skill',axis=1) )

# df_print(add_selectbox)

def change_skill_name(inputs, name):
    for line in inputs:
        if line.skill == name:
            new_name = st.text_input("Change Skill Name: ")
            if new_name != '':
                line.skill = new_name
    return inputs

df_print(add_selectbox)
outputs = change_skill_name(inputs, add_selectbox)


