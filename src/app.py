from sqlalchemy import null, values
import streamlit as st
import pandas as pd
from pipeline.business_prioirty import ciox_busines_lines, Business_Line, write_json
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
# load_file
extract, filename = extract_file_name('y')
load = compressed_files(filename, path=extract, sep="|").rename(columns=lambda x: x.replace(" ", "_"))
# saved user input
inputs = ciox_busines_lines()
# get skills & projects apart of the file
ls_skill = [line.skill for line in inputs]

columns_for_input = ["Audit_Type", "Project_Type", "Outreach_Status"]
unique_items = {item:load[item].unique() for item in columns_for_input}
columns = list(load.columns)

add_selectbox = st.sidebar.selectbox(
    "Pick a Custom Skill to custmize",
    ls_skill
)
    
# select skill that need to be changed
for line in inputs:
    if line.skill == add_selectbox: select_skill = line

if st.sidebar.radio(f"Remove {add_selectbox} Skill", ["No","Yes"]) == "Yes":
    inputs = [line for line in inputs if line.skill != add_selectbox]
# user input change
change = st.text_input("Change Skill Name: ", placeholder=select_skill.skill)

if change:
    select_skill.skill = change

def make_change(change_type, choice, c):
    with c:
        change = st.multiselect(change_type, choice, default=select_skill.filters[change_type])
        if change:
            select_skill.filters[change_type] = change
        return select_skill

# filters
with st.expander("Choose Filters"):
    cols = [c for c in st.columns(len(columns_for_input))]
    for unique, c in zip(unique_items.items(), cols):
        k, v = unique
        make_change(k, v, c)

conditions = {
    "Nan":".isna()",
    "Not Nan":".notna()",

    "equals": " == ",
    "not_equals": " != ",

    "contains":".str.contains ",
    "starts":".str.startswith ",
    "ends":".str.endswith ",

    "greater": " > ",
    "greater_than_or_equal": " >= ",
    "less": " < ",
    "less_than_or_equal": " <= ",
}

def conditions_key_wiget(key):
        
    col0, col1, col2, col3 = st.columns(4)
    with col0:
        if key != 1:
            if st.button("Remove", key=key): return None
    with col1:
        column = st.selectbox("Columns", ["Select"] + columns, key=key, )
    with col2:
        cond = st.selectbox("Condition", list(conditions.keys()), key=key)
    with col3:
        value = st.text_input("value",key=key)

    if column != "Select":
        if cond in ("Nan", "Not Nan"):
            return f"`{column}`{conditions[cond]}"
        else:
            return f"`{column}`{conditions[cond]} ({value})"

# If there are multiple conditions, we combine them together, with the
# given operator in the middle
OPERATOR_SIGNS = {"Or": " | ", "And": " & "}


def filters(filter, key):
    if filter != None and key <= 4:
        key += 1
        operate = st.selectbox("Operator", OPERATOR_SIGNS.keys() ,key=key)
        new_filter = conditions_key_wiget(key)

        if new_filter != None:
            combine = f"( {filter} ) {OPERATOR_SIGNS[operate]} ( {new_filter} )"
            return filters(combine, key)
        else:
            return filter


with st.expander("Sepecial Case"):
    f1 = conditions_key_wiget(1)
    filter_stack = filters(f1, 1)

st.write(filter_stack)


with st.expander("Choose Scroing, Order Matters!"):
    columns = columns + list(select_skill.scoring.keys())

    st.multiselect("Scroing", columns, default=list(select_skill.scoring.keys()))
    st.multiselect("Order: True=ASCD, False=DESC", len(select_skill.scoring.keys())*[True, False], default=list(select_skill.scoring.values()))

output = [line.as_dict() for line in inputs if line.skill != "New_skill"]

st.dataframe(load.query(filter_stack))

if st.sidebar.button(f"Submit Changes"):
    write_json(output)

