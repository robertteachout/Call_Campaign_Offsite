from typing import final

import pandas as pd
import streamlit as st

from pipeline.business_prioirty import (Business_Line, ciox_busines_lines,
                                        write_json)
from pipeline.tables import (compressed_files, contact_counts,
                             extract_file_name, tables)
from pipeline.utils import Business_Days, daily_piv, time_check, x_Bus_Day_ago

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
columns = ["Select"] + list(load.columns)

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

    "greater": " > ",
    "greater_than_or_equal": " >= ",
    "less": " < ",
    "less_than_or_equal": " <= ",

    "contains":".str.contains ",
    "starts":".str.startswith ",
    "ends":".str.endswith ",
}

def conditions_key_wiget(key):
    col0, col1, col2, col3 = st.columns(4)
    sign = "nan"
    with col0:
        if key != 0:
            operate = st.selectbox("Operator", OPERATOR_SIGNS.keys() ,key=key)
            sign = OPERATOR_SIGNS[operate]
    with col1:
        column = st.selectbox("Columns", columns, key=key, )
    with col2:
        cond = st.selectbox("Condition", list(conditions.keys()), key=key)
    with col3:
        value = st.text_input("value",key=key)

        # if isinstance(value, str): 
        #     value = f"'{value}'"

    if column != "Select":
        if cond in ("Nan", "Not Nan"):
            filter = f"`{column}`{conditions[cond]}"
            return sign, filter
        else:
            filter =  f"`{column}`{conditions[cond]} ({value})"
            return sign, filter
    else:
        return None, None

# If there are multiple conditions, we combine them together, with the
# given operator in the middle
OPERATOR_SIGNS = {"Or": " | ", "And": " & "}


def filters(filter, key):
    if filter and key <= 4:
        key += 1
        sign, new_filter = conditions_key_wiget(key)
        if new_filter:
            combine = f"( {filter} ) {sign} ( {new_filter} )"
            return filters(combine, key)
        else:
            return filter

def generate_widgets():
    for i in range(100):
        yield i

# weg = generate_widgets()
# def save_temp(a):
#     with open("file.txt", "w") as f:
#         f.truncate()
#         f.write(f"{a}")
# with open("file.txt", "r") as f:
#     a = f.readline()  # starts as a string
#     a = 0 if a == "" else int(a)  # check if its an empty string, otherwise should be able to cast using int()

# if st.button("add filter"):
#     st.write(a)
#     a += 1
# if st.button("remove filter"):
#     st.write(a)
#     a -= 1


with st.expander("Sepecial Case"):
    _, f1 = conditions_key_wiget(0)
    filter_stack = filters(f1, 1)
    # if filter_stack:
    #     # st.write(filter_stack)
    #     # st.dataframe(load.query(filter_stack))
    # else:
    #     st.dataframe(load)

orders = {True:"Ascending", False:"Descending"}

with st.expander("Choose Scroing, Order Matters!"):
    columns = columns + list(select_skill.scoring.keys())
    scoring = select_skill.scoring 
    # scoring["Select"] = True

    def score(key, column, order):
        col1, col2 = st.columns(2)
        res = 0
        with col1:
            for x, z in enumerate(columns):
                if z == column:
                    res = x
            select_col = st.selectbox("Scroing", columns, key=key, index=res)
        with col2:
            for x, z in enumerate(orders):
                if z == order:
                    res = x
            select_order = st.selectbox("Order", ["Ascending", "Descending"], index=res, key=key)
        return select_col, select_order

    new_score = {}
    for idx, s in enumerate(scoring.items()):
        k , v = score(idx, *s)
        new_score[k] = v

    st.write(new_score)
    
output = [line.as_dict() for line in inputs if line.skill != "New_skill"]


if st.sidebar.button(f"Submit Changes"):
    write_json(output)

