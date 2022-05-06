import pandas as pd
import streamlit as st

from front_end import sidebar
from pipeline.business_prioirty import ciox_busines_lines, write_json
from pipeline.tables import compressed_files, extract_file_name
from pipeline.utils import Business_Days

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

select_skill, inputs = sidebar.add_wegits(ls_skill, inputs)

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
    "equals": " == ",
    "not_equals": " != ",

    "Nan":".isna()",
    "Not Nan":".notna()",

    "greater": " > ",
    "greater_than_or_equal": " >= ",
    "less": " < ",
    "less_than_or_equal": " <= ",

    "contains":".str.contains ",
    "starts":".str.startswith ",
    "ends":".str.endswith ",
}

def list_to_string(column, condition, value):
    if condition in ("Nan", "Not Nan"):
        filter = f"`{column}`{conditions[condition]}"
        return filter
    else:
        filter =  f"`{column}`{conditions[condition]} ({value})"
        return filter

def sort_selectbox(column, columns):
    for idx, item in enumerate(columns):
        if item == column:
            target = idx
    return target

def conditions_key_wiget(key):
    col0, col1, col2, col3 = st.columns(4)
    operator = None
    with col0:
        if key != 0:
            operator_wiget = st.selectbox("Operator", OPERATOR_SIGNS.keys(), key=key)
            operator = OPERATOR_SIGNS[operator_wiget]
    with col1:
        column = st.selectbox("Columns", columns, key=key )
    with col2:
        condition = st.selectbox("Condition", list(conditions.keys()), key=key)
    with col3:
        value = st.text_input("value",key=key)

    if column != "Select":
        return operator, list_to_string(column, condition, value)
    else:
        return None, None

# If there are multiple conditions, we combine them together, with the
# given operator in the middle
OPERATOR_SIGNS = {"Or": " | ", "And": " & "}


def filters(filter, key):
    if filter and key <= 4:
        key += 1
        operator, new_filter = conditions_key_wiget(key)
        if new_filter:
            combine = f"( {filter} ) {operator} ( {new_filter} )"
            return filters(combine, key)
        else:
            return filter

with st.expander("Sepecial Case"):
    _, f1 = conditions_key_wiget(0)
    filter_stack = filters(f1, 1)

    st.write(filter_stack)

orders = {True:"Ascending", False:"Descending"}

with st.expander("Choose Scroing, Order Matters!"):
    columns = columns + list(select_skill.scoring.keys())
    scoring = select_skill.scoring 
    # scoring["Select"] = True

    def score(key, column, order):
        col1, col2 = st.columns(2)
        idx = 0
        with col1:
            idx = sort_selectbox(column, columns)
            select_col = st.selectbox("Scroing", columns, key=key, index= idx)
        with col2:
            idx = sort_selectbox(orders, order)
            select_order = st.selectbox("Order", ["Ascending", "Descending"], index=idx, key=key)
        return select_col, select_order

    new_score = {}
    for idx, s in enumerate(scoring.items()):
        k , v = score(idx, *s)
        new_score[k] = v

    st.write(new_score)
    
output = [line.as_dict() for line in inputs if line.skill != "New_skill"]


if st.sidebar.button(f"Submit Changes"):
    write_json(output)

