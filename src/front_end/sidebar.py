import streamlit as st


def add_wegits(ls_skill, inputs):
    add_selectbox = st.sidebar.selectbox(
        "Pick a Custom Skill to custmize",
        ls_skill
    )
    for line in inputs:
        if line.skill == add_selectbox: select_skill = line

    if st.sidebar.radio(f"Remove {add_selectbox} Skill", ["No","Yes"]) == "Yes":
        inputs = [line for line in inputs if line.skill != add_selectbox]

    return select_skill, inputs
