import streamlit as st

# Set page title
st.title("ỨNG DỤNG MODERN DATA STACK XÂY DỰNG LAKEHOUSE CHO BRAZILIAN E-COMMERCE (OLIST)")

# Description
st.write("Welcome to the Streamlit App! Use the buttons below to navigate to different tools.")

# Buttons for navigation
if st.button("Open Dagster"):
    st.write("Redirecting to Dagster...")
    st.markdown("[Go to Dagster](http://localhost:3001)", unsafe_allow_html=True)

if st.button("Open Metabase"):
    st.write("Redirecting to Metabase...")
    st.markdown("[Go to Metabase](http://localhost:3000)", unsafe_allow_html=True)

if st.button("Open Jupyter Notebook"):
    st.write("Redirecting to Jupyter Notebook...")
    st.markdown("[Go to Notebook](http://127.0.0.1:8888/lab/workspaces/auto-K/tree/work)", unsafe_allow_html=True)

if st.button("Open MinIO"):
    st.write("Redirecting to MinIO...")
    st.markdown("[Go to MinIO](http://localhost:9001)", unsafe_allow_html=True)