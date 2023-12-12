import streamlit as st
from mitosheet.streamlit.v1 import spreadsheet
import pandas as pd
from datetime import datetime
from clone_data import clone_data, load_data

st.set_page_config(layout='wide')

base_url = "https://lap.rainscales.com/"
username = "test"
password = "test"

drop = ["Org", "Prj", "Tsk"]

st.title('LAP Statistics')

# SIDEBAR
selected_tab = st.sidebar.selectbox('Select tabs:', ['Progress Management', 'Performance', 'Grafana Events'])
from_date = st.sidebar.date_input('From Date', pd.to_datetime('2023-01-01'))
to_date = st.sidebar.date_input('To Date', datetime.today())
from_date = from_date.strftime("%Y-%m-%d")
to_date = to_date.strftime("%Y-%m-%d")

st.sidebar.markdown("")
st.sidebar.markdown("")
with st.sidebar:
    with st.spinner("Please Wait ..."):
        if st.sidebar.button("Refetch Data"):
            clone_data(from_date, to_date)

# LOAD DATA
grafana_events, progress, reviewer, worker, stats, selections = load_data()

orgs = selections['Org'].unique().tolist()
select_orgs = st.selectbox("Select organization name", sorted(orgs))

projects = selections[selections["Org"] == select_orgs]["Prj"].unique().tolist()
select_projects = st.selectbox("Select project name", sorted(projects))

tasks = selections[(selections['Org'] == select_orgs) & (selections['Prj'] == select_projects)]['Tsk'].unique().tolist()
select_tasks = st.selectbox("Select task name", sorted(tasks))


if 'Progress Management' in selected_tab:

    stat = stats[(stats['Org'] == select_orgs) & (stats['Prj'] == select_projects) & (stats['Tsk'] == select_tasks)]

    st.write("### Summary")
    stat = stat.reset_index(drop = True)
    st.write(stat.drop(columns = drop))

    df = progress[(progress['Org'] == select_orgs) & (progress['Prj'] == select_projects) & (progress['Tsk'] == select_tasks)]

    spreadsheet(df.drop(columns = drop))

elif "Performance" in selected_tab:

    worker_df = worker[(worker['Org'] == select_orgs) & (worker['Prj'] == select_projects) & (worker['Tsk'] == select_tasks)]
    reviewer_df = reviewer[(reviewer['Org'] == select_orgs) & (reviewer['Prj'] == select_projects) & (reviewer['Tsk'] == select_tasks)]

    st.write(f'### Role: Worker')

    spreadsheet(worker_df.drop(columns = drop + ["Job"]))

    st.write(f'### Role: Reviewer')

    spreadsheet(reviewer_df.drop(columns = drop + ["Job"]), key = "a")

else:

    grafana_df = grafana_events[(grafana_events['Org'] == select_orgs) & (grafana_events['Prj'] == select_projects) & (grafana_events['Tsk'] == select_tasks)]
    spreadsheet(grafana_df.drop(columns = drop))