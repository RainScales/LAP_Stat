import streamlit as st
from mitosheet.streamlit.v1 import spreadsheet
import pandas as pd
from datetime import datetime
from clone_data import clone_data, load_data
import pickle
import os

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

if os.path.exists(os.path.join("data", "last_update.pkl")):
    with open(os.path.join("data", "last_update.pkl"), 'rb') as f:
        last_update = pickle.load(f)
        if last_update < datetime.today().date():
            with st.sidebar:
                with st.spinner("Auto refreshing. Please Wait ..."):
                    clone_data(from_date, to_date)
                    with open(os.path.join("data", "last_update.pkl"), 'wb') as f:
                        pickle.dump(datetime.today().date(), f)

with st.sidebar:
    with st.spinner("Please Wait ..."):
        if st.sidebar.button("Refetch Data"):
            clone_data(from_date, to_date)
            with open(os.path.join("data", "last_update.pkl"), 'wb') as f:
                pickle.dump(datetime.today().date(), f)

# LOAD DATA
grafana_events, progress, performance, stats, selections = load_data()

orgs = selections['Org'].unique().tolist()
select_orgs = st.selectbox("Select organization name", sorted(orgs))

projects = selections[selections["Org"] == select_orgs]["Prj"].unique().tolist()
select_projects = st.selectbox("Select project name", sorted(projects))

tasks = selections[(selections['Org'] == select_orgs) & (selections['Prj'] == select_projects)]['Tsk'].unique().tolist()
select_tasks = st.selectbox("Select task name", sorted(tasks))


if 'Progress Management' in selected_tab:
    if stats.empty:
        st.write("### No data available")
    else:
        stat = stats[(stats['Org'] == select_orgs) & (stats['Prj'] == select_projects) & (stats['Tsk'] == select_tasks)]

        stat = stat.drop(columns = drop )

        st.write("### Worker and Reviewer Summary")
        stat_worker = stat[stat["Filter"] == "Worker"]
        stat_worker = stat_worker.drop(columns = "Filter")
        stat_worker = stat_worker.reset_index(drop = True)
        # st.write(stat_worker)

        stat_reviewer = stat[stat["Filter"] == "Reviewer"]
        stat_reviewer = stat_reviewer.drop(columns = "Filter")
        stat_reviewer = stat_reviewer.reset_index(drop = True)
        # st.write(stat_reviewer)

        st.write(f"<div style='display: flex; flex-wrap: wrap;'>"
         f"<div style='margin-right: 20px;'>{stat_worker.to_html(index = False)}</div>"
         f"<div>{stat_reviewer.to_html(index = False)}</div>"
         f"</div>", unsafe_allow_html=True)

        df = progress[(progress['Org'] == select_orgs) & (progress['Prj'] == select_projects) & (progress['Tsk'] == select_tasks)]

        spreadsheet(df.drop(columns = drop))

elif "Performance" in selected_tab:
    if performance.empty:
        st.write("### No data available")
    
    else:
        performance_df = performance[(performance['Org'] == select_orgs) & (performance['Prj'] == select_projects) & (performance['Tsk'] == select_tasks)]

        spreadsheet(performance_df.drop(columns = drop))

else:
    if grafana_events.empty:
        st.write("### No data available")

    else:
        grafana_df = grafana_events[(grafana_events['Org'] == select_orgs) & (grafana_events['Prj'] == select_projects) & (grafana_events['Tsk'] == select_tasks)]
        spreadsheet(grafana_df.drop(columns = drop))