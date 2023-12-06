import streamlit as st
from mitosheet.streamlit.v1 import spreadsheet
from utils import process_management_fn
import pandas as pd
from datetime import datetime
from utils import API, Grafana_Queries

st.set_page_config(layout='wide')

# base_url = "http://117.2.164.10:50082/"
# username = "admin"
# password = "admin"

base_url = "https://lap.rainscales.com/"
username = "test"
password = "test"

st.title('LAP Statistics')

selected_tab = st.sidebar.selectbox('Select tabs:', ['Grafana Events', 'Job info by tasks', 'Performance'])

from_date = st.sidebar.date_input('From Date', pd.to_datetime('2023-01-01'))
to_date = st.sidebar.date_input('To Date', datetime.today())
skip = st.sidebar.number_input('Skip', value=0)
limit = st.sidebar.number_input('Limit', value=10)
sort = st.sidebar.selectbox("Sort order (by task)", ["asc", "desc"])

from_date = from_date.strftime("%Y-%m-%d")
to_date = to_date.strftime("%Y-%m-%d")

apis = API(base_url, username, password, from_date, to_date)
grafana = Grafana_Queries(base_url, username, password, from_date, to_date)
orgs = apis.get_orgs_ids()
orgs_names = list(map(lambda x: x[1], orgs))
orgs_ids = list(map(lambda x: x[0], orgs))

if 'Grafana Events' in selected_tab:
    select_orgs = st.selectbox("Select organization name", orgs_names)
    tasks = apis.get_task_ids(org_name = select_orgs)
    tasks_id = list(map(lambda x: x[0], tasks))
    tasks_names = list(map(lambda x: x[1], tasks))
    select_tasks = st.selectbox("Select task name", sorted(tasks_names))
    df = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(select_orgs)], tasks_id[tasks_names.index(select_tasks)]))
    spreadsheet(df)

elif 'Job info by tasks' in selected_tab:
    select_orgs = st.selectbox("Select organization name", orgs_names)
    tasks = apis.get_task_ids(org_name = select_orgs)
    tasks_id = list(map(lambda x: x[0], tasks))
    tasks_names = list(map(lambda x: x[1], tasks))
    select_tasks = st.selectbox("Select task name", sorted(tasks_names))
    df = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(select_orgs)], tasks_id[tasks_names.index(select_tasks)]))
    data, stat = apis.get_response(df, skip, limit, sort = sort, org_name = select_orgs, task_name = select_tasks)

    data_df = pd.DataFrame(data)

    spreadsheet(data_df)

    st.write(f"### Summary")
    st.write("Frame total:", stat[0])
    st.write("Frame completed:", stat[1])
    st.write("Object completed:", stat[2])
    st.write("Remaining frame:", stat[3])

else:
    select_orgs = st.selectbox("Select organization name", orgs_names)
    tasks = apis.get_task_ids(org_name = select_orgs)
    tasks_id = list(map(lambda x: x[0], tasks))
    tasks_names = list(map(lambda x: x[1], tasks))
    select_tasks = st.selectbox("Select task name", sorted(tasks_names))
    df = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(select_orgs)], tasks_id[tasks_names.index(select_tasks)]))
    data, _ = apis.get_response(df, skip, limit, sort = sort, org_name = select_orgs, task_name = select_tasks)

    data_df = pd.DataFrame(data)

    worker_df = data_df[["job_id", "user (anno)", "worker name (anno)", "team", "frame (total)", "object"]]
    review_df = data_df[["job_id", "user (review)", "worker name (review)", "team", "frame (reviewed)", "object"]]

    worker_df = worker_df[worker_df['user (anno)'].notna()]
    worker_df.rename(columns={"user (anno)": "user", "worker name (anno)": "worker name", "frame (total)" : "frame"}, inplace=True)
    review_df = review_df[review_df['user (review)'].notna()]
    review_df.rename(columns={"user (review)": "user", "worker name (review)": "worker name", "frame (reviewed)" : "frame"}, inplace=True)

    st.write(f'### Role: Worker')

    spreadsheet(apis.get_performance(worker_df))

    st.write(f'### Role: Reviewer')

    spreadsheet(apis.get_performance(review_df, True), key = "a")