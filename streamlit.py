import streamlit as st
from mitosheet.streamlit.v1 import spreadsheet
from stat_lap import process_management_fn
import pandas as pd
from datetime import datetime
from stat_lap import API, Grafana_Queries

st.set_page_config(layout='wide')

base_url = "http://117.2.164.10:50082/"
username = "admin"
password = "admin"

st.title('LAP Stats')

selected_tab = st.sidebar.selectbox('Select tabs:', ['Grafana Events', 'Job info by tasks'])

from_date = st.sidebar.date_input('From Date', pd.to_datetime('2023-01-01'))
to_date = st.sidebar.date_input('To Date', datetime.today())
skip = st.sidebar.number_input('Skip', value=0)
limit = st.sidebar.number_input('Limit', value=10)
sort = st.sidebar.selectbox("Sort order", ["asc", "desc"])

from_date = from_date.strftime("%Y-%m-%d")
to_date = to_date.strftime("%Y-%m-%d")

apis = API(base_url, username, password, from_date, to_date)
grafana = Grafana_Queries(base_url, username, password, from_date, to_date)
orgs = apis.get_orgs_ids()
orgs_names = list(map(lambda x: x[1], orgs))
orgs_ids = list(map(lambda x: x[0], orgs))

if 'Grafana Events' in selected_tab:
    select_orgs = st.selectbox("Select organization name", orgs_names)
    df = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(select_orgs)]))
    spreadsheet(df)

else:
    select_orgs = st.selectbox("Select organization name", orgs_names)

    df = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(select_orgs)]))

    data, stat, tn = apis.parse_response(df, skip, limit, sort = sort, org_name = select_orgs)
    select_tasks = st.selectbox("Select task name", set(map(lambda x :x[1], apis.get_task_ids(org_name = select_orgs))))

    indices = [index for index, value in enumerate(tn) if value == select_tasks]

    data_df = pd.DataFrame(data)

    spreadsheet(data_df.loc[indices])

    st.write(f"### Summary")
    st.write("Frame Total:", stat[0])
    st.write("Frame completed:", stat[1])
    st.write("Object completed:", stat[2])
    st.write("Remaining frame:", stat[3])