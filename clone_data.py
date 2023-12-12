import pandas as pd
from datetime import datetime
from utils import API, Grafana_Queries, get_id_name
import os

def load_data():
    with open(os.path.join("data","grafana_events.pkl"), 'rb') as f:
        grafana_events = pd.read_pickle(f)

    with open(os.path.join("data","progress.pkl"), 'rb') as f:
        progress = pd.read_pickle(f)

    with open(os.path.join("data","reviewer.pkl"), 'rb') as f:
        reviewer = pd.read_pickle(f)

    with open(os.path.join("data","worker.pkl"), 'rb') as f:
        worker = pd.read_pickle(f)

    with open(os.path.join("data","stats.pkl"), 'rb') as f:
        stats = pd.read_pickle(f)

    with open("data/selections.pkl", 'rb') as f:
        selections = pd.read_pickle(f)
    
    return grafana_events, progress, reviewer, worker, stats, selections

base_url = "https://lap.rainscales.com/"
username = "test"
password = "test"

def clone_data(from_date, to_date):
    apis = API(base_url, username, password, from_date, to_date)
    grafana = Grafana_Queries(base_url, username, password, from_date, to_date)
    orgs = apis.get_orgs_ids()
    orgs_names = list(map(lambda x: x[1], orgs))
    orgs_ids = list(map(lambda x: x[0], orgs))

    df_grafana_events = pd.DataFrame()
    df_progress = pd.DataFrame()
    df_performance_worker = pd.DataFrame()
    df_performance_reviewer = pd.DataFrame()
    df_stats = pd.DataFrame()
    df_selections = pd.DataFrame(columns = ["Org", "Prj", "Tsk"])

    for org in sorted(orgs_names):
        prj_ids, prj_names = get_id_name(apis, org)
        for prj in prj_names:
            tasks_ids, tasks_names = get_id_name(apis, org, prj)
            for task in tasks_names:
                # SELECTIONS
                df_selections.loc[len(df_selections)] = [org, prj, task]

                # GRAFANA EVENTS
                data_grafana = grafana.process_response(grafana.get_update_job(orgs_ids[orgs_names.index(org)], prj_ids[prj_names.index(prj)], tasks_ids[tasks_names.index(task)]))
                data_grafana['Org'] = org
                data_grafana['Prj'] = prj
                data_grafana['Tsk'] = task
                df_grafana_events = df_grafana_events.append(data_grafana, ignore_index = True)

                # PROGRESS MANAGEMENT
                data_progress_raw, stat_progress = apis.get_response(data_grafana, org_name = org, task_name = task)
                data_progress = pd.DataFrame(data_progress_raw)
                data_progress['Org'] = org
                data_progress['Prj'] = prj
                data_progress['Tsk'] = task
                df_progress = df_progress.append(data_progress, ignore_index = True)

                df_stat = pd.DataFrame([stat_progress], columns = ["Frame total", "Frame completed", "Object completed", "Remaining Frame"])
                df_stat['Org'] = org
                df_stat['Prj'] = prj
                df_stat['Tsk'] = task
                df_stats = df_stats.append(df_stat, ignore_index = True)

                # PERFORMANCE
                data_perf = pd.DataFrame(data_progress_raw)

                worker_df = data_perf[["Job", "User (annotate)", "Worker name (annotate)", "Team", "Frame (total)", "Object"]]
                worker_df = worker_df[worker_df['User (annotate)'].notna()]
                worker_df.rename(columns={"User (annotate)": "User", "Worker name (annotate)": "Worker name", "Frame (total)" : "Frame"}, inplace=True)
                worker_df = apis.get_performance(worker_df)
                worker_df['Org'] = org
                worker_df['Prj'] = prj
                worker_df['Tsk'] = task
                df_performance_worker = df_performance_worker.append(worker_df, ignore_index = True)

                review_df = data_perf[["Job", "User (review)", "Worker name (review)", "Team", "Frame (reviewed)", "Object"]]
                review_df = review_df[review_df['User (review)'].notna()]
                review_df.rename(columns={"User (review)": "User", "Worker name (review)": "Worker name", "Frame (reviewed)" : "Frame"}, inplace=True)
                review_df = apis.get_performance(review_df)
                review_df['Org'] = org
                review_df['Prj'] = prj
                review_df['Tsk'] = task
                df_performance_reviewer = df_performance_reviewer.append(review_df, ignore_index = True)

    df_grafana_events.to_pickle("data/grafana_events.pkl")
    df_progress.to_pickle("data/progress.pkl")
    df_performance_worker.to_pickle("data/worker.pkl")
    df_performance_reviewer.to_pickle("data/reviewer.pkl")
    df_stats.to_pickle("data/stats.pkl")
    df_selections.to_pickle("data/selections.pkl")