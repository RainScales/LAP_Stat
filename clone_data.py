import pandas as pd
from datetime import datetime
from utils import API, Grafana_Queries, get_id_name
import os

def load_data():
    with open(os.path.join("data","grafana_events.pkl"), 'rb') as f:
        grafana_events = pd.read_pickle(f)

    with open(os.path.join("data","progress.pkl"), 'rb') as f:
        progress = pd.read_pickle(f)

    with open(os.path.join("data","performance.pkl"), 'rb') as f:
        performance = pd.read_pickle(f)

    with open(os.path.join("data","stats.pkl"), 'rb') as f:
        stats = pd.read_pickle(f)

    with open("data/selections.pkl", 'rb') as f:
        selections = pd.read_pickle(f)
    
    return grafana_events, progress, performance, stats, selections

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
    df_performance = pd.DataFrame()
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
                data_progress_raw, stat_progress = apis.get_response_task(data_grafana, org_name = org, task_name = task)
                data_progress = data_progress_raw
                data_progress['Org'] = org
                data_progress['Prj'] = prj
                data_progress['Tsk'] = task
                df_progress = df_progress.append(data_progress, ignore_index = True)

                df_stat_worker = pd.DataFrame([stat_progress[0]], columns = ["Frame total", "Frame completed", "Object completed", "Remaining frame"])
                df_stat_worker['Org'] = org
                df_stat_worker['Prj'] = prj
                df_stat_worker['Tsk'] = task
                df_stat_worker['Filter'] = "Worker"

                df_stat_reviewer = pd.DataFrame([stat_progress[1]], columns = ["Frame total", "Frame completed", "Object completed", "Remaining frame"])
                df_stat_reviewer['Org'] = org
                df_stat_reviewer['Prj'] = prj
                df_stat_reviewer['Tsk'] = task
                df_stat_reviewer['Filter'] = "Reviewer"

                df_stats = df_stats.append(df_stat_worker, ignore_index = True)
                df_stats = df_stats.append(df_stat_reviewer, ignore_index = True)

                # PERFORMANCE
                data_perf = data_progress_raw

                worker_df = data_perf[["Job", "User (annotate)", "Frame (annotated)", "Object (annotated)"]]
                worker_df = apis.get_performance(worker_df)

                review_df = data_perf[["Job", "User (review)", "Frame (reviewed)", "Object (reviewed)"]]

                review_df = apis.get_performance(review_df, rv = True)

                df_perf = pd.merge(worker_df, review_df, on = "Job", how = 'inner')
                df_perf = df_perf.drop(columns = ["Job"])
                df_perf["Org"] = org
                df_perf["Prj"] = prj
                df_perf["Tsk"] = task

                df_performance = df_performance.append(df_perf, ignore_index = True)

    df_grafana_events.to_pickle("data/grafana_events.pkl")
    df_progress.to_pickle("data/progress.pkl")
    df_performance.to_pickle("data/performance.pkl")
    df_stats.to_pickle("data/stats.pkl")
    df_selections.to_pickle("data/selections.pkl")