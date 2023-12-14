from enum import Enum
import requests
from urllib.parse import urljoin
import base64
import pandas as pd
from datetime import datetime
import numpy as np
import json

def get_id_name(api, select_org, project = None):
    if not project:
        res = api.get_project_ids(org_name = select_org)
    else:
        res = api.get_task_ids(org_name = select_org, project_name = project)
    
    return list(map(lambda x: x[0], res)), list(map(lambda x: x[1], res))


def generate_basic_auth_header(username, password):
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth_header = f"Basic {encoded_credentials}"

    return auth_header

def safe_division(numerator, denominator, decimal_point=None):
    if decimal_point is not None and decimal_point == 0:
        return None

    result = numerator / denominator if denominator != 0 else None
    
    if decimal_point is not None and result is not None:
        result = round(result, decimal_point)

    return result

def compare_time(from_date, to_date, date):
    date = date.date()
    from_date = pd.to_datetime(from_date).date()
    to_date = pd.to_datetime(to_date).date()
    if not from_date and not to_date:
        return True
    elif not from_date:
        return date <= to_date
    elif not to_date:
        return date >= from_date
    else:
        return date >= from_date and date <= to_date

class Message(Enum):
    SUCCESSFUL = 'SUCCESSFUL'
    FAILURES = 'FAILURES'
    INTERNAL_ERROR = "INTERNAL ERROR"

class Status_Code(Enum):
    SUCCESSFUL = 200
    FAILURES = 400
    INTERNAL_ERROR = 100

class Grafana_Queries():
    def __init__(self, base_url, username, password, from_date, to_date):
        self.base_url = base_url
        self.headers = {
            "accept": "application/vnd.cvat+json",
            "Content-Type": "application/json",
            "Authorization" : generate_basic_auth_header(username, password)
        }
        self.grafana_url = urljoin(self.base_url, "analytics/api/ds/query?ds_type=grafana-clickhouse-datasource")
        self.columns = "obj_name, obj_val, job_id, task_id, user_id, user_name, payload, timestamp, org_id"
        self.from_date = from_date
        self.to_date = to_date

    def url(self, path):
        return urljoin(self.base_url, path)

    def get_update_job(self, org_id, project_id, task_id):
        selected = self.columns
        if not self.from_date and not self.to_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id} AND project_id = {project_id} AND task_id = {task_id}) ORDER BY timestamp, job_id ASC"
        elif not self.from_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id} AND project_id = {project_id} AND task_id = {task_id}) AND (timestamp <= '{self.to_date} 23:19:19') ORDER BY timestamp, job_id ASC"
        elif not self.to_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id} AND project_id = {project_id} AND task_id = {task_id}) AND (timestamp >= '{self.from_date} 00:00:00') ORDER BY timestamp, job_id ASC"
        else:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id} AND project_id = {project_id} AND task_id = {task_id}) AND (timestamp BETWEEN '{self.from_date} 00:00:00' AND '{self.to_date} 23:19:19') ORDER BY timestamp, job_id ASC"

        data = {
            "queries":
            [{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"},"datasource":{"type":"grafana-clickhouse-datasource","uid":"PDEE91DDB90197936"},"format":1,"meta":{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"}},"queryType":"sql","rawSql": query,"refId":"A","datasourceId":1,"intervalMs":600000,"maxDataPoints":812}]}
        
        response = requests.post(url = self.grafana_url, json = data, headers = self.headers)
        while response.status_code != 200:
            response = requests.post(url = self.grafana_url, json = data, headers = self.headers)
        return response.json()

    def process_response(self, resp):
        data = pd.DataFrame(resp["results"]["A"]["frames"][0]['data']["values"])
        data = data.transpose()
        data.columns = self.columns.split(", ")

        return data


class API():
    def __init__(self, base_url, username, password, from_date, to_date):
        self.base_url = base_url
        self.headers = {
            "accept": "application/vnd.cvat+json",
            "Content-Type": "application/json",
            "Authorization" : generate_basic_auth_header(username, password)
        }
        self.grafana_url = urljoin(self.base_url, "analytics/api/ds/query?ds_type=grafana-clickhouse-datasource")
        self.from_date = np.datetime64(from_date + " 00:00:00") if from_date else None
        self.to_date = np.datetime64(to_date + " 23:19:19") if to_date else None
        self.orgs_ids = None
        self.tasks_ids = None
        self.total_frame = 0
        self.total_frame_successful = 0
        self.total_frame_unsuccessful = 0
        self.total_object_successful = 0

    def url(self, path):
        return urljoin(self.base_url, path)
    
    
    def get_update_date(self, job_id):
        response = requests.get(self.url(f"/api/jobs/{job_id}"), headers = self.headers)
        while response.status_code != 200:
            response = requests.get(self.url(f"/api/jobs/{job_id}"), headers = self.headers)
        time_unix = int(datetime.strptime(response.json()["updated_date"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
        return time_unix + 7 * 3600
    
    def get_time(self, job_id):
        query = f"SELECT timestamp, obj_val FROM cvat.events WHERE scope = 'update:job' AND job_id={job_id} AND obj_name='state' ORDER BY timestamp ASC"
        
        data = {
            "queries":
            [{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"},"datasource":{"type":"grafana-clickhouse-datasource","uid":"PDEE91DDB90197936"},"format":1,"meta":{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"}},"queryType":"sql","rawSql": query,"refId":"A","datasourceId":1,"intervalMs":600000,"maxDataPoints":812}]}


        response = requests.post(url = self.grafana_url, json = data, headers = self.headers)
        while response.status_code != 200:
            response = requests.post(url = self.grafana_url, json = data, headers = self.headers)
        results = response.json()["results"]["A"]["frames"][0]['data']["values"]

        result_rows =  list(zip(results[0], results[1]))

        total_annotating_time = 0

        for prev_row, cur_row in zip(result_rows, result_rows[1:]):
            if prev_row[1] == "in progress":
                total_annotating_time += int((cur_row[0] - prev_row[0]) / 1000)

        if result_rows and result_rows[-1][1] == "in progress":
            total_annotating_time += int(
                (self.get_update_date(job_id) - result_rows[-1][0] / 1000)
            )

        time = total_annotating_time / 60 # Minute
        return time

    def parse_time_issue(self, job_id):
        time = self.get_time(job_id)
        
        response = requests.get(self.url("/api/issues"), headers = self.headers, params = {"job_id": job_id})
        while response.status_code != 200:
            response = requests.get(self.url("/api/issues"), headers = self.headers, params = {"job_id": job_id})
        issues = response.json()['count']

        return time, issues
    
    def get_performance(self, df, rv = False):

        if df.empty:
            return df

        for index , row in df.iterrows():
            time, issues = self.parse_time_issue(int(row["Job"][4:]))
            if not rv:
                df.loc[index, 'Time (annotate)'] = time
                df.loc[index, 'Issues'] = issues
                df.loc[index, 'Performance/Frame (annotated)'] = safe_division(time, row["Frame (annotated)"], 1)
                df.loc[index, 'Performance/Object (annotated)'] = safe_division(time, row["Object (annotated)"], 2)
                df.loc[index, '%Issues'] = str(safe_division(issues * 100, row["Object (annotated)"], 1)) + "%" if row['Object (annotated)'] else None
            else:
                df.loc[index, 'Time (reviewed)'] = time
                df.loc[index, 'Performance/Frame (reviewed)'] = safe_division(time, row["Frame (reviewed)"], 1)
                df.loc[index, 'Performance/Object (reviewed)'] = safe_division(time, row["Object (reviewed)"], 2)

        return df
    
    def get_num_anno_frame(self, job_id):
        response = requests.get(self.url(f"api/jobs/{job_id}/annotations/?use_default_location=true"), headers = self.headers)
        while response.status_code != 200:
            response = requests.get(self.url(f"api/jobs/{job_id}/annotations/?use_default_location=true"), headers = self.headers)
        
        return len(response.json()['tags'])
    
    def get_jobs(self, params = None):
        response = requests.get(self.url("api/jobs"), headers = self.headers, params = params)
        while response.status_code != 200:
            response = requests.get(self.url("api/jobs"), headers = self.headers, params = params)

        return response.json()
    
    def get_orgs_ids(self):
        orgs_ids = []
        params = {"page": 1}
        response = requests.get(self.url("api/organizations"), headers = self.headers, params = params).json()
        while True:
            for org in response['results']:
                orgs_ids.append((org["id"], org["slug"]))
            if response['next']:
                params['page'] += 1
                response = requests.get(self.url("api/organizations"), headers = self.headers, params = params)
                while response.status_code != 200:
                    response = requests.get(self.url("api/organizations"), headers = self.headers, params = params)
                response = response.json()
                
            else:
                break

        return orgs_ids
    
    def get_project_ids(self, org_name = None):
        projects_ids = []

        params = {"page": 1, "org": org_name}
        response = requests.get(self.url("api/projects"), headers = self.headers, params = params)
        while response.status_code != 200:
            response = requests.get(self.url("api/projects"), headers = self.headers, params = params)
        response = response.json()

        while True:
            for project in response['results']:
                datetime_object = datetime.strptime(project["created_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                if not compare_time(self.from_date, self.to_date, datetime_object):
                    pass
                else:
                    projects_ids.append((project["id"], project["name"], org_name))
            if response['next']:
                params['page'] += 1
                response = requests.get(self.url("api/projects"), headers = self.headers, params = params)
                while response.status_code != 200:
                    response = requests.get(self.url("api/projects"), headers = self.headers, params = params)
                response = response.json()
                
            else:
                break

        return projects_ids


    def get_task_ids(self, org_name = None, project_name = None):
        tasks_ids = []

        params = {"page": 1, "org": org_name, "project_name": project_name}
        response = requests.get(self.url("api/tasks"), headers = self.headers, params = params)
        while response.status_code != 200:
            response = requests.get(self.url("api/tasks"), headers = self.headers, params = params)
        response = response.json()

        while True:
            for task in response['results']:
                datetime_object = datetime.strptime(task["created_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                if not compare_time(self.from_date, self.to_date, datetime_object):
                    pass
                else:
                    tasks_ids.append((task["id"], task["name"], org_name))
            if response['next']:
                params['page'] += 1
                response = requests.get(self.url("api/tasks"), headers = self.headers, params = params)
                while response.status_code != 200:
                    response = requests.get(self.url("api/tasks"), headers = self.headers, params = params)
                response = response.json()
                
            else:
                break

        return tasks_ids
    
    def get_num_labels(self, job_id):
        response = requests.get(self.url(f"api/jobs/{job_id}/annotations/?use_default_location=true"), headers = self.headers)
        while response.status_code != 200:
            response = requests.get(self.url(f"api/jobs/{job_id}/annotations/?use_default_location=true"), headers = self.headers)
        return len(response.json()['shapes'])
    
    def get_response_task(self, df, org_name = None, task_name = None):
        params = {"org": org_name, "page": 1, "task_name": task_name}
        response = self.get_jobs(params)

        final_output = pd.DataFrame(columns = ["Job", "Frame", "User (annotate)", "Frame (annotated)", "Object (annotated)", "Stage (annotate)", 
                                               "User (review)", "Frame (reviewed)", "Object (reviewed)", "Stage (review)"])
        stat_worker = {"Frame total": 0, "Frame completed": 0, "Object completed": 0, "Remaining frame": 0}
        stat_reviewer = {"Frame total": 0, "Frame completed": 0, "Object completed": 0, "Remaining frame": 0}
        while True:
            for job in response['results']:
                # Returned values
                general_info = {"Job": None, "Frame": 0}

                worker = {"User (annotate)": None, "Frame (annotated)": 0, "Object (annotated)": 0, "Stage (annotate)": None}
                reviewer = {"User (review)": None, "Frame (reviewed)": 0, "Object (reviewed)": 0, "Stage (review)": None}
                # Calculations

                general_info["Job"] = "Job#" + str(job["id"])
                general_info["Frame"] = job["stop_frame"] - job['start_frame'] + 1

                stat_worker["Frame total"] += general_info["Frame"]
                stat_reviewer["Frame total"] += general_info["Frame"]

                gfn = df[df["job_id"] == job['id']] # Grafana events

                if job['stage'] == "annotation":
                    worker["User (annotate)"] = job["assignee"]["username"] if job["assignee"] else None
                    worker['Stage (annotate)'] = job['state']
                    if job['state'] == 'completed':
                        worker["Frame (annotated)"] = general_info["Frame"]
                        worker["Object (annotated)"] = self.get_num_labels(job["id"])
                
                elif job['stage'] in ['validation', 'acceptance']:
                    # Annotation stage already completed -> Frames, Objects are completed
                    worker["Frame (annotated)"] = general_info["Frame"]
                    worker["Object (annotated)"] = self.get_num_labels(job["id"])

                    reviewer["User (review)"] = job["assignee"]["username"] if job["assignee"] else None
                    reviewer['Stage (review)'] = job['state']
                    if job['state'] == 'completed':
                        reviewer["Frame (reviewed)"] = general_info["Frame"]
                        reviewer["Object (reviewed)"] = self.get_num_labels(job["id"])
   
                    reviewer["Frame (reviewed)"] = general_info["Frame"]
                    reviewer["Object (reviewed)"] = self.get_num_labels(job["id"])

                    # Get the last two assignee, the latter one is the reviewer, the other is the worker
                    worker_info_name = None
                    assignee = gfn[gfn['obj_name'] == 'assignee']
                    assignee = assignee[assignee['obj_val'] != "None"]
                    last_two = assignee.tail(2)
                    if len(last_two) <= 1: # Stage changed to validation/acceptance when job is created
                        pass
                    else:
                        worker_info = last_two.iloc[0]
                        worker_info_name = json.loads(worker_info['obj_val'].replace("'", "\""))['username']
                    worker["User (annotate)"] = worker_info_name
                    worker["Stage (annotate)"] = 'completed'

                # Add to DataFrame
                final_output.loc[len(final_output)] = {
                    "Job": general_info["Job"], 
                    "Frame": general_info["Frame"], 
                    "User (annotate)": worker["User (annotate)"], 
                    "Frame (annotated)": worker["Frame (annotated)"], 
                    "Object (annotated)": worker["Object (annotated)"], 
                    "Stage (annotate)": worker["Stage (annotate)"], 
                    "User (review)": reviewer["User (review)"],
                    "Frame (reviewed)": reviewer["Frame (reviewed)"], 
                    "Object (reviewed)": reviewer["Object (reviewed)"], 
                    "Stage (review)": reviewer["Stage (review)"]
                }

                # Add stats
                stat_worker["Frame completed"] += worker["Frame (annotated)"]
                stat_worker["Object completed"] += worker["Object (annotated)"]

                stat_reviewer["Frame completed"] += reviewer["Frame (reviewed)"]
                stat_reviewer["Object completed"] += reviewer["Object (reviewed)"]

            if response['next']:
                params['page'] += 1
                response = self.get_jobs(params)
            else: 
                break
        
        stat_worker["Remaining frame"] = stat_worker["Frame total"] - stat_worker["Frame completed"]
        stat_reviewer["Remaining frame"] = stat_reviewer["Frame total"] - stat_reviewer["Frame completed"]

        return final_output, (stat_worker, stat_reviewer)

    # def get_response(self, df, org_name = None, task_name = None):

    #     output_payload = []

    #     params = {"org": org_name, "page": 1, "task_name": task_name}
    #     response = self.get_jobs(params)

    #     while True:
    #         for job in response['results']:
    #             payload = {
    #                 # general
    #                 "Job": "Job#" + str(job["id"]),
    #                 "Frame (total)": job["stop_frame"] - job['start_frame'] + 1,
    #                 "Team": org_name,
    #                 # annotator
    #                 "User (annotate)": int(job["assignee"]['id']) if job["assignee"] else None,
    #                 "Worker name (annotate)": job["assignee"]["username"] if job["assignee"] else None,
    #                 "Frame (annotated)": self.get_num_anno_frame(job['id']),
    #                 # reviewer
    #                 "User (review)": None,
    #                 "Worker name (review)": None,
    #                 "Frame (reviewed)": 0,

    #                 "Stage": job['state'],
    #                 "Object" : self.get_num_labels(job["id"])
    #             }

    #             self.total_frame += payload["Frame (total)"]
    #             self.total_object_successful += payload["Object"]
    #             if job['state'] == "complete":
    #                 self.total_frame_successful += payload["Frame (total)"]
    #             else:
    #                 self.total_frame_unsuccessful += payload["Frame (total)"]

    #             output_payload.append(payload)
            
    #         if response['next']:
    #             params['page'] += 1
    #             response = self.get_jobs(params)
                
                
    #         else: 
    #             break

    #     output_stat = [self.total_frame, self.total_frame_successful, self.total_object_successful, self.total_frame_unsuccessful]

    #     """
    #     process payload based on grafana
    #     """
    #     for job in output_payload:
    #         jid = int(job["Job"][4:])
    #         last_condition = df[(df['job_id'] == jid) & (df['obj_name'] == 'status')].tail(1)
    #         if last_condition.empty or last_condition.iloc[0]['obj_val'] == 'annotation': continue
    #         else:
    #             assignees = df[(df['job_id'] == jid) & (df['obj_name'] == 'assignee')]
    #             reviewer = assignees.tail(1)
    #             try:
    #                 obj_v = json.loads(reviewer['obj_val'].tolist()[0].replace("\'", "\""))
    #             except:
    #                 continue

    #             job['User (review)'] = obj_v['id']
    #             job['Worker name (review)'] = obj_v['username']
    #             job['Frame (reviewed)'] = job["Frame (annotated)"]

    #             last_worker = assignees.tail(2)

    #             if last_worker.empty or last_worker.iloc[0]['obj_val'] == "None" or len(last_worker) == 1: 
    #                 job['User (annotate)'] = obj_v['id']
    #                 job['Worker name (annotate)'] = obj_v['username']
    #             else:
    #                 last_worker = last_worker.iloc[0]

    #                 try:
    #                     lw = json.loads(last_worker['obj_val'].tolist()[0].replace("\'", "\""))
    #                 except:
    #                     continue
                        
    #                 job['User (annotate)'] = lw['id']
    #                 job['Worker name (annotate)'] = lw['username']

    #     return output_payload, output_stat