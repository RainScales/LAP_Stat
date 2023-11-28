from enum import Enum
import requests
from urllib.parse import urljoin
import base64
import pandas as pd
from datetime import datetime
import numpy as np
import json

def generate_basic_auth_header(username, password):
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    auth_header = f"Basic {encoded_credentials}"

    return auth_header

def compare_time(from_date, to_date, date):
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
    INTERNAL_ERROR = 500

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

    def get_update_job(self, org_id):
        selected = self.columns
        if not self.from_date and not self.to_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id}) ORDER BY timestamp, job_id ASC"
        elif not self.from_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id}) AND (timestamp <= '{self.to_date} 23:59:59') ORDER BY timestamp, job_id ASC"
        elif not self.to_date:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id}) AND (timestamp >= '{self.from_date} 00:00:00') ORDER BY timestamp, job_id ASC"
        else:
            query = f"SELECT {selected} FROM cvat.events WHERE (scope = 'update:job' AND obj_name <> 'state' AND org_id = {org_id}) AND (timestamp BETWEEN '{self.from_date} 00:00:00' AND '{self.to_date} 23:59:59') ORDER BY timestamp, job_id ASC"

        data = {
            "queries":
            [{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"},"datasource":{"type":"grafana-clickhouse-datasource","uid":"PDEE91DDB90597936"},"format":1,"meta":{"builderOptions":{"fields":["*"],"filters":[{"condition":"AND","filterType":"custom","key":"timestamp","operator":"WITH IN DASHBOARD TIME RANGE","type":"DateTime64(3, 'Etc/UTC')","value":"TODAY"},{"condition":"AND","filterType":"custom","key":"scope","operator":"IN","type":"String","value":[""]}],"mode":"list","orderBy":[{"dir":"ASC","name":"timestamp"}],"table":"events"}},"queryType":"sql","rawSql": query,"refId":"A","datasourceId":1,"intervalMs":600000,"maxDataPoints":852}],"from":"1700557340464","to":"1701162140464"}
        

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
        self.from_date = np.datetime64(from_date + " 00:00:00") if from_date else None
        self.to_date = np.datetime64(to_date + " 23:59:59") if to_date else None
        self.orgs_ids = self.get_orgs_ids()
        self.tasks_ids = self.get_task_ids()
        self.total_frame = 0
        self.total_frame_successful = 0
        self.total_frame_unsucessful = 0
        self.total_object_successful = 0
        

    def url(self, path):
        return urljoin(self.base_url, path)
    
    def get_num_anno_frame(self, job_id):
        response = requests.get(self.url(f"api/jobs/{job_id}/annotations/?action=download&location=local&use_default_location=true"), headers = self.headers)
        num_lbl = set()

        for i in response.json()['shapes']:
            num_lbl.add(i['id'])
        
        return len(num_lbl)
    
    def get_jobs(self, params = None):
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
                response = requests.get(self.url("api/organizations"), headers = self.headers, params = params).json()
            else:
                break

        return orgs_ids

    def get_task_ids(self, org_name = None):
        tasks_ids = []
        params = {"page": 1}
        for org in self.orgs_ids:
            if org_name:
                params = {"page": 1, "org": org_name}
            else:
                params = {"page": 1, "org_id": org[0]}
            response = requests.get(self.url("api/tasks"), headers = self.headers, params = params).json()
            while True:
                for task in response['results']:
                    datetime_object = datetime.strptime(task["created_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    if not compare_time(self.from_date, self.to_date, datetime_object):
                        continue
                    tasks_ids.append((task["id"], task["name"], org))
                if response['next']:
                    params['page'] += 1
                    response = requests.get(self.url("api/tasks"), headers = self.headers, params = params).json()
                else:
                    break

        return tasks_ids
    
    def get_num_labels(self, job_id):
        params = {"job_id": job_id}
        response = requests.get(self.url(f"api/jobs/{job_id}/annotations"), headers = self.headers, params = params)

        return len(response.json()['shapes'])

    def parse_response(self, df, skip, limit, sort = 'asc', org_name = None):

        output_payload = []
        task_names = []

        if sort == 'desc':
            self.tasks_ids.sort(key = lambda x: x[1], reverse = True)
        else:
            self.tasks_ids.sort(key = lambda x: x[1])

        for task in self.tasks_ids[skip:limit]:
            if org_name:
                params = {"org": org_name, "page": 1, "task_id": task[0]}
            else:
                params = {"org_id": task[2][0], "page": 1, "task_id": task[0]}
            response = self.get_jobs(params)
            while True:
                for job in response['results']:

                    payload = {
                        # general
                        "job_id": job["id"],
                        "frame (total)": job["stop_frame"] - job['start_frame'] + 1,
                        "team": task[2][1],
                        # annotator
                        "user (anno)": job["assignee"]['id'] if job["assignee"] else None,
                        "worker name (anno)": job["assignee"]["username"] if job["assignee"] else None,
                        "frame (annotated)": self.get_num_anno_frame(job['id']),
                        # reviewer
                        "user (review)": None,
                        "worker name (review)": None,
                        "frame (reviewed)": 0,

                        "stage": job['state'],
                        "object" : self.get_num_labels(job["id"])
                    }

                    self.total_frame += payload["frame (total)"]
                    self.total_object_successful += payload["object"]
                    if job['state'] == "complete":
                        self.total_frame_successful += payload["frame (total)"]
                    else:
                        self.total_frame_unsucessful += payload["frame (total)"]

                    output_payload.append(payload)
                    task_names.append(task[1])
                
                if response['next']:
                    params['page'] += 1
                    response = self.get_jobs(params)
                    
                else: 
                    
                    break
        output_stat = [self.total_frame, self.total_frame_successful, self.total_frame_unsucessful, self.total_object_successful]

        """
        process payload based on grafana
        """
        for job in output_payload:
            jid = job["job_id"]
            last_condition = df[(df['job_id'] == jid) & (df['obj_name'] == 'status')].tail(1)
            if last_condition.empty or last_condition.iloc[0]['obj_val'] == 'annotation': continue
            else:
                assignees = df[(df['job_id'] == jid) & (df['obj_name'] == 'assignee')]
                reviewer = assignees.tail(1)

                obj_v = json.loads(reviewer['obj_val'].tolist()[0].replace("\'", "\""))

                job['user (review)'] = obj_v['id']
                job['worker name (review)'] = obj_v['username']
                job['frame (reviewed)'] = job["frame (annotated)"]
            

                last_worker = assignees.tail(2)

                if last_worker.empty or last_worker.iloc[0]['obj_val'] == "None" or len(last_worker) == 1: 
                    job['user (anno)'] = obj_v['id']
                    job['worker name (anno)'] = obj_v['username']
                else:
                    last_worker = last_worker.iloc[0]
                    lw = json.loads(last_worker['obj_val'].tolist()[0].replace("\'", "\""))
                    job['user (anno)'] = lw['id']
                    job['worker name (anno)'] = lw['username']

        return output_payload, output_stat, task_names

def process_management_fn(from_date=None, end_date=None, skip=0, limit=10, sort='asc', org = None):
    """
    Process Management

    @param from_date: Work day
    @param end_date: End works
    @param skip: start indexes
    @param limit: end indexes
    @param sort: sort by task name ('asc' or 'desc')

    @return: 
    """

    base_url = "http://117.2.164.10:50082/"
    username = "admin"
    password = "admin"

    if from_date:
        from_date = from_date.strftime("%Y-%m-%d")

    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    apis = API(base_url, username, password, from_date, end_date)

    grafana = Grafana_Queries(base_url, username, password, from_date, end_date)

    payload, stats, tn = apis.parse_response(skip, limit, sort = sort, org = org)

    """
    job: job id
    frame: number of frames

    user (anno): annotator
    worker name (anno): name of annotator
    team (anno): team name of annotator
    frame (anno): number of frames annotated
    object (anno): number of objects annotated
    deadline (anno): annotation deadline
    stage (anno): status of the annotation

    user (review): job reviewer
    worker name (review): name of reviewer
    team (review): team name of reviewer
    frame (review): number of frames reviewed
    object (review): number of objects reviewed
    deadline (review): review deadline
    stage (review): status of the review

    """

    return {
        "data":{
            "payload":
                payload,
            "stats": {
                'total_frame': stats[0],
                'total_frame_successful': stats[1],
                'total_frame_unsuccessful': stats[2],
                'total_object_successful': stats[3],
            }
        },
        'status': 200,
        'message': Message.SUCCESSFUL.value

    }, grafana.process_response(grafana.get_update_job()), tn