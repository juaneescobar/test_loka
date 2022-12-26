from google.cloud import storage
import subprocess
import google.auth
import google.auth.transport.requests
import requests
import six.moves.urllib.parse
import google.auth
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
import json
import os
import datetime
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import api_connection
from datetime import datetime,timedelta

#Environment variables.
PROJECT_NAME = 'fif-fpay-reconciliation-test'
LOCATION     = 'us-east1'
COMPOSER_ENV = 'pay-recon-cl-test-global-orchestrator'
DAG_NAME     = 'events-cars-location'
currentTimeDate = datetime.now() - timedelta(days=2)
currentTime = currentTimeDate.strftime('%Y-%m-%d')

# If you are using the stable API, set this value to False
# For more info about Airflow APIs see https://cloud.google.com/composer/docs/access-airflow-api

def caller(data, context):
    try:
        event_path=check_storage(context)
        webserver_web_ui = api_connection.get_airflow_web_uri(PROJECT_NAME, LOCATION, COMPOSER_ENV)
        result=""
        if '/us-east1-pay-recon-cl-test--a606718c-bucket/data_batch/'+currentTime in event_path:
            if_exists = check_blobcontent_from_gcs(event_path)
            print(if_exists, event_path)
            if if_exists is True:
                dag_name = DAG_NAME
                run_params = "Project Name:%s, Location:%s, Composer Env:%s, Dag Name:%s, WebServer Web UI:%s" % (PROJECT_NAME,LOCATION,COMPOSER_ENV,DAG_NAME,webserver_web_ui)
                print(run_params)
                data = {"conf": {"event_file_path": "" + str(event_path) +""}, "dag_run_id": "raw_file_"+ str(datetime.datetime.now())}
                api_connection.trigger_dag(webserver_web_ui,dag_name,data)
                result='Call Processed'
            else:
                result="No Call get Processed"
        else:
            result="No Call Processed "
    except Exception as e:
        print("Problem in trigger dag ",e)
    return result

def check_storage(context):
    try:
        print("File Triggered")
        event=context.resource
        event_path=""
        event_dump_data=json.dumps(event)
        event_json_data=json.loads(event_dump_data)
        path_details=event_json_data['name'].split('/buckets/')[1]
        if path_details.endswith('/'):
            event_path="_FOLDER_" 
        else:
            path_array=path_details.split('/')
            for path in path_array:
                if path!='objects':
                    event_path +="/"+path
            print("event_path : "+event_path)
    except Exception as e:
        print("Problem in checking storage ",e)
    return event_path

def check_blobcontent_from_gcs(gcsfilepath):
    try:
        storage_client = storage.Client()
        bucketName = gcsfilepath.split("/")[1]
        fileName = gcsfilepath.split("/")[-1]
        fileNameWOExt = fileName.split(".")[0]
        fileNameExt =fileName.split(".")[1]
        prefixPath = gcsfilepath.replace("/"+bucketName+"/","").replace(fileName,"")
        bucket = storage_client.bucket(bucketName)
        blobs_all = list(bucket.list_blobs(prefix=prefixPath))
        for blob in blobs_all:
            if not blob.name.endswith("/"):
                blobNameWOExt = blob.name.split("/")[-1].split(".")[0]
                blobNameExt = blob.name.split("/")[-1].split(".")[1]
                if ((fileNameWOExt == blobNameWOExt) and (fileNameExt != blobNameExt)):
                    return False
        return True
    except Exception as e:
        print("Exception while checking same file with diff extension : ",e)