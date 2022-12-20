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

#Environment variables.
PROJECT_NAME = 'fif-fpay-reconciliation-test'
LOCATION     = 'us-east1'
COMPOSER_ENV = 'pay-recon-cl-test-global-orchestrator'
DAG_NAME     = 'events-cars-location'
IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
# If you are using the stable API, set this value to False
# For more info about Airflow APIs see https://cloud.google.com/composer/docs/access-airflow-api

def caller(data, context):
    try:
        event_path=check_storage(context)
        webserver_web_ui = 'https://u571d24c1781a3b15p-tp.appspot.com/code?dag_id=pay-recon-stddata-ingestion-process'
        result=""
        if '/us-east1-pay-recon-cl-test--a606718c-bucket/data_batch/2019-06-01-15-17' in event_path:
            if_exists = check_blobcontent_from_gcs(event_path)
            print(if_exists, event_path)
            if if_exists is True:
                # dag_name = 'events-cars-location'                
                run_params = "Project Name:%s, Location:%s, Composer Env:%s, Dag Name:%s, WebServer Web UI:%s" % (PROJECT_NAME,LOCATION,COMPOSER_ENV,'events-cars-location',webserver_web_ui)
                print(run_params)
                # client_id = connect_composer(PROJECT_NAME,LOCATION,COMPOSER_ENV)
                # print(client_id)
                trigger_dag(data, None)
                print("trigger corrio")
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

def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API
    When called via Google Cloud Functions (GCF),
    """

    # Fill in with your Composer info here
    # Navigate to your webserver's login page and get this from the URL
    client_id = '09331eb14d8a4eb1903dcac38b1f8907-dot-us-east1.composer.googleusercontent.com'

    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
    webserver_id = 'u571d24c1781a3b15p-tp'
    # The name of the DAG you wish to trigger
    dag_name = 'gcs_to_bigquery_operator'
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/api/experimental/dags/'
        + dag_name
        + '/dag_runs'
    )
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        webserver_url, client_id, method='POST', json={"conf": data, "replace_microseconds": 'false'})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE

def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text