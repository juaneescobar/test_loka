import subprocess
import requests
import six.moves.urllib.parse
from google.auth.transport.requests import AuthorizedSession
import google.auth
import google.auth.compute_engine.credentials
import google.auth.iam
import google.oauth2.credentials
import google.oauth2.service_account
import json
import os
import datetime
from typing import Any

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"	
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

def trigger_dag(webserver_web_ui,dag_name,data):
    try:
        """
        Make a request to trigger a dag using the stable Airflow 2 REST API.
        https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

        Args:
        web_server_url: The URL of the Airflow 2 web server.
        dag_id: The DAG ID.
        data: Additional configuration parameters for the DAG run (json).
        """ 
        endpoint = f"api/v1/dags/{dag_name}/dagRuns"
        request_url = f"{webserver_web_ui}/{endpoint}"
        json_data = data

        response = web_server_request(
        request_url, method="POST", json=json_data
        )

        if response.status_code == 403:
            raise requests.HTTPError(
                "You do not have a permission to perform this operation. "
                "Check Airflow RBAC roles for your account."
                f"{response.headers} / {response.text}"
            )
        elif response.status_code != 200:
            response.raise_for_status()
        else:
            return response.text
    except Exception as e:
        print("Problem in triggering dag ",e)
    return 'Success'    

def web_server_request(url: str, method: str = "GET", **kwargs: Any):

    authed_session = AuthorizedSession(CREDENTIALS)
    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)

def get_airflow_web_uri(project_id, location, composer_environment):
    environment_url = (
    'https://composer.googleapis.com/v1/projects/{}/locations/{}'
    '/environments/{}').format(project_id, location, composer_environment)
    print(environment_url)
    composer_response = web_server_request(environment_url,'GET')
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']
    print(airflow_uri)
    return airflow_uri
