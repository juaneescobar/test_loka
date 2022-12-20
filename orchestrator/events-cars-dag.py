from datetime import timedelta

from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator


import json
import os 
from airflow import models
from airflow.models import Variable
from jinja2 import Template
import pytz
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email_operator import EmailOperator
from airflow.operators import dummy_operator, bash_operator, python_operator
import datetime

##################################################################################################
####################    Conf Variables             ###############################################
##################################################################################################


config = Variable.get('config_general', deserialize_json= True)

ambiente_destino=config['proj_name']
esquema_destino=config['dataset']
destino=ambiente_destino+"."+esquema_destino
destino_dosPuntos=ambiente_destino+":"+esquema_destino

tz = pytz.timezone('US/Pacific')
tstmp = datetime.datetime.now(tz).strftime('%Y%m%d%H%M%S')

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

dag_email_recipient = 'juane.escobard@gmail.com'

##################################################################################################
########################    DAG Definition      ##################################################
##################################################################################################
default_args = {
    'owner': 'JEED',
    'depends_on_past': False,
    'email': dag_email_recipient,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=10),
    ##################################################################################################    
    'start_date':  yesterday, #datetime.datetime(year=2020, month=11, day=20), #Fecha de inicio. 
    ##################################################################################################    
    'schedule_interval': '0 0 * * *' #runs every day at 12 am, ingesting all the files from the previous day
}


with models.DAG('events-cars-location',default_args=default_args, schedule_interval='0 0 * * *') as dag:

    start           = dummy_operator.DummyOperator(task_id='start')
    end             = dummy_operator.DummyOperator(task_id='end',trigger_rule=TriggerRule.ONE_SUCCESS,)   

##################################################################################################
#########################    DAG INIT                               ##############################
##################################################################################################

    # calling dataflow job in order to ingest the new data  
    ingest_events_table = DataflowCreatePythonJobOperator(
            task_id="ingest_raw_events_cars",
            py_file=f'gs://us-east1-pay-recon-cl-test--a606718c-bucket/dags/pipeline/batch_load.py',
            options={"runner":"DataflowRunner","project":"fif-fpay-reconciliation-test","region":"us-central1" ,"temp_location":"gs://us-east1-pay-recon-cl-test--a606718c-bucket/", "staging_location":"gs://us-east1-pay-recon-cl-test--a606718c-bucket/"},
            job_name=f'job{tstmp}',
            location='us-central1',
            wait_until_finished=True,
    )

    # run summary of the table in bigquery, with this table you could calculate differente mesures during the operational time
    summary_events_cars = BigQueryOperator(
        task_id='bq-summary-events-cars',
        sql='''with journey as (
                    select *, 
                    lag(data.location.lat) OVER(PARTITION BY data.id ORDER BY data.location.at) AS end_lat, 
                    lag(data.location.lng) OVER(PARTITION BY data.id ORDER BY data.location.at) AS end_lng,
                    FROM `fif-fpay-reconciliation-test.pay_recon_cl.cars_events_table` 
                    WHERE event= 'update'),

                    operation_period as(
                    select data.id, data.start, data.finish, organization_id from `fif-fpay-reconciliation-test.pay_recon_cl.cars_events_table` tb where tb.on = 'operating_period'
                    ),

                    final_data as (
                    select data.id id_vehicle, data.location.at, op.start, op.finish, op.id,
                    ST_DISTANCE(ST_GEOGPOINT(CAST(data.location.lng AS FLOAT64), CAST(data.location.lat AS FLOAT64)),
                                ST_GEOGPOINT(CAST(end_lng AS FLOAT64), CAST(end_lat AS FLOAT64))) dist_recorrida 
                                from journey tb
                    inner join operation_period op on tb.organization_id = op.organization_id
                    where 
                    data.location.at between start and finish 
                    order by op.id)

                    select id operation_id, id_vehicle, sum(dist_recorrida) dist_total from final_data
                    group by id_vehicle, id;
                    ''',
        destination_dataset_table='{}.summary_events_cars'.format(destino),
        write_disposition='WRITE_TRUNCATE', #WRITE_TRUNCATE -> overwrite data
        use_legacy_sql=False,    
        dag=dag
    )

    fail_email_body = f"""
    Hi, <br><br>
    events cars location DAG has been failure at {tstmp}.
    """

    Failed = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="Failed",
    to=dag_email_recipient,
    subject='event cars location failed',
    html_content=fail_email_body)

    Failed.set_upstream(ingest_events_table)

start >> ingest_events_table >> summary_events_cars >> Failed >> end