import google.auth
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import List
import json
from copy import deepcopy
from google.cloud import bigquery
from google.cloud import storage
import json
from datetime import datetime,timedelta

currentTimeDate = datetime.now() - timedelta(days=2)
currentTime = currentTimeDate.strftime('%Y-%m-%d')

# define dataflow funtion, direct runner is conf during local testing
def dataflow_batch(uri, table_id):
    
    options = PipelineOptions(beam_args = None, runner="DirectRunner", streaming=False)
         
    with beam.Pipeline(options=options) as pipe:
        input_data = pipe | "Read json file from gs" >> beam.io.ReadFromText(uri)

        input_dict = input_data | "Convert to Dict" >> beam.Map(lambda elm: json.loads(elm))

        input_dict | "Print Output data" >> beam.Map(lambda elm: print(json.dumps(elm)))

        input_dict | "Write to BQ" >> beam.io.gcp.bigquery.WriteToBigQuery(table_id, dataset=dataset,schema=table_schema, project=project, insert_retry_strategy="neverRetry", create_disposition="CREATE_NEVER", method="STREAMING_INSERTS")
        print('cargo batch')

# gdeine dataset, table and project to use
cred, project = google.auth.default()
dataset, table_name = "pay_recon_cl", "create_table"

# schema if the data ingested
table_schema = 'event:string,on:string,at:string,data:record,organization_id:string'

client = bigquery.Client()
storage_client = storage.Client()

# getting blobs inside us-east1-pay-recon-cl-test--a606718c-bucket
bucket = storage_client.get_bucket("us-east1-pay-recon-cl-test--a606718c-bucket")
blobs = bucket.list_blobs()

print("data_batch/"+currentTime)       
# Opening JSON file
for  blob in blobs:
    if "data_batch/"+currentTime in blob.name:
        print('ss')
        
        uri = "gs://us-east1-pay-recon-cl-test--a606718c-bucket/"+blob.name
        table_id = "cars_events_table"

        # calling dataflow to ingest each of the files with the matching pattern
        dataflow_batch(uri, table_id)

        # moving ingested files to a new folder call /data_load
        bucket.rename_blob(blob, 'data_load'+(blob.name).replace("data_batch", ""))   
        print('Blob {} in bucket {} moved to blob {} .'.format(blob, bucket.name, 'data_load'+(blob.name).replace("data_batch", ""))) 
        
      
    else:
        "No files available with the current date"