![image](https://user-images.githubusercontent.com/37155220/208578794-bf3dcc73-3229-442a-8102-d2b68cef4ace.png)

Taking into account that for this project it will be required to ingest daily multiple events that have a common scheme and from the ingest, perform different transformations that include historical calculations, unions and groupings in order to monitor the location of the vehicles.

A common architecture will be used in the Google Cloud Platform environment through the use of Cloud Storage as a data lake to centralize the information that would arrive daily from the different events of the vehicles.
For processing and ingestion, Dataflow would be used through a .py called batch_load, then the result will be written in bigquery on the events_cars_table table that contains the 'update' type event schema because it is the one with the most attributes in the field ' data' type record.

Once our table has been ingested, we proceed to carry out transformations such as unions or grouped calculations through sql statements that include temporary tables during the process, the latter would correspond to our data warehouse.

The visualization, although not included in the challenge, could be done using power bi or data studio, which could be connected to the final tables in our data warehouse (bigquery).

The automation of this flow was carried out with two different approaches:

through airflow events-cars-dag, which fully automates the flow and runs the entire batch of the previous day, that is, at 12 am it processes all the events of the previous day.
With cloud funtion (cloud funtion trigger) , we create a storage type trigger and a validator in which files with the desired pattern are searched for in order to start the events-cars-dag dag and thus ingest the files once they reach our bucket.
