import os
import sys
from settings.setting import query_user_profile_bronze_to_silver, gsc_settings, folders
import gcloud
from gcloud import storage
import shutil
import logging
from datetime import date, datetime, timedelta

from gcloud import storage
from google.cloud import bigquery
import json

from airflow.decorators import dag, task
from robo_dreams.settings.setting import vars
from airflow.models import Variable

ProjectId = gsc_settings['ProjectId']
bucket_name = gsc_settings['bucket_name']
os.environ.setdefault("GCLOUD_PROJECT", "de-07-svitlana-stasovska")

TABLE = 'user_profile_2'
TABLE_ID = 'de-07-svitlana-stasovska.bronze.user_profile_2'

BQ_CLIENT = bigquery.Client()
folder = folders['raw_dir_user_profile']
folder_ =str('data')+'/'+str('_user_profiles')  
filename_ = Variable.get("user_profile_enriched") #'user_profiles.json'
    
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("birth_date", "date"),
        bigquery.SchemaField("phone_number", "STRING"),
    
    ],
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)  

args = {
    'owner': 'stasovska',
    'depends_on_past': False,
    'email': ['s.stasovska@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id = 'user_profile_process',
    default_args = args, 
    description = "Load csv_gcs_bronze_silver",
    start_date = datetime(2024, 1, 1), 
    schedule_interval = None,
    catchup = False,
    max_active_runs = 1,
    tags = ['csv_load', 'bronze','silver','user_profile']

)  

client = storage.Client()
bucket = client.get_bucket('svt_bucket_02')

def user_profile_process(): 

    @task(task_id='move_files_to_gc_bronze')
    def move_files_to_gc_bronze():

        filename = "%s/%s" % (folder_, filename_)
    
        #ref to gcs-bucket
        blob = bucket.blob(filename)

        # Uploading from a local file using open()
        with open(filename, 'rb') as f:
            blob.upload_from_file(f)  
        
        logging.info(f'File : {filename} transfered to gcs, into the folder : {folder}')    
    
        gcs_uri = str(bucket_name)+str(folder)+'/'+str(filename_)

        logging.info('gcs_uri: {gcs_uri}')

        #Insert into bronze table From gsc (raw_bucket)
        load_job = BQ_CLIENT.load_table_from_uri(
            gcs_uri, TABLE_ID, job_config=job_config
        )    

        load_job.result() 

        destination_table = BQ_CLIENT.get_table(TABLE_ID)
        logging.info(f'Loaded {destination_table.num_rows} rows in table {TABLE}')

    @task(task_id='user_profile_bronze_to_silver')
    def user_profile_bronze_to_silver():

        query = query_user_profile_bronze_to_silver
        try:
            BQ_CLIENT.query_and_wait(query)
            logging.info("Insert completed") 
        except Exception as exc:                 
            logging.error(f'{exc}')
            raise

    move_files_to_gc_bronze() >> user_profile_bronze_to_silver()
           
user_profile_process = user_profile_process()            

