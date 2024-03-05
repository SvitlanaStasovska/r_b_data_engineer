import os
import sys
from settings.setting import vars
import gcloud
from gcloud import storage
from settings.setting import query_user_profile_enriched, gsc_settings
import shutil
import logging
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from gcloud import storage
from google.cloud import bigquery
import json

ProjectId = gsc_settings['ProjectId']
bucket_name = gsc_settings['bucket_name']

os.environ.setdefault("GCLOUD_PROJECT", ProjectId)

SCHEMA_LIST = []
TABLE = 'user_profile'
TABLE_ID = ProjectId+'.bronze.user_profile'

BQ_CLIENT = bigquery.Client()

#Job_config    
job_config = bigquery.LoadJobConfig()
#job_config.schema = SCHEMA_LIST
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.field_delimiter = ','
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.null_marker = ''

client = storage.Client()
bucket = client.get_bucket(bucket_name) #'svt_bucket_02'

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
    dag_id = 'user_profile_enriched_process',
    default_args = args, 
    description = "Load csv_gcs_bronze",
    start_date = datetime(2024, 1, 1), 
    schedule_interval = None,
    catchup = False,
    max_active_runs = 1,
    tags = ['csv_load', 'bronze','silver','user_profile', 'enriched']

)

def user_profile_enriched_process():
                         
    @task(task_id='user_profile_enriched')        
    def user_profile_enriched():

        query = query_user_profile_enriched
        #logging.info(query)
        try:
            BQ_CLIENT.query_and_wait(query)
            logging.info("Data enriched")     
        except Exception as exc:                 
            logging.error(f'{exc}')
            raise

    user_profile_enriched()
           
user_profile_enriched_process = user_profile_enriched_process()          

