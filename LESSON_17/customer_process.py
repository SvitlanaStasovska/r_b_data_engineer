import os
import sys
from settings.setting import vars
import gcloud
from gcloud import storage
from settings.setting import query_customer_bronze_to_silver_upd, gsc_settings, folders
import shutil
import logging
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from gcloud import storage
from google.cloud import bigquery
import json

from settings.setting import query_customer_bronze_to_silver

os.environ.setdefault("GCLOUD_PROJECT", "de-07-svitlana-stasovska")

ProjectId = gsc_settings['ProjectId']
bucket_name = gsc_settings['bucket_name']
path = folders['raw_dir_customer']

SCHEMA_LIST = []
TABLE = 'Customer'
TABLE_ID = ProjectId+'.bronze.Customer'

BQ_CLIENT = bigquery.Client()

#Job_config    
job_config = bigquery.LoadJobConfig()
job_config.schema = SCHEMA_LIST
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
    dag_id = 'customer_process',
    default_args = args, 
    description = "Load csv_gcs_bronze",
    start_date = datetime(2024, 1, 1), 
    #lauch at 7^10 a.m.
    schedule_interval = '10 7 * * * ',
    catchup = False,
    max_active_runs = 1,
    tags = ['csv_load', 'bronze','silver','customer']

)

def customer_process():

    # identifying table schema
    @task(task_id='set_schema')
    def set_schema():
        hdw_schema = BQ_CLIENT.get_table(TABLE_ID)
        for field in hdw_schema.schema:
            SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))
        job_config.schema = SCHEMA_LIST


    # Load csv-files 1.to gcs (Google Cloud Storage:bucket='svt_bucket_02') 2.to bronze dataset
    @task(task_id='move_files_to_gc_bronze')
    def move_files_to_gc_bronze(file_path_inf):
    
        file_path = str(file_path_inf[0])
        filename_ = str(file_path_inf[1])

        #form gsc-path for gsc
        f_gcs = file_path[28:37]
        folder =str('data')+'/'+str('customers')+'/'+str(f_gcs)

        filename = "%s/%s" % (folder, filename_)
        #ref to gcs-bucket
        blob = bucket.blob(filename)

        # Uploading from a local file using open()
        with open(filename, 'rb') as f:
            blob.upload_from_file(f)  

        logging.info(f'File : {filename} transfered to gcs, into the folder : {folder})
        #Load CSV-files from gsc to BigQuery table
        gcs_uri = bucket_name+folder+'/'+filename_

        logging.info('gcs_uri: {gcs_uri}')

        #Insert into bronze table From gsc (raw_bucket)
        load_job = BQ_CLIENT.load_table_from_uri(
            gcs_uri, TABLE_ID, job_config=job_config
        )    

        load_job.result() 

        destination_table = BQ_CLIENT.get_table(TABLE_ID)
        logging.info(f'Loaded {destination_table.num_rows} rows in table {TABLE}')


    # cursor folders\files
    @task(task_id='listFoldersFiles')
    def listFoldersFiles(path):

        file_path_inf = []
        for i in os.listdir(path):
            if os.path.isdir(path + '\\' + i):
                folder = path + '\\' + i + '\\' 
                #logging.info(f'folder : {folder})
                list_dir = os.listdir(folder)
                for file in list_dir:
                    file_ = file 
                file_path_inf.append(folder)
                file_path_inf.append(file_)
        return(file_path_inf)
              
    #load data from bronze to silver dataset
    @task(task_id='customer_bronze_to_silver')
    def customer_bronze_to_silver():

        query = query_customer_bronze_to_silver_upd
        #logging.info(f'query : {query}')
        try:
            BQ_CLIENT.query_and_wait(query)
            logging.info("Insert completed")     
        except Exception as exc:                 
            logging.error(f'{exc}')
            raise

        
    set_schema() >> move_files_to_gc_bronze(listFoldersFiles()) >> customer_bronze_to_silver()
           
customer_process = customer_process()          

