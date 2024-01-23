DAG_ID = 'process_sales_gc'

import os
import sys
#from settings.setting import vars
#import gcloud
from gcloud import storage
import logging

from airflow.decorators import dag, task
from robo_dreams.settings.setting import vars

from airflow.models import Variable

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
    dag_id=DAG_ID,
    default_args = args,
    description = "Move data into GC",
    schedule_interval = '0 1 * * * ',
    start_date = datetime(2024, 1, 1, 22, 0, 0),
    catchup = True,
    tags = ['gc']
)
    
os.environ.setdefault("GCLOUD_PROJECT", "DE-07")

def move_files_gc():
    @task(task_id='move_files_gc')
    def move_files_to_gc():

        filename_ = Variable.get(filename) #'2022-08-09_1.json
        date_ = Variable.get(date)         #'2022-08-09'
    
        d_year = date_[0:4]
        d_mm = date_[5:7]
        d_dd = date_[8:10]

        #Init folder
        folder =str('src1')+'/'+str('sales')+'/'+str('v1')+'/'+str(d_year)+'/'+str(d_mm)+'/'+str(d_dd)

        client = storage.Client()
        bucket = client.get_bucket('svt_bucket_02')
        blob = bucket.blob(filename_)

        # src1/sales/v1/2022/08/01/
        filename = "%s/%s" % (folder, filename_)
        blob = bucket.blob(filename)

        # Uploading from a local file using open()
        with open(filename, 'rb') as f:
            blob.upload_from_file(f)  
        logging.info(f' File : {filename} transfered to GoogleCloud in  folder : {folder} !')
    

    move_files_to_gc()
    
DAG_ID = move_files_gc()