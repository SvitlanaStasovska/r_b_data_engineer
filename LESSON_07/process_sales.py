DAG_ID = 'process_sales'

import os
import sys
#import json
import requests
import argparse
from pathlib import Path
from datetime import date, timedelta
import logging

from fastavro import writer, schema
from rec_avro import to_rec_avro_destructive, rec_avro_schema

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

# Initialyzing varibles from config-file
#AUTH_TOKEN = os.environ['AUTH_TOKEN']

AUTH_TOKEN = vars['AUTH_TOKEN']
BASE_URL = vars['BASE_URL']
raw_dir =  vars['raw_dir']
stg_dir =  vars['stg_dir']

#  1. task_id=load_files_from_api, яка трігає першу джобу з завдання 2
#  2. task_id=convert_files_avro, яка трігає другу джобу з завдання 2

def extract_data_from_api():

    
    date_ = Variable.get("date_")
    page_ = Variable.get("page_")
    
    logging.info(f'date: {date_}, page_ : {page_}')
    
    # Creating name of the file and route for them
    filename = 'sales_'+date_+'_'+page_

    raw_dir_ = str(raw_dir)+'/'+str(date_)
    stg_dir_ = str(stg_dir)+'/'+str(date_)
    
    filename_ = raw_dir_+'/'+str(filename)+'.json'

    filename_avro = str(stg_dir_)+'/'+str(filename)+'.avro'
    filename_avro_ = Path(filename_avro)

    # Check if folder doesn't exist - create it
    isExistFolder = os.path.exists(raw_dir_)
    isExistFolder_stg = os.path.exists(stg_dir_)

    isExistFile = os.path.exists(filename_)
    isExistFile_stg = os.path.exists(filename_avro_)
    
    logging.info(f'isExistFile = {isExistFile}')

    if not isExistFolder:
        os.makedirs(raw_dir_)      

    if isExistFile:
        os.remove(filename_)

    if not isExistFolder_stg:
        os.makedirs(stg_dir_)

    if isExistFile_stg:
        os.remove(filename_avro_)

    # Get JSON from API   
    response = requests.get (
            url=BASE_URL,
            params={'date': {date_}, 'page': {page_}},
            headers={'Authorization': AUTH_TOKEN},
            )
            
    logging.info(f'response_status_code: {response.status_code}')    
    

    if (str(response.json()).find("ValueError") > 0 or response.status_code != 200) :
        print(f"There is no data on your demand on date : {date_} and page : {page_}")
        sys.exit(0)
    
    f = open("r_b_wr.csv", "w")

    # Save json into file in raw-folder
    with open (file=filename_, mode='w', encoding='Windows-1251') as wr:
        wr.write(str(response.json()))


def convert_to_avro():

    # Convert into AVRO-format and move to the stg-folder
    avroObjects = (to_rec_avro_destructive(rec) for rec in response.json())
   
    with open(filename_avro_, 'wb') as file_avro:
        writer(file_avro, schema.parse_schema(rec_avro_schema()), avroObjects)
    
    logging.info('Converted into AVRO !')

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
        
with DAG(
    DAG_ID,
    default_args = args,
    description = "Get files from api and convert into AVRO-format",
    schedule_interval = '0 1 * * * ',
    start_date = datetime(2024, 1, 1, 22, 0, 0),
    catchup = True,
    tags = ['avro','files','json']
) as dag:

    load_files_from_api = PythonOperator (
        task_id = 'get_files_from_api',
        python_callable = extract_data_from_api,
        dag=dag

    )  

    convert_files_avro = PythonOperator (
        task_id = 'convert_to_avro',
        python_callable = convert_to_avro,
        dag=dag

    ) 

    load_files_from_api >>  convert_files_avro        