import os
import sys
#import json
import requests
import argparse
from pathlib import Path
from datetime import date 
from settings.setting import vars
from fastavro import writer, schema
from rec_avro import to_rec_avro_destructive, rec_avro_schema


# Initialyzing varibles from config-file
AUTH_TOKEN = os.environ['AUTH_TOKEN']
BASE_URL = vars['BASE_URL']
raw_dir =  vars['raw_dir']
stg_dir =  vars['stg_dir']

def main():

    # Adjusting parser and parser arguments to retrieve data from API
    parser = argparse.ArgumentParser(description="Lesson_2 Getting data from API and putting args into the folders raw_dir and stg_dir")
    parser.add_argument("date", help="date to get data from the API")
    parser.add_argument("page", help="page number to get data from the API")
    args = parser.parse_args()

    date_ = args.date
    page_ = args.page
    
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
            params={'date': {args.date}, 'page': {args.page}},
            headers={'Authorization': AUTH_TOKEN},
            )
    

    if (str(response.json()).find("ValueError") > 0 or response.status_code != 200) :
        print(f"There is no data on your demand on date : {args.date} and page : {args.page}")
        sys.exit(0)

    # Save json into file in raw-folder
    with open (file=filename_, mode='w', encoding='Windows-1251') as wr:
        wr.write(str(response.json()))

    # Convert files into AVRO-format and move to the stg-folder
    avroObjects = (to_rec_avro_destructive(rec) for rec in response.json())
   
    with open(filename_avro_, 'wb') as file_avro:
        writer(file_avro, schema.parse_schema(rec_avro_schema()), avroObjects)


if __name__ == '__main__':
    main()