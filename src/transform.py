import logging
import json 
import pandas as pd
from google.cloud import storage
from typing import Dict, List, Optional, Any

from .gc import GoogleClient
from .parser import extract_cvedata
from .extract import get_years

logging.basicConfig(level=logging.INFO)

# Declaring google client instance globally so multiple functions can use it
gc = GoogleClient()

# Declaring an empty list globally so that it can be used to create the combined dataset
combined_proccessed_records = []

def create_combined_table(combined_processed_records: Dict = {}):
    try:
        logging.info(f'Creating combined table...')
        gc.csv_bigquery(files=combined_processed_records)
    except Exception as e:
        logging.info(f'Failed to create a combined table: {e}')
        

def transform_tocsv_load_to_gcs_bq(year: str = ''):
    logging.info(f'Transforming raw json to csv for year: {year}')

    
    storage_client = gc.storage_client

    bucket_id = gc.bucket_name

    # fetching the bucket we need
    bucket = storage_client.bucket(bucket_id)

    # fetching raw jsons using blob names
    blob_prefix = f'{year}/'
    blobs = bucket.list_blobs(prefix=blob_prefix)

    #logging.info(f'These are the blobs retrived from {bucket_id}: {list(blobs)}')

    processed_records = []

    for blob in blobs:
        if not blob.name.endswith('.json'):
            continue

        # we will first download the raw text
        try:
            content = blob.download_as_text()
            # Creating a valid python object from the raw json string
            cve_data_json = json.loads(content)

            #Passing this into the cve json extractor from parser.py
            record = extract_cvedata(cve_data_json= cve_data_json)

            if record:
                processed_records.append(record)

        except Exception as e:
            logging.error(f'Failed to download blob contents and create a record!: {e}') 

    #logging.info(f'These are the processed records: {processed_records}')

    # add to combined proccessed records
    combined_proccessed_records.extend(processed_records)

    # Use the custom bigquery function to parse the year object as a new table
    gc.csv_bigquery(files = processed_records, year=year)
    

if __name__ == '__main__':
    #years = ['1999', '2000']

    years = get_years()

    for year in years:
        transform_tocsv_load_to_gcs_bq(year)

    create_combined_table(combined_processed_records=combined_proccessed_records)

    


            

