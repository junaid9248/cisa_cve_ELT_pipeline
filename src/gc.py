from google.cloud import storage, bigquery
from google.oauth2 import service_account

import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from src.config import GCLOUD_API_KEY, GCLOUD_BUCKETNAME, GCLOUD_APP_CREDENTIALS, GCLOUD_PROJECTNAME

logging.basicConfig(level = logging.INFO)

class googleClient():

    def __init__(self, bucket_name: str = GCLOUD_BUCKETNAME):

        self.projectID = GCLOUD_PROJECTNAME
        self.credentials = service_account.Credentials.from_service_account_file(GCLOUD_APP_CREDENTIALS)

        #Defining the google storage client and bigquery client with credentials and project id 
        self.storage_client = storage.Client(credentials=self.credentials, project= self.projectID)
        self.bq_client = bigquery.Client(credentials=self.credentials, project=self.projectID)
        
        self.bucket_name = bucket_name

        # Retrieving the bucket through it's name 
        self.bucket= self.storage_client.bucket(self.bucket_name) 

    def upload_blob(self, raw_json, filename):
        try:
            blob = self.bucket.blob(blob_name = filename)

            blob.upload_from_string(json.dumps(raw_json))
            logging.info(f'Uploaded {filename} to {self.bucket_name}')
        except Exception as e:
            logging.error(f'Failed to upload {filename} to {self.bucket_name}: {e}')

    def csv_to_bucket(self, year_data, year: str = ''):

        try:

            df = pd.DataFrame(year_data)
            blob_name = f'cve_csv/cve_data_{year}.csv'

            #Creating a blob with file path for CSVs
            blob = self.bucket.blob(blob_name = blob_name)

            blob.upload_from_string(
                df.to_csv(index = False),
                content_type= 'csv/text'
            )

            logging.info(f'Succesfully upload csv for {year} to GCS bucket {self.bucket_name}')
        
        except Exception as e:
            logging.warning(f'Failed to upload {year} csv to GCS bucket {self.bucket_name}: {e}')

    def csv_bigquery(self, isLocal, files :Dict = {}, year:Optional[str] = '1999'):

        # Check if it is automated mode 
        if isLocal == False:
            dataset_name = 'cve_all_new'
            dataset_table_name = f'cve_{year}_table'

            # Check if desired dataset exists
            if (self.bq_client.get_dataset(f'{self.projectID}.{dataset_name}')):
                # Name 
                dataset_tosend = bigquery.Dataset(dataset_name)
                dataset_tosend.location ='US'

                dataset_recv = self.bq_client.create_dataset(dataset_tosend, timeout= 30)
                print(dataset_recv)

                #If dataset does not exists create a new dataset and then add a new table for the entered years

                # Create a schema for the table
                year_table_schema = [
                    bigquery.SchemaField('cve_id', 'STRING', mode='REQUIRED', description='Unique CVE identifier'),
                    bigquery.SchemaField("published_date", "STRING", description="Date first published"),
                    bigquery.SchemaField("updated_date", "STRING", description="Latest date updated"),
                    bigquery.SchemaField('cisa_kev','STRING', description='If appeared in CISA KEV catalog'),
                    bigquery.SchemaField('cisa_kev_date', 'STRING', description='Date appeared in CISA KEV catalog'),

                    bigquery.SchemaField('cvss_version', 'STRING', description='CVSS version recorded'),

                    bigquery.SchemaField('base_score', 'STRING', description='Base CVSS score for CVE entry'),

                    bigquery.SchemaField('base_severity', 'STRING', description='Severity classiication for CVE entry'),

                    bigquery.SchemaField('attack_vector)', 'STRING', description='Attack vector for '),

                    bigquery.SchemaField('attack_complexity', 'STRING', description='Complexity of attack'),
                    bigquery.SchemaField('privileges_required', 'STRING', description='Level of privillege required'),
                    bigquery.SchemaField('user_interaction', 'STRING', description='Level of user interaction needed'),
                    bigquery.SchemaField('scope', 'STRING', description=''),

                    bigquery.SchemaField('confidentiality_impact', 'STRING', description='If confidentiality of system affected'),
                    bigquery.SchemaField('integrity_impact', 'STRING', description='If integrity of system affected'),
                    bigquery.SchemaField('availability_impact', 'STRING', description='If availability of system affected'),

                    bigquery.SchemaField('ssvc_timestamp', 'STRING', description='Date SSVC score was added'),
                    bigquery.SchemaField('ssvc_exploitation', 'STRING', description='Whether exploitable'),
                    bigquery.SchemaField('ssvc_automatable', 'STRING', description='Wheter automable'),
                    bigquery.SchemaField('ssvc_technical_impact', 'STRING', description='SSVC impact level'),
                    bigquery.SchemaField('ssvc_decision', 'STRING', description='SSVC decision for metrics'),

                    bigquery.SchemaField('impacted_vendor', 'STRING', description='List of vendors impacted'),
                    bigquery.SchemaField('impacted_products', 'STRING', description='List of products impacted'),
                    bigquery.SchemaField('vulnerable_versions', 'STRING', description='List of product versions impacted'),

                    bigquery.SchemaField('cwe_number', 'STRING', description='CWE description number'),
                    bigquery.SchemaField('cwe_description', 'STRING', description='Description of CWE'),

               ]
                
                table = bigquery.Table(dataset_table_name, schema= year_table_schema)

                ret_tab = self.bq_client.create_table(table)

                logging.info
                

            else:
                logging.info(f'Dataset {dataset_name} already exists!')




            
            


        
            



