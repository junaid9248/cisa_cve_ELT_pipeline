from google.cloud import storage
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

        #Defining the google client with credentials and project id 
        self.client = storage.Client(credentials=self.credentials, project= self.projectID)
        
        self.bucket_name = bucket_name

        # Retrieving the bucket through it's name 
        self.bucket= self.client.bucket(self.bucket_name) 

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
            



