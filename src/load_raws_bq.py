from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
import logging
import os
import argparse
from src.gc import GoogleClient
from src.extract2 import cveExtractor
from time import sleep
#googleclient = GoogleClient()

logging.basicConfig(level=logging.INFO)
#If not available locally will not execute
load_dotenv(override=True)

class ndjson_loader():

    def __init__(self, years_list: List = []):
        self.years_list = years_list 
        self.google_client = GoogleClient()
        self.isFirstRun = True

    def load_ndjsons_to_bq(self):
        bucket_id = self.google_client.bucket_name
        for year in self.years_list:
            sleep(3)
            gcs_ndjsonblob_uri = f"gs://{bucket_id}/NDjson_files/{year}/*.ndjson"
            self.googleclient.create_fill_raws_table(source_uri=gcs_ndjsonblob_uri, isFirstRun= self.isFirstRun, year = year)
            self.isFirstRun = False
