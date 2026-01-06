from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv
import logging
import os
import argparse
from src.gc import GoogleClient
from src.extract2 import cveExtractor
from time import sleep
googleclient = GoogleClient()

logging.basicConfig(level=logging.INFO)
#If not available locally will not execute
load_dotenv(override=True)

def load_ndjsons_to_bq(year: str = '', isFirstRun: bool = True):
    bucket_id = googleclient.bucket_name

    gcs_ndjsonblob_uri = f"gs://{bucket_id}/NDjson_files/{year}/*.ndjson"
    googleclient.create_fill_raws_table(source_uri=gcs_ndjsonblob_uri, isFirstRun= isFirstRun, year = year)


def run():
    # Creating a argument parser using the argparse library
    argparser = argparse.ArgumentParser(description= 'Transform raw CVE ND json text files into a raw bronze table')

    # Adding years list argument for custom 
    argparser.add_argument('years', 
                           nargs='?',
                           type=str,
                           default=None, 
                           help='Comma separated years list, can be custom list for test purposes or entire list of years using get_years() function from extractor')

    args = argparser.parse_args()

    if args.years:
        # testing
        years = args.years.split(',')
    else:
        # Automated
        extractor = cveExtractor()
        years = extractor.get_years()

    isFirstRun = True
    for year in years:
        sleep(5)
        load_ndjsons_to_bq(year=year, isFirstRun=isFirstRun)
        isFirstRun = False


if __name__ == '__main__':
    run()