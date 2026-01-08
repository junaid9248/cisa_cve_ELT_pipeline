import argparse
from src.fetch_years import fetch_all_years
from src.extract2 import cveExtractor
from src.load_raws_bq import ndjson_loader
from typing import Optional, List, Dict
import logging 

logging.basicConfig(level=logging, format='%(asctime)s - %(levelname)s - %(message)s')

def run_elt_pipeline(args):

    #If any years were provided
    if args.testyearslist:
        # If testyears list is provided
        # example: python main.py --local 1999,2000,2001 -> Only gets data for custom list of years in local mode
        # example: python main.py --cloud 1999,2000,2001 -> Only gets data for custom list of years in cloud mode 
        years = [testyear.strip() for testyear in args.testyearslist.split(',')]
        number_of_years = len(years)
        logging.info(f'Starting test mode for years: {years}')
    else:
        years, number_of_years = fetch_all_years()

    if args.cloud:
    #Start extraction script in cloud mode
        logging.info(f'Starting extraction for {number_of_years} years in cloud mode...')
        #If terminal execution was done using --cloud argument then is_local is set to false obviously
        is_local_mode = False
    elif args.local:
        logging.info(f'Starting extraction for {number_of_years} years in local mode...')
        is_local_mode = True

    # STEP 1: Extract the raws and dump ndjson into data lake (GCS bucket)
    extractor = cveExtractor(islocal= is_local_mode)
    extractor.run(years=years)

    # STEP 2: Initialize the loader class and load ndjsons to a cve_raws table
    loader = ndjson_loader(years_list=years)
    loader.load_ndjsons_to_bq()




if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description='Arguments passed to pipeline run function')

        # Argument for local flag that creates a cveExtractor() instance with islocal set to true
    argparser.add_argument('--local', action='store_true', help='Run in local mode and store datasets to dataset_local folder')

    # Argument for local flag that creates a cveExtractor() instance with islocal set to false
    argparser.add_argument('--cloud', action='store_true', help='Run in GC mode and save to cloud storage + bigquery')

    # Argument for custom, reduced list of years passed in either mode for testing purposes 
    argparser.add_argument('testyearslist',
                           nargs='?',
                           default= None,
                           type= str, 
                           help='Comma separated years list passed manually for testing')
    
    args= argparser.parse_args()

    if args:
        run_elt_pipeline(args = args)

