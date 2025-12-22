import logging
import pandas as pd
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from src.gc import GoogleClient as gc

def test_table():
    #Initialize the GoogleClient instance
    googleclient = gc()

    #Access the BigQuery client
    bigquery_client = googleclient.bigquery_client

    try:
        dataset_id = 'cisa-cve-data-pipeline.cve_all'
        table_id = dataset_id + '.cve_combined_table'
        
        test_query = f'''
            SELECT cve_id FROM `{table_id}`
            WHERE regexp_contains(cve_id, r"^CVE-1999-") AND REGEXP_CONTAINS(cve_id, r"^CVE-2000-")
        '''

        query_job = bigquery_client.query(test_query)
        # The returned object contains errors if the job fails.
        results = query_job.result()

        result_df = results.to_dataframe()

        if not result_df.empty:
            logging.info(f'Dataset {dataset_id} contains CVE IDs from 1999 or 2000, which is expected.')
    except Exception as e:
        logging.error(f"Failed to access BigQuery table: {e}")

if __name__ == "__main__":
    test_table()
