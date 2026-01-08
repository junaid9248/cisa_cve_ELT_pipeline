import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from typing import List
import logging
import os
logging.basicConfig(level=logging.INFO)
#If not available locally will not execute
load_dotenv(override=True)

def fetch_all_years() -> List:
    base_raws_url = "https://api.github.com"
    repo_owner = "cisagov"
    repo_name = "vulnrichment"

    retry_strategy = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
    adapter = HTTPAdapter(
            max_retries= retry_strategy,
        )

    session = requests.Session()
    session.mount('https://', adapter=adapter)

    GH_TOKEN = os.environ.get('GH_TOKEN')

    if GH_TOKEN:
            # Add token to self.headers then update the header to current sessoion by usung update method
            session.headers.update({
                'User-Agent': 'CISA-Vulnrichment-Extractor/1.0',
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f'token {GH_TOKEN}'})
            logging.info('GitHub token for authentication was found and used to establish session')
    else:
        logging.warning(" No GitHub token found")

    #get all years
    fetch_url = f"{base_raws_url}/repos/{repo_owner}/{repo_name}/contents"
    try:
        response = session.get(fetch_url)
        if response.status_code == 200:
            data = response.json()
            years = []

            for item in data:
                if item['type'] == 'dir' and item['name'] not in ['.github', 'assets']:
                    years.append(item['name'])
            logging.info(f"Number of available years: {len(years)}")
            return  years, len(years)
      
    except requests.RequestException as e:
        logging.error(f"Error fetching years: {e}")
        return []



if __name__ == '__main__':
    fetch_all_years()
    