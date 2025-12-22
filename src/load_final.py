import logging
logging.basicConfig(level=logging.INFO)

import json 
import pandas as pd
from typing import Dict, List, Optional

from .gc import GoogleClient


if __name__ == '__main__':
    gc = GoogleClient()

    query = f'''
                 '''
    
    gc.combined_final_table_bigquery()
