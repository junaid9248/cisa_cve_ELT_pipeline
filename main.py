import sys
import logging

from src.extract import cveExtractor
from src.config import IS_LOCAL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':

    if len(sys.argv) > 1:

        IS_LOCAL = False
        
        logging.info(f"Extraction has started in {'Local' if IS_LOCAL else 'Google cloud'} mode")
        years = list(sys.argv[2].split(','))

        extractor = cveExtractor(IS_LOCAL)
        extractor.run(years)
    
    else:
        extractor = cveExtractor(IS_LOCAL)
        extractor.run()


