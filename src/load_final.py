import logging
logging.basicConfig(level=logging.INFO)

import json 
import pandas as pd
from typing import Dict, List, Optional

from .gc import GoogleClient

merge_query = f'''

    MERGE `{gc.projectID}.cve_all.cve_combined_final_table` 
    as target
    USING (
            cve_id,
            published_date,
            updated_date,
            cisa_kev,
            cisa_kev_date,
            cvss_version,
            base_score,
            base_severity,
            attack_vector,
            attack_complexity,
            privileges_required,
            user_interaction,
            scope,
            confidentiality_impact,
            integrity_impact,
            availability_impact,
            ssvc_timestamp,
            ssvc_exploitation,
            ssvc_automatable,
            ssvc_technical_impact,
            ssvc_decision,
            impacted_vendor,
            impacted_products,
            vulnerable_versions,
            cwe_number,
            cwe_description

FROM `{gc.projectID}.cve_all.cve_combined_staging_table`
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cve_id
    ORDER BY updated_date DESC, published_date DESC
) = 1
) as source
ON target.cve_id == source.cve_id


WHEN MATCHED THEN
    UPDATE SET
        published_date = source.published_date,
        updated_date = source.updated_date,
        cisa_kev = source.cisa_kev,
        cisa_kev_date = source.cisa_kev_date,
        cvss_version = source.cvss_version,
        base_score = source.base_score,
        base_severity = source.base_severity,
        attack_vector = source.attack_vector,
        attack_complexity = source.attack_complexity,
        privileges_required = source.privileges_required,
        user_interaction = source.user_interaction,
        scope = source.scope,
        confidentiality_impact = source.confidentiality_impact,
        integrity_impact = source.integrity_impact,
        availability_impact = source.availability_impact,
        ssvc_timestamp = source.ssvc_timestamp,
        ssvc_exploitation = source.ssvc_exploitation,
        ssvc_automatable = source.ssvc_automatable,
        ssvc_technical_impact = source.ssvc_technical_impact,
        ssvc_decision = source.ssvc_decision,
        impacted_vendor = source.impacted_vendor,
        impacted_products = source.impacted_products,
        vulnerable_versions = source.vulnerable_versions,
        cwe_number = source.cwe_number,
        cwe_description = source.cwe_description

WHEN NOT MATCHED THEN
    INSERT(
        cve_id,
        published_date,
        updated_date,
        cisa_kev,
        cisa_kev_date,
        cvss_version,
        base_score,
        base_severity,
        attack_vector,
        attack_complexity,
        privileges_required,
        user_interaction,
        scope,
        confidentiality_impact,
        integrity_impact,
        availability_impact,
        ssvc_timestamp,
        ssvc_exploitation,
        ssvc_automatable,
        ssvc_technical_impact,
        ssvc_decision,
        impacted_vendor,
        impacted_products,
        vulnerable_versions,
        cwe_number,
        cwe_description)

    VALUES (
        source.cve_id,
        source.published source.cve_id.updated_date,
        source.cisa_kev,
        source.cisa_kev_date,
        source.cvss_version,
        source.base_score,
        source.base_severity,
        source.attack_vector,
        source.attack_complexity,
        source.privileges_required,
        source.user_interaction,
        source.scope,
        source.confidentiality_impact,
        source.integrity_impact,
        source.availability_impact,
        source.ssvc_timestamp,
        source.ssvc_exploitation,
        source.ssvc_automatable,
        source.ssvc_technical_impact,
        source.ssvc_decision,
        source.impacted_vendor,
        source.impacted_products,
        source.vulnerable_versions,
        source.cwe_number,
        source.cwe_description);

'''

def main(merge_query: str) -> None:
    gc = GoogleClient()

    try:
        logging.info("BigQuery_client initialized and starting table for merging...")
        gc.combined_final_table_bigquery(query=merge_query)
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == '__main__':
    main(merge_query=merge_query)