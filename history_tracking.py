import logging
import os
import sys
import urllib.parse
import pandas as pd

from dotenv import load_dotenv
from simple_salesforce import Salesforce
from utils import salesforce_login as login

logging.basicConfig(format="%(asctime)s : %(message)s", level=logging.ERROR)

load_dotenv()
sf_credentials = (
    f"{os.getenv('SALESFORCE_INSTANCE_URL')}",
    f"{os.getenv('SALESFORCE_TOKEN')}",
)

def get_history_tracked_fields(sf: Salesforce, object_name: str = "Account"):
    """
    Fetches all fields for a given object that have history tracking enabled.
    Note: Tooling API requires filtering FieldDefinition by EntityDefinitionId.
    """
    query = f"SELECT EntityDefinition.QualifiedApiName, QualifiedApiName FROM FieldDefinition WHERE EntityDefinitionId = '{object_name}' AND IsFieldHistoryTracked = TRUE"
    encoded_query = urllib.parse.quote(query)
    
    try:
        result = sf.toolingexecute(f"query?q={encoded_query}")
        records = result.get('records', [])
        
        if records:
            out = []
            for record in records:
                entity_name = record.get('EntityDefinition', {}).get('QualifiedApiName') if record.get('EntityDefinition') else None
                field_name = record.get('QualifiedApiName')
                out.append({
                    "Entity": entity_name,
                    "Field": field_name
                })
            
            df = pd.DataFrame(out)
            output_file = f"history_tracked_fields_{object_name}.csv"
            df.to_csv(output_file, index=False)
            print(f"Successfully exported {len(records)} history tracked fields for {object_name} to {output_file}")
            return out
        else:
            print(f"No fields with history tracking enabled were found for {object_name}.")
            return []
            
    except Exception as e:
        logging.error(f"Failed to query Tooling API: {e}")
        raise

if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")
    get_history_tracked_fields(sf)
