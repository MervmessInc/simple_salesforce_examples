import logging
import simple_salesforce.exceptions as sf_exceptions
import sys

from simple_salesforce import Salesforce, format_soql
from tqdm import tqdm

from utils import salesforce_login as login

logging.basicConfig(format="%(asctime)s : %(message)s", level=logging.ERROR)

#
# sf_credentials = (
#    "<instance_url>",
#    "<session_id>",
# )
#
sf_credentials = (
    "",
    "",
)

custom_field_name = "IM_Config_Library_Id"


def query(sf: Salesforce):

    custom_objects = []
    s_out = []
    s_err = []

    describe_response = sf.describe()

    for sobj in describe_response["sobjects"]:
        if sobj["custom"] and sobj["name"][-3:] == "__c":
            custom_objects.append({"name": sobj["name"]})

    if custom_objects:
        pbar = tqdm(total=len(custom_objects), desc="sf query", ncols=100)

        for sobj in custom_objects:
            pbar.update(1)

            try:
                query_str = format_soql(
                    f"SELECT Id, Name, {custom_field_name}__c FROM {sobj['name']}"
                )

                query_result = sf.query(query_str)
                if query_result["totalSize"] > 0:
                    for record in query_result["records"]:
                        s_out.append(
                            f"{sobj['name']} ~ Id: {record['Id']}, Name: {record['Name']}, GUID: {record[f'{custom_field_name}__c']}"
                        )

            except sf_exceptions.SalesforceMalformedRequest as e:
                s_err.append({"sobject": sobj["name"], "error": e})
                # raise

        pbar.close()

    with open(f"{custom_field_name}_query.txt", "w", encoding="utf-8") as f:
        for message in s_out:
            logging.info(f"INFO ~ {message}")
            f.write(f"{message}\n")

    print("\n*** Errors ***\n")
    for message in s_err:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    query(sf)
