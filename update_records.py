import logging
import os
import pprint
import simple_salesforce.exceptions as sf_exceptions
import sys
import uuid

from dotenv import load_dotenv
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
load_dotenv()
sf_credentials = (
    f"{os.getenv('SALESFORCE_INSTANCE_URL')}",
    f"{os.getenv('SALESFORCE_TOKEN')}",
)

custom_field_name = "Archive_Id"


def update(sf: Salesforce):

    custom_objects = []
    s_out = []
    s_err = []
    row_set = []

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
                    data = []
                    for record in query_result["records"]:
                        guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, record["Id"]))
                        data.append(
                            {"Id": record["Id"], f"{custom_field_name}__c": guid}
                        )

                    row_set.append({"sobject": sobj["name"], "data": data})

            except sf_exceptions.SalesforceMalformedRequest as e:
                s_err.append({"sobject": sobj["name"], "error": e})
                # raise

        pbar.close()

    if row_set:
        pbar = tqdm(total=len(row_set), desc="sf update", ncols=100)

        for row in row_set:
            pbar.update(1)

            try:
                results = sf.bulk.__getattr__(row["sobject"]).update(
                    row["data"], batch_size=10000
                )
                s_out.append({"sobject": row["sobject"], "results": results})

            except Exception as e:
                s_err.append(
                    {"sobject": row["sobject"], "error": e, "data": row["data"]}
                )

        pbar.close()

    print("\n*** Processing completed ***\n")
    for msg in s_out:
        pretty = pprint.pformat(msg["results"], indent=2, width=80)
        logging.info(f"sObject: {msg['sobject']} ~\n{pretty}")

    print("\n*** Errors ***\n")
    for message in s_err:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    update(sf)
