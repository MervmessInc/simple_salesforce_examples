import json
import logging
import pprint
import requests
import simple_salesforce.exceptions as sf_exceptions
import sys
import uuid

from simple_salesforce import Salesforce, format_soql
from tqdm import tqdm

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

custom_field_name = "Archive_Id__c"


def salesforce_login(creds: list):
    """salesforce_login(creds: list)

    Args:
        creds (list): sf_credentials

    Returns:
        sf (Salesforce): simple_salesforce Salesforce Client
    """
    sf = Salesforce(
        instance=creds[0],
        session_id=creds[1],
    )

    # Test to see if we have a working session.
    try:
        sf.is_sandbox()
        return sf
    except Exception:
        return None


def main():
    sf = salesforce_login(sf_credentials)

    custom_objects = []
    s_out = []
    s_err = []
    row_set = []

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    r = requests.get(f"{sf.base_url}sobjects", headers=sf.headers)
    pyObj = json.loads(r.content)

    for sobj in pyObj["sobjects"]:
        if sobj["custom"] and sobj["name"][-3:] == "__c":
            custom_objects.append({"name": sobj["name"]})

    if custom_objects:
        pbar = tqdm(total=len(custom_objects), desc="sf query", ncols=100)

        for sobj in custom_objects:
            pbar.update(1)

            try:
                query_str = format_soql(
                    f"SELECT Id, Name, {custom_field_name} FROM {sobj['name']}"
                )

                query_result = sf.query(query_str)
                if query_result["totalSize"] > 0:
                    data = []
                    for record in query_result["records"]:
                        guid = str(uuid.uuid5(uuid.NAMESPACE_DNS, record["Id"]))
                        data.append({"Id": record["Id"], "Archive_Id__c": guid})

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
        print(f"sObject: {msg['sobject']} ~\n{pretty}")

    print("\n*** Errors ***\n")
    for message in s_err:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")


if __name__ == "__main__":
    main()
