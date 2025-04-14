import logging
import pandas as pd
import sys

from dotenv import load_dotenv
from simple_salesforce import Salesforce
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

exclude = []


def describe_object(sf: Salesforce):

    custom_objects = []
    s_err = []
    s_out = []

    describe_response = sf.describe()

    # for sobj in describe_response["sobjects"]:
    #    if (
    #        sobj["custom"]
    #        and sobj["name"][-3:] == "__c"
    #        and not any(substring in sobj["name"] for substring in exclude)
    #    ):
    #        custom_objects.append({"name": sobj["name"]})
    custom_objects.append(
        {"name": "Opportunity"}
    )  # Add Risk_Binder_Section__c to the list of objects to describe
    custom_objects.append(
        {"name": "Quote"}
    )  # Add Premium_Share_Tax_Jur_Level__c to the list of objects to describe
    custom_objects.append(
        {"name": "QuoteLineItem"}
    )  # Add QuoteLineItem to the list of objects to describe
    custom_objects.append(
        {"name": "InsurancePolicy"}
    )  # Add InsurancePolicy to the list of objects to describe

    if custom_objects:
        pbar = tqdm(total=len(custom_objects), desc="sf", ncols=100)

        for row in custom_objects:
            pbar.update(1)

            try:
                describe_response = sf.__getattr__(row["name"]).describe()
                for field in describe_response["fields"]:
                    s_out.append(
                        {
                            "sobject": row["name"],
                            "field": field["name"],
                            "type": field["type"],
                            "length": field["length"],
                            "externalId": field["externalId"],
                            "unique": field["unique"],
                        }
                    )

            except Exception as e:
                s_err.append({"sobject": sobj["name"], "error": e})
                raise

        pbar.close()

    df = pd.DataFrame(s_out)
    df.to_csv("sObject.csv", index=False)

    for message in s_err:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    describe_object(sf)
