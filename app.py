import logging
import os
import pprint
import sys

from dotenv import load_dotenv
from simple_salesforce import Salesforce

from mdapi_update_object import update_object
from mdapi_update_permission_set import update_permission_set
from update_records import update
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


def main(sf: Salesforce):

    try:
        print("\n*** Update Objects ***\n")
        update_object(sf)

    except Exception as e:
        pprint.pprint(e)
        # sys.exit(1)

    try:
        print("\n*** Update Permission Set ***\n")
        update_permission_set(sf)

    except Exception as e:
        pprint.pprint(e)
        sys.exit(1)

    try:
        print("\n*** Update Records ***\n")
        update(sf)

    except Exception as e:
        pprint.pprint(e)


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    main(sf)
