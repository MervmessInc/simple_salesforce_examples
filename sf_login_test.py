import logging
import sys

from simple_salesforce import Salesforce

logging.basicConfig(format="%(asctime)s : %(message)s", level=logging.DEBUG)

if __name__ == "__main__":
    try:
        sf = Salesforce(
            username="test-z1upabcxzffh@example.com",
            password="",
            security_token="4MpoIX7SwcHZLZgHslVOFcRU",
            domain="test",
        )
        print("\n*** Logged into Salesforce ***")
        sys.exit(0)

    except Exception as e:
        logging.debug(f"Error : {e.message}")
        print("\n*** Not Logged into Salesforce ***")
        sys.exit(1)
