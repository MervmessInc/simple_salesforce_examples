import logging
import sys

from simple_salesforce import Salesforce

logging.basicConfig(format="%(asctime)s : %(message)s", level=logging.DEBUG)

if __name__ == "__main__":
    sf = Salesforce(
        username="test-z1upabcxzffh@example.com",
        password="jdkw)iXqzzv8g",
        security_token="4MpoIX7SwcHZLZgHslVOFcRU",
        domain="test",
    )
    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")
