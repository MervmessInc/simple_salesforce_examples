import sys
import os
from dotenv import load_dotenv
from utils import salesforce_login as login
from history_tracking import get_history_tracked_fields

# Load environment variables for Salesforce login
load_dotenv()
sf_credentials = (
    f"{os.getenv('SALESFORCE_INSTANCE_URL')}",
    f"{os.getenv('SALESFORCE_TOKEN')}",
)


def main():
    """
    Example script demonstrating how to use the history_tracking module
    to fetch all fields that have history tracking enabled.
    """
    print("Logging into Salesforce...")
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Successfully Logged into Salesforce ***\n")

    # Demonstrate using the new module for a specific object
    object_to_query = "Account"
    print(f"Calling get_history_tracked_fields() for {object_to_query}...")
    fields = get_history_tracked_fields(sf, object_name=object_to_query)

    if fields:
        print(f"\nExample Output - First 5 tracked fields for {object_to_query}:")
        for f in fields[:5]:
            print(f"  - Object: {f.get('Entity')}, Field: {f.get('Field')}")

        print(
            f"\nNote: The full results have also been saved to 'history_tracked_fields_{object_to_query}.csv' by the module."
        )
    else:
        print(
            f"\nNo history tracked fields were found for {object_to_query} in this org."
        )


if __name__ == "__main__":
    main()
