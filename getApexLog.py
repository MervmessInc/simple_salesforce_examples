import errno
import os
import pandas as pd
import requests
import sys

from dotenv import load_dotenv
from simple_salesforce import Salesforce, format_soql
from utils import salesforce_login as login

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


def getApexLogs(sf: Salesforce, length: int) -> pd.DataFrame:
    query_str = format_soql(
        f"""
SELECT Id, StartTime, Location, LogLength
FROM ApexLog
WHERE LogLength > {length}
AND StartTime = TODAY
ORDER BY LogLength DESC"""
    )

    result = sf.query(query_str)
    df = pd.DataFrame(result["records"])
    df.drop(["attributes"], axis=1, inplace=True)

    return df


def downloadApexLog(sf: Salesforce, apex_log_id: str) -> str:
    # base url
    base_url = sf.sf_instance
    # download url
    download_url = f"https://{base_url}/apexdebug/traceDownload.apexp?id={apex_log_id}"
    # download file
    file = requests.get(download_url, headers=sf.headers)

    return file.content


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    # Get Apex Logs
    df = getApexLogs(sf, 10000)
    if df.empty:
        print("No Apex Logs found.")
        sys.exit(0)

    print(f"Number of Apex Logs: {len(df)}")

    try:
        os.mkdir("apexlogs")
        print("Directory created successfully.")
    except OSError as e:
        if e.errno == errno.EEXIST:
            print("Directory already exists.")
        else:
            raise

    # Get each Apex Log
    for i in range(len(df)):
        apex_log_id = df.loc[i, "Id"]
        apex_log = downloadApexLog(sf, apex_log_id)
        with open(f"./apexlogs/apex_log-{apex_log_id}.txt", "wb") as f:
            f.write(apex_log)

    print(f"Downloaded {len(df)} Apex Logs.")
    print("\n*** Finished ***\n")
