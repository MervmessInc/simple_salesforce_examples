import json
import logging
import pprint
import requests
import sys
import time

from simple_salesforce import Salesforce


logging.basicConfig(format="%(asctime)s : %(message)s", level=logging.ERROR)


def salesforce_login(creds: list) -> Salesforce:
    """salesforce_login(creds: list)

    Args:
        creds (list): Salesforce instance & Bearer Token

    Returns:
        Salesforce: An instance of Salesforce, a handy way to wrap a Salesforce session
                    for easy use of the Salesforce REST API.
    """
    try:
        sf = Salesforce(
            instance=creds[0],
            session_id=creds[1],
            version="55.0",
        )

        sf.is_sandbox()
        return sf

    except Exception as e:
        logging.error(e)
        return None


def get_sobject_fields(sf: Salesforce, object_name: str) -> list:
    """get_sobject_fields(sf: Salesforce, object_name: str)

    Args:
        sf (Salesforce): simple_salesforce.Salesforce object
        object_name (str): Name of Salesforce sObject

    Returns:
        fields: List of fields on the sObject
    """
    fields = []
    describe_url = f"{sf.base_url}sobjects/{object_name}/describe"

    result = requests.request(
        "GET",
        describe_url,
        headers=sf.headers,
    )

    pyObj = result.json()
    for field in pyObj["fields"]:
        if field["type"] not in ["address", "location"]:
            fields.append(field["name"])

    return fields


def build_soql_query(fields: list, object_name: str) -> str:
    """build_soql_query(fields: list, object_name: str)

    Args:
        fields (list): List of fields on the sObject
        object_name (str): Name of Salesforce sObject

    Returns:
        str: SOQL query string
    """
    query = f"SELECT {','.join([str(field) for field in fields])} FROM {object_name}"
    return query


def bulk_query_request(query: str) -> str:
    """bulk_query_request(query: str)

    Args:
        query (str): SOQL query string

    Returns:
        str: Bulk API v2 query request json body
    """
    request_body = json.dumps(
        {
            "operation": "query",
            "query": f"{query}",
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }
    )

    return request_body


def main():
    """
    Example Salesforce batch query. Login using bearer token.
    """
    sf = salesforce_login(
        (
            "",
            "",
        )
    )

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    object_names = [
        "Account",
        "Contact",
    ]

    jobs = []

    for object_name in object_names:
        fields = get_sobject_fields(sf, object_name)
        body = bulk_query_request(build_soql_query(fields, object_name))

        bulk_v2_query_url = f"{sf.base_url}jobs/query"

        result = requests.request(
            "POST",
            bulk_v2_query_url,
            headers=sf.headers,
            data=body,
        )
        pyObj = result.json()
        jobs.append(
            {
                "object_name": object_name,
                "job_id": pyObj["id"],
                "job_state": pyObj["state"],
            }
        )
        job_state = "UploadComplete"

    while job_state == "UploadComplete" or job_state == "InProgress":
        time.sleep(10)
        for job in jobs:
            if job["job_state"] == "UploadComplete" or job["job_state"] == "InProgress":
                result = requests.request(
                    "GET",
                    f"{bulk_v2_query_url}/{job['job_id']}",
                    headers=sf.headers,
                )
                pyObj = result.json()
                pprint.pprint(pyObj)
                job["job_state"] = pyObj["state"]
                job_state = pyObj["state"]
                if job_state == "UploadComplete" or job_state == "InProgress":
                    break

    for job in jobs:
        if job["job_state"] != "JobComplete":
            print(f"\n*** Job ({job['object_name']}) Unsuccessful ***\n")

        else:
            result = requests.request(
                "GET",
                f"{bulk_v2_query_url}/{job['job_id']}/results",
                headers=sf.headers,
            )

            row_count = result.headers["Sforce-NumberOfRecords"]

            csv = result.text

            with open(f"{job['object_name']}.csv", "w", encoding="utf-8") as file:
                file.write(csv)

            line_count = 0
            for line in csv.splitlines():
                line_count += 1

            print(f"\n{job['object_name']} - {row_count} : {line_count}")


if __name__ == "__main__":
    main()
