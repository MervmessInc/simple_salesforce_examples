import logging
import os
import simple_salesforce.exceptions as sf_exceptions
import sys

from dotenv import load_dotenv
from simple_salesforce import Salesforce, format_soql

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


def build_object_permissions(sf: Salesforce, object_name: str):
    object_permissions = sf.mdapi.PermissionSetObjectPermissions(
        allowCreate=False,
        allowDelete=True,
        allowEdit=True,
        allowRead=True,
        modifyAllRecords=True,
        object=f"{object_name}",
        viewAllRecords=True,
    )
    return object_permissions


def build_field_permissions(sf: Salesforce, object_name: str):
    field_permissions = sf.mdapi.PermissionSetFieldPermissions(
        editable=True,
        field=f"{object_name}.{custom_field_name}__c",
        readable=True,
    )
    return field_permissions


def update_permission_set(sf: Salesforce):

    custom_objects = []
    s_err = []

    describe_response = sf.describe()

    for sobj in describe_response["sobjects"]:
        if (
            sobj["custom"]
            and sobj["name"][-3:] == "__c"
            and sobj["name"] != "fRecruit__Opportunity_Placement__c"
        ):
            custom_objects.append({"name": sobj["name"]})

    try:
        query_str = format_soql(
            f"""
SELECT Id, Name, IsCustom, NamespacePrefix 
From PermissionSet
Where IsCustom = True
AND NamespacePrefix = ''
AND Name = 'Update_{custom_field_name}'
"""
        )
        permission_sets = sf.query(query_str)

    except sf_exceptions.SalesforceMalformedRequest:
        raise

    if not permission_sets["records"]:
        raise Exception("Missing Permission Set")

    for pset in permission_sets["records"]:
        permission_set = sf.mdapi.PermissionSet.read(pset["Name"])
        permission_set["objectPermissions"] = []
        permission_set["fieldPermissions"] = []

        if custom_objects:
            for sobj in custom_objects:
                object_permission = build_object_permissions(sf, sobj["name"])
                field_permission = build_field_permissions(sf, sobj["name"])
                permission_set["objectPermissions"].append(object_permission)
                permission_set["fieldPermissions"].append(field_permission)

        try:
            sf.mdapi.PermissionSet.update(permission_set)

        except Exception as e:
            s_err.append({"permissionset": permission_set["fullName"], "error": e})
            # raise

    if s_err:
        for message in s_err:
            logging.error(f"ERROR ~ {str(message['error']).strip()}")

        raise Exception("Update Permission Set ERROR")


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    update_permission_set(sf)
