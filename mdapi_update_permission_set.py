import json
import logging
import requests
import simple_salesforce.exceptions as sf_exceptions
import sys

from simple_salesforce import Salesforce, format_soql


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
        field=f"{object_name}.{custom_field_name}",
        readable=True,
    )
    return field_permissions


def main():
    sf = salesforce_login(sf_credentials)

    custom_objects = []
    s_out = []

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    r = requests.get(f"{sf.base_url}sobjects", headers=sf.headers)
    pyObj = json.loads(r.content)

    for sobj in pyObj["sobjects"]:
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
AND Name = 'Update_Archive_Id_c'
"""
        )
        permission_sets = sf.query(query_str)

    except sf_exceptions.SalesforceMalformedRequest:
        raise

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
            s_out.append({"permissionset": permission_set["fullName"], "error": e})
            # raise

    for message in s_out:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")


if __name__ == "__main__":
    main()
