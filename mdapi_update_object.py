import json
import logging
import requests
import sys

from pprint import pprint
from simple_salesforce import Salesforce
from tqdm import tqdm

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.ERROR)

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

custom_field_name = "Archive_Id"


def build_custom_field(sf: Salesforce, object_name: str):
    custom_field = sf.mdapi.CustomField(
        fullName=f"{object_name}.{custom_field_name}__c",
        businessOwnerGroup=None,
        businessOwnerUser=None,
        businessStatus=None,
        caseSensitive=True,
        complianceGroup=None,
        customDataType=None,
        defaultValue=None,
        deleteConstraint=None,
        deprecated=None,
        description=None,
        displayFormat=None,
        encryptionScheme=None,
        escapeMarkup=None,
        externalDeveloperName=None,
        externalId=True,
        fieldManageability=None,
        formula=None,
        formulaTreatBlanksAs=None,
        inlineHelpText=None,
        isAIPredictionField=None,
        isConvertLeadDisabled=None,
        isFilteringDisabled=None,
        isNameField=None,
        isSortingDisabled=None,
        label=f"{custom_field_name}",
        length=100,
        lookupFilter=None,
        maskChar=None,
        maskType=None,
        metadataRelationshipControllingField=None,
        mktDataLakeFieldAttributes=None,
        mktDataModelFieldAttributes=None,
        populateExistingRows=None,
        precision=None,
        referenceTargetField=None,
        referenceTo=None,
        relationshipLabel=None,
        relationshipName=None,
        relationshipOrder=None,
        reparentableMasterDetail=None,
        required=False,
        restrictedAdminField=None,
        scale=None,
        securityClassification=None,
        startingNumber=None,
        stripMarkup=None,
        summarizedField=None,
        summaryFilterItems=[],
        summaryForeignKey=None,
        summaryOperation=None,
        trackFeedHistory=None,
        trackHistory=None,
        trackTrending=False,
        translateData=None,
        type=sf.mdapi.FieldType("Text"),
        unique=True,
        valueSet=None,
        visibleLines=None,
        writeRequiresMasterRead=None,
    )
    return custom_field


def salesforce_login(creds: list):
    """salesforce_login()

    Returns:
        simple_salesforce.Salesforce: sf Salesforce client objects.
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


def create_custom_field(sf: Salesforce, object_name: str):
    meta_field = build_custom_field(sf, object_name)
    sf.mdapi.CustomField.create(meta_field)


def delete_custom_field(sf: Salesforce, object_name: str):
    # custom_field = sf.mdapi.CustomField.read(f"{object_name}.{custom_field_name}__c")
    # pprint(custom_field)
    sf.mdapi.CustomField.delete(f"{object_name}.{custom_field_name}__c")


def main():
    sf = salesforce_login(sf_credentials)

    custom_objects = []
    s_out = []

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
        pbar = tqdm(total=len(custom_objects), desc="sf", ncols=100)

        for sobj in custom_objects:
            pbar.update(1)
            try:
                create_custom_field(sf, sobj["name"])
                # delete_custom_field(sf, sobj["name"])
            except Exception as e:
                s_out.append({"sobject": sobj["name"], "error": e})
                # raise

        pbar.close()

    pprint(s_out)
    # pprint(custom_objects)


if __name__ == "__main__":
    main()
