import json
import logging
import requests
import sys

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
sf_credentials = (
    "",
    "",
)

custom_field_name = "Archive_Id"


def build_custom_field(sf: Salesforce, object_name: str):
    """build_custom_field(sf: Salesforce, object_name: str)

    Args:
        sf (Salesforce): simple_salesforce Salesforce Client
        object_name (str): name of the sObject

    Returns:
       custom_field: sf.mdapi.CustomField
    """
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


def create_custom_field(sf: Salesforce, object_name: str):
    """create_custom_field(sf: Salesforce, object_name: str)

    Args:
        sf (Salesforce): simple_salesforce Salesforce Client
        object_name (str): name of the sObject
    """
    meta_field = build_custom_field(sf, object_name)
    sf.mdapi.CustomField.create(meta_field)


def delete_custom_field(sf: Salesforce, object_name: str):
    """delete_custom_field(sf: Salesforce, object_name: str)

    Args:
        sf (Salesforce): simple_salesforce Salesforce Client
        object_name (str): name of the sObject
    """
    sf.mdapi.CustomField.delete(f"{object_name}.{custom_field_name}__c")


def update_object(sf: Salesforce):

    custom_objects = []
    s_out = []

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

    for message in s_out:
        logging.error(f"ERROR ~ {str(message['error']).strip()}")

    for message in custom_objects:
        logging.info(f"INFO ~ Processed {str(message['name']).strip()}")


if __name__ == "__main__":
    sf = login(sf_credentials)

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    update_object(sf)
