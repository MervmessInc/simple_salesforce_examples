import json
import os
import requests
import simple_salesforce.exceptions as sf_exceptions
import sys
import tabulate as tb
import xlsxwriter

from dotenv import load_dotenv
from simple_salesforce import Salesforce, format_soql
from tqdm import tqdm

#
# sf_credentials = (
#    "<instance_url>",
#    "<session_id>",
# )
#
load_dotenv()
sf1_credentials = (
    f"{os.getenv('SALESFORCE_INSTANCE_URL')}",
    f"{os.getenv('SALESFORCE_TOKEN')}",
)

sf2_credentials = (
    "",
    "",
)


def salesforce_login(creds1: list, creds2: list):
    """salesforce_login(creds1: list, creds2: list)

    Args:
        creds1 (list): sf_credentials
        creds2 (list): sf_credentials

    Returns:
       sf1, sf2 (Salesforce): simple_salesforce Salesforce Client
    """

    sf1 = Salesforce(
        instance=creds1[0],
        session_id=creds1[1],
    )

    sf2 = Salesforce(
        instance=creds2[0],
        session_id=creds2[1],
    )

    # Test to see if we have a working session.
    try:
        sf1.is_sandbox()
        return sf1, sf2
    except Exception:
        return None, None


def write_worksheet(book: xlsxwriter.Workbook, sheet: str, sObjects: list):
    """write_worksheet(book: xlsxwriter.Workbook, sheet: str, sObjects: list)

    Args:
        book (xlsxwriter.Workbook): excel workbook
        sheet (str): excel sheet title
        sObjects (list): list of Salesforce custom objects with row counts.
    """

    # Add a format. Light red fill with dark red text.
    red_format = book.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
    # Add a format. Green fill with dark green text.
    green_format = book.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})

    header = ("Name", "Row Count", "Compare")

    work_sheet = book.add_worksheet(sheet)

    x = 0
    y = 0
    for lbl in header:
        work_sheet.write(x, y, lbl)
        y += 1

    x = 1
    y = 0

    for sObj in sObjects:
        if sObj.get("count") == sObj.get("compare"):
            cell_format = green_format
        else:
            cell_format = red_format

        work_sheet.write(x, y, sObj.get("name"), cell_format)
        work_sheet.write(x, y + 1, sObj.get("count"), cell_format)
        work_sheet.write(x, y + 2, sObj.get("compare"), cell_format)
        cell1 = xlsxwriter.utility.xl_rowcol_to_cell(x, y + 1)
        cell2 = xlsxwriter.utility.xl_rowcol_to_cell(x, y + 2)
        work_sheet.write_formula(x, y + 3, f"={cell1}={cell2}", cell_format)
        y = 0
        x += 1

    work_sheet.autofilter(0, 0, x, 3)


def sf_compare():
    sf, sf2 = salesforce_login(sf1_credentials, sf2_credentials)

    custom_objects = []
    s_out = []

    if not sf:
        print("\n*** Not Logged into Salesforce ***\n")
        sys.exit(1)

    print("\n*** Logged into Salesforce ***\n")

    r = requests.get(f"{sf.base_url}sobjects", headers=sf.headers)

    pyObj = json.loads(r.content)

    for sobj in pyObj["sobjects"]:
        if sobj["custom"] and sobj["name"][-1:] != "e":
            custom_objects.append({"name": sobj["name"], "count": 0, "compare": 0})

    if custom_objects:
        pbar = tqdm(total=len(custom_objects), desc="sf1", ncols=100)

        for sobj in custom_objects:
            pbar.update(1)

            try:
                row_count = sf.query(
                    format_soql(f"SELECT count() FROM {sobj['name']} ")
                )
                sobj["count"] = row_count["totalSize"]

            except sf_exceptions.SalesforceMalformedRequest:
                sobj["count"] = "ERROR"

        pbar.close()

        pbar = tqdm(total=len(custom_objects), desc="sf2", ncols=100)
        for sobj in custom_objects:
            pbar.update(1)

            try:
                row_count = sf2.query(
                    format_soql(f"SELECT count() FROM {sobj['name']} ")
                )
                sobj["compare"] = row_count["totalSize"]

            except sf_exceptions.SalesforceMalformedRequest:
                sobj["compare"] = "ERROR"

            if sobj["count"] != sobj["compare"]:
                s_out.append(sobj)

        pbar.close()

        workbook = xlsxwriter.Workbook(f"{sf.sf_instance}_object_count.xlsx")
        write_worksheet(workbook, "Custom Objects", custom_objects)
        workbook.close()

        print()
        print(f'"{sf.sf_instance}" vs "{sf2.sf_instance}"')
        if s_out:
            print(tb.tabulate(s_out, headers="keys", tablefmt="grid"))
            print()


if __name__ == "__main__":
    sf_compare()
