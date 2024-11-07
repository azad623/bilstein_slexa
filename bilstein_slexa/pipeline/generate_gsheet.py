import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from bilstein_slexa import g_sheet_schema_path, service_account_path
from datetime import datetime
import json
import numpy as np

# Load credentials
credentials = Credentials.from_service_account_file(service_account_path)
SCOPE = credentials.with_scopes(
    [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
)
gc = gspread.authorize(SCOPE)
sheet_service = build("sheets", "v4", credentials=SCOPE)
drive_service = build("drive", "v3", credentials=SCOPE)


# Load JSON mapping
def load_column_mapping():
    with open(g_sheet_schema_path, "r") as file:
        mapping = json.load(file)
    return mapping


def get_existing_sheet_by_name(gc, name):
    """Search for an existing Google Sheet with a specific name."""
    try:
        for sheet in gc.list_spreadsheet_files():
            if sheet["name"] == name:
                return gc.open_by_key(sheet["id"])
    except Exception as e:
        print(f"Error finding sheet by name: {e}")
    return None


# Reorder DataFrame columns based on mapping and add missing columns
def order_columns(df, mapping):
    ordered_columns = []
    for column in mapping["columns"]:
        new_name = column["name"]
        old_name = column["map"]
        # If old_name exists, rename it to new_name; else, create a blank column
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
        else:
            df[new_name] = ""
        ordered_columns.append(new_name)
    return df[ordered_columns]  # Return the DataFrame with ordered columns


def get_column_letter(n):
    """Convert a zero-based column index to an Excel-style column letter."""
    column_letter = ""
    while n >= 0:
        column_letter = chr((n % 26) + 65) + column_letter
        n = n // 26 - 1
    return column_letter


# Create Google Sheet and upload DataFrame
def upload_to_google_sheet(df, folder_id, sheet_name, header_color=(0.5, 0.7, 0.9)):

    # Create a new Google Sheet in the specified folder
    spreadsheet = get_existing_sheet_by_name(gc, sheet_name)

    if spreadsheet:
        print(f"Found existing sheet with name '{sheet_name}'. Updating it.")
        worksheet = spreadsheet.get_worksheet(0)
    else:
        spreadsheet = gc.create(sheet_name)
        spreadsheet.share(
            "gsheet-account-service@azadsandbox-437909.iam.gserviceaccount.com",
            perm_type="user",
            role="writer",
        )
        worksheet = spreadsheet.get_worksheet(0)
    try:
        # Move the Google Sheet to the shared folder
        drive_service.files().update(
            fileId=spreadsheet.id, addParents=folder_id, fields="id, parents"
        ).execute()

        # Get the first worksheet and update it with the DataFrame data
        # Replace NaN, Infinity, and -Infinity with an empty string
        df.replace([np.nan, np.inf, -np.inf], "", inplace=True)
        worksheet = spreadsheet.get_worksheet(0)
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

        # Apply header color and style
        last_column_letter = get_column_letter(
            len(df.columns) - 1
        )  # Adjusts range based on number of columns
        header_range = f"A1:{last_column_letter}1"
        worksheet.format(
            header_range,
            {
                "backgroundColor": {
                    "red": header_color[0],
                    "green": header_color[1],
                    "blue": header_color[2],
                },
                "textFormat": {"bold": True, "fontSize": 13},
                "horizontalAlignment": "CENTER",
            },
        )

        requests = [
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1,
                    },
                    "properties": {"pixelSize": 150},
                    "fields": "pixelSize",
                }
            }
            for i in range(len(df.columns))
        ]
        sheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet.id, body={"requests": requests}
        ).execute()

        # Get and print the link to the new sheet
        sheet_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}"
        print(f"New sheet created: {sheet_link}")
        return sheet_link

    except HttpError as error:
        # Log the error and provide helpful feedback
        print(f"An error occurred: {error}")
        print("Check that the folder ID and file ID are correct and accessible.")
        return None


# Main function
def get_gsheet_url(df, folder_id):
    # Load the column mapping JSON
    column_mapping = load_column_mapping()

    # Order DataFrame columns
    df_ordered = order_columns(df, column_mapping)

    # Define the sheet name with date
    sheet_name = f"Bilstein_Report_{datetime.now().strftime('%Y-%m-%d')}"

    # Upload DataFrame to Google Sheet and format headers
    sheet_url = upload_to_google_sheet(df_ordered, folder_id, sheet_name)

    return sheet_url
