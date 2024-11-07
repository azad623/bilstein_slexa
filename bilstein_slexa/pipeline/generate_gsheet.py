import pandas as pd
import gspread
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from bilstein_slexa import g_sheet_schema_path, service_account_path
from datetime import datetime
import json
import numpy as np


# Load JSON mapping
def load_column_mapping():
    with open(g_sheet_schema_path, "r") as file:
        mapping = json.load(file)
    return mapping["columns"]


# Rename DataFrame columns based on JSON mapping
def rename_columns(df, column_mapping):
    for col in column_mapping:
        old_name = col.get("map")
        new_name = col["name"]
        if old_name in df.columns:
            df.rename(columns={old_name: new_name}, inplace=True)
        else:
            # Create a new column if 'map' is empty
            df[new_name] = ""

    return df


# Load credentials
credentials = Credentials.from_service_account_file(service_account_path)
scoped_credentials = credentials.with_scopes(
    [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
)
gc = gspread.authorize(scoped_credentials)


# Create Google Sheet and upload DataFrame
def upload_to_google_sheet(df, folder_id, sheet_name, header_color=(0.5, 0.7, 0.9)):

    # Create a new Google Sheet in the specified folder
    spreadsheet = gc.create(sheet_name)
    spreadsheet.share(
        "gsheet-account-service@azadsandbox-437909.iam.gserviceaccount.com",
        perm_type="user",
        role="writer",
    )

    # Move the file to a specific folder in Google Drive
    drive_service = build("drive", "v3", credentials=scoped_credentials)

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

        # Apply header color
        header_range = f"A1:{chr(64 + len(df.columns))}1"  # Adjusts range based on number of columns
        worksheet.format(
            header_range,
            {
                "backgroundColor": {
                    "red": header_color[0],
                    "green": header_color[1],
                    "blue": header_color[2],
                }
            },
        )

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
def get_gsheet_ur(df, folder_id):
    # Load the column mapping JSON
    column_mapping = load_column_mapping()

    # Rename columns in the DataFrame
    df = rename_columns(df, column_mapping)

    # Define the sheet name with date
    sheet_name = f"Bilstein_Report_{datetime.now().strftime('%Y-%m-%d')}"

    # Upload DataFrame to Google Sheet and format headers
    sheet_url = upload_to_google_sheet(df, folder_id, sheet_name)

    return sheet_url
