import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the paths for the CSV files from the environment variables
output_dir = os.getenv("OUTPUT_DIR")
curate_list_path = os.path.join(output_dir, "raw_list.csv")
atr_path = os.path.join(output_dir, "atr.csv")
hit_list_path = os.path.join(output_dir, "hit_list.csv")

# Read the CSV files
curate_list = pd.read_csv(curate_list_path)
atr = pd.read_csv(atr_path)

# Convert address column to lowercase in both DataFrames
curate_list["address"] = curate_list["address"].str.lower()
atr["address"] = atr["address"].str.lower()

# Merge curate_list and atr on address column
merged = pd.merge(curate_list, atr, on="address", how="left", indicator=True)

# Filter rows that are only in curate_list
filtered_curate_list = merged[merged["_merge"] == "left_only"]

# Drop the indicator column and reset the index
filtered_curate_list = filtered_curate_list.drop(columns=["_merge"]).reset_index(drop=True)

# Authenticate with Google Sheets
creds_path = os.getenv('GOOGLE_AUTH_JSON_PATH')
spreadsheet_id = os.getenv('SPREADSHEET_ID')

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
gc = gspread.authorize(credentials)

# Write DataFrame to Google Sheet
def write_to_google_sheet(dataframe, sheet_name, gc, spreadsheet_id):
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # If the sheet doesn't exist, create a new one
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
    
    # Replace NaN values with an empty string
    dataframe = dataframe.fillna('')
    
    # Update the worksheet with the DataFrame data
    worksheet.clear()
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

# Write filtered_curate_list to Google Sheet
write_to_google_sheet(filtered_curate_list, "hit_list", gc, spreadsheet_id)

print("Filtered data pushed to Google Sheet.")

