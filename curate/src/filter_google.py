import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

class GoogleSheetWriter:
    def __init__(self, creds_path, spreadsheet_id):
        self.creds_path = creds_path
        self.spreadsheet_id = spreadsheet_id
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.creds_path, self.scope)
        self.gc = gspread.authorize(self.credentials)

    def write_dataframe_to_sheet(self, dataframe, sheet_name):
        try:
            spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # If the sheet doesn't exist, create a new one
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)

        # Replace NaN values with an empty string
        dataframe = dataframe.fillna('')

        # Update the worksheet with the DataFrame data
        worksheet.clear()
        worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

def main():
    # Load environment variables from .env file
    load_dotenv()

    # Read the CSV files
    output_dir = os.getenv("OUTPUT_DIR")
    curate_list_path = os.path.join(output_dir, "raw_list.csv")
    atr_path = os.path.join(output_dir, "atr.csv")

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

    # Write filtered_curate_list to Google Sheet
    sheet_writer = GoogleSheetWriter(creds_path, spreadsheet_id)
    sheet_writer.write_dataframe_to_sheet(filtered_curate_list, "hit_list")

    print("Filtered data pushed to Google Sheet.")

if __name__ == "__main__":
    main()

