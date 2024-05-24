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

