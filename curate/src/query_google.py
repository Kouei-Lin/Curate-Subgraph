import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

class GoogleSheets:
    def __init__(self, creds_path):
        self.gc = self.authenticate_google_sheets(creds_path)

    def authenticate_google_sheets(self, creds_path):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        return gspread.authorize(credentials)

    def write_to_google_sheet(self, dataframe, sheet_name, spreadsheet_id):
        try:
            spreadsheet = self.gc.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # If the sheet doesn't exist, create a new one
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
        
        # Clear existing data
        worksheet.clear()

        # Update worksheet with data
        worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())

class Subgraph:
    def __init__(self, endpoint, api_key):
        self.endpoint = endpoint
        self.api_key = api_key

    def fetch_data(self, registry_address):
        items = []
        offset = 0
        has_more_items = True

        while has_more_items:
            query = f'''
            query {{
              litems(
                orderBy: latestRequestSubmissionTime
                first: 1000
                skip: {offset}
                where: {{
                  registryAddress: "{registry_address}",
                  status: Registered
                }}
              ) {{
                key0
                key1
                key2
                key3
              }}
            }}
            '''
            response = requests.post(self.endpoint, json={'query': query}, headers={'Authorization': f'Bearer {self.api_key}'})

            if response.status_code == 200:
                data = response.json().get('data', {})
                new_items = data.get('litems', [])
                items.extend(new_items)
                if len(new_items) < 1000:
                    has_more_items = False
                else:
                    offset += 1000
                    print(f"Offset: {offset}")  # Print offset
            else:
                print(f"Error: {response.status_code} - {response.text}")
                has_more_items = False

        return items

def extract_chain_and_address(row):
    address = row['key0']
    if address is None:
        chain_id = None
    elif 'eip155' in address:
        parts = address.split(':')
        if len(parts) >= 3:
            chain_id = parts[-2]
            address = parts[-1]
        else:
            chain_id = None
    elif 'solana' in address:
        chain_id = 'solana'
        address = address.split(':')[-1]  # Extract address for Solana
    else:
        chain_id = None
    return pd.Series({'chain_id': chain_id, 'address': address})

# Load environment variables from .env file
load_dotenv()

# Define environment variables
creds_path = os.getenv('GOOGLE_AUTH_JSON_PATH')
spreadsheet_id = os.getenv('SPREADSHEET_ID1')
api_key = os.getenv('GRAPH_API_KEY')
subgraph_endpoint = 'https://gateway-arbitrum.network.thegraph.com/api/[api-key]/subgraphs/id/2mi5zidQdekNdS6ojDj4tnKZe9MiKcseWQGAguTwcvBV'

# Instantiate GoogleSheets and Subgraph classes
google_sheets = GoogleSheets(creds_path)
subgraph = Subgraph(subgraph_endpoint, api_key)

# Define registry addresses
registry_addresses = {
    'atr': os.getenv('XDAI_REGISTRY_ADDRESS_TAGS'),
    'token': os.getenv('XDAI_REGISTRY_TOKENS'),
    'cdn': os.getenv('XDAI_REGISTRY_DOMAINS')
}

# Loop through each registry address
for registry_name, registry_address in registry_addresses.items():
    print(f"Fetching data for {registry_name}...")
    all_registry_data = []

    # Fetch data for the current registry address
    registry_data = subgraph.fetch_data(registry_address)
    all_registry_data.extend(registry_data)

    # Convert data to DataFrame
    df = pd.DataFrame(all_registry_data)

    # Extract chain_id and address
    df[['chain_id', 'address']] = df.apply(extract_chain_and_address, axis=1)

    # Reorder columns
    df = df[['chain_id', 'address', 'key1', 'key2', 'key3']]

    # Write DataFrame to Google Sheet
    google_sheets.write_to_google_sheet(df, registry_name, spreadsheet_id)
    print(f"Data saved to Google Sheet for {registry_name}")

