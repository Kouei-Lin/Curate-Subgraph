import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import requests
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables from .env file
load_dotenv()

# Function to run subgraph.py
def run_subgraph():
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
                      status_in: ["Registered", "RegistrationRequested"]
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

    # Define environment variables
    api_key = os.getenv('GRAPH_API_KEY')
    subgraph_endpoint = 'https://gateway-arbitrum.network.thegraph.com/api/[api-key]/subgraphs/id/2mi5zidQdekNdS6ojDj4tnKZe9MiKcseWQGAguTwcvBV'

    # Define registry addresses
    registry_addresses = {
        'atr': os.getenv('XDAI_REGISTRY_ADDRESS_TAGS'),
        'token': os.getenv('XDAI_REGISTRY_TOKENS'),
        'cdn': os.getenv('XDAI_REGISTRY_DOMAINS')
    }

    # Create a directory to save CSV files if it doesn't exist
    output_dir = os.getenv('OUTPUT_DIR')
    os.makedirs(output_dir, exist_ok=True)

    # Instantiate Subgraph class
    subgraph = Subgraph(subgraph_endpoint, api_key)

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

        # Save DataFrame to CSV
        output_file = os.path.join(output_dir, f"{registry_name}.csv")
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

# Function to run dune.py
def run_dune():
    class Dune:
        def __init__(self, blockchains, output_dir, query_id, creds_path):
            self.blockchains = blockchains
            self.output_dir = output_dir
            self.query_id = query_id
            self.creds_path = creds_path

        def run_query(self):
            # Load environment variables from .env file
            load_dotenv()

            # Initialize DuneClient with the API key loaded from environment variables
            dune_api_key = os.getenv("DUNE_API_KEY")
            dune = DuneClient(api_key=dune_api_key)

            # Loop over each blockchain
            for blockchain, chain_id in self.blockchains.items():
                # Define the query parameters
                params = [
                    QueryParameter.text_type(name="blockchain", value=blockchain),
                    QueryParameter.text_type(name="chain_id", value=chain_id),
                ]
                
                # Define the query
                query = QueryBase(
                    name=f"Query for {blockchain}",
                    query_id=self.query_id,
                    params=params,
                )
                
                print(f"Running query for {blockchain}...")
                
                # Run the query and get results
                results_df = dune.run_query_dataframe(query)
                
                # Append the results to the CSV file
                mode = 'a' if os.path.exists(self.output_dir) else 'w'
                results_df.to_csv(self.output_dir, mode=mode, index=False, header=mode=='w')
            print("Results saved to", self.output_dir)

    # Define the blockchains and their corresponding chain IDs
    blockchains = {
        "ethereum": "1",
        "arbitrum": "42161",
        "optimism": "10",
        "base": "8453",
        "gnosis": "100",
        "bnb": "56",
        "fantom": "250",
        "avalanche_c": "43113",
        "zksync": "324",
        "celo": "42220",
    }

    # Define the filename for saving the CSV file
    output_dir = os.getenv("OUTPUT_DIR")
    csv_filename = os.path.join(output_dir, "raw_list.csv")

    # Remove the existing CSV file if it exists
    if os.path.exists(csv_filename):
        os.remove(csv_filename)
        print("Existing CSV file removed.")

    # Initialize DuneClient with the API key loaded from environment variables
    dune_api_key = os.getenv("DUNE_API_KEY")
    dune = DuneClient(api_key=dune_api_key)

    # Instantiate Dune class
    dune_instance = Dune(blockchains, csv_filename, query_id=3713189, creds_path=os.getenv('GOOGLE_AUTH_JSON_PATH'))

    # Run the query
    dune_instance.run_query()

# Function to run filter_google.py
def run_filter_google():

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

# Main function to execute all scripts
def main():
    run_subgraph()
    run_dune()
    run_filter_google()

if __name__ == "__main__":
    main()

