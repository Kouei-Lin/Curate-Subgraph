import os
import requests
import pandas as pd
from dotenv import load_dotenv

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

