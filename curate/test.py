import os
import csv
from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
api_key = os.getenv('API_KEY')

# Define the GraphQL endpoint
endpoint = 'https://gateway-arbitrum.network.thegraph.com/api/[api-key]/subgraphs/id/2mi5zidQdekNdS6ojDj4tnKZe9MiKcseWQGAguTwcvBV'

# Define the registry addresses
registry_addresses = {
    'atr': os.getenv('XDAI_REGISTRY_ADDRESS_TAGS'),
    'token': os.getenv('XDAI_REGISTRY_TOKENS'),
    'cdn': os.getenv('XDAI_REGISTRY_DOMAINS')
}

# Define a function to fetch data for a given registry address
def fetch_data_for_registry(registry_address):
    items = []
    offset = 0
    has_more_items = True

    while has_more_items:
        # Construct the GraphQL query with pagination
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

        # Send the POST request to the GraphQL endpoint
        response = requests.post(endpoint, json={'query': query}, headers={'Authorization': f'Bearer {api_key}'})

        # Check if the request was successful
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
            # Print the error message if the request failed
            print(f"Error: {response.status_code} - {response.text}")
            has_more_items = False

    return items

def save_to_csv(registry_name, data):
    filename = f"{registry_name}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['chain_id', 'address', 'key1', 'key2', 'key3'])
        writer.writeheader()
        for item in data:
            chain_id = item['key0']  # Use key0 as chain_id
            address = item['key1']  # Extract address from key1
            item['chain_id'] = chain_id
            item['address'] = address
            del item['key0']  # Remove the original key0 column
            del item['key1']  # Remove the original key1 column
            writer.writerow(item)

# Loop through each registry address
for registry_name, registry_address in registry_addresses.items():
    print(f"Fetching data for {registry_name}...")
    all_registry_data = []

    # Fetch data for the current registry address
    registry_data = fetch_data_for_registry(registry_address)
    all_registry_data.extend(registry_data)

    # Save all data to CSV for this registry
    save_to_csv(registry_name, all_registry_data)
    print(f"Data saved to {registry_name}.csv")

