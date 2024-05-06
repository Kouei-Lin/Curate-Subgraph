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

# Define the registry addresses and chain IDs
registry_addresses = {
    'atr': os.getenv('XDAI_REGISTRY_ADDRESS_TAGS'),
    'token': os.getenv('XDAI_REGISTRY_TOKENS'),
    'cdn': os.getenv('XDAI_REGISTRY_DOMAINS')
}

chain_ids = [10, 56, 100, 250, 324, 8453, 43114, 42161, 42220]

# Define a function to fetch data for a given registry address and chain ID
def fetch_data_for_registry(registry_address, chain_id):
    # Construct the GraphQL query
    query = f'''
    query {{
      litems(
        orderBy: latestRequestSubmissionTime
        first: 1000
        where: {{
          registryAddress: "{registry_address}",
          status: Registered,
          key0_contains: "eip155:{chain_id}:"
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
        items = data.get('litems', [])
        return items
    else:
        # Print the error message if the request failed
        print(f"Error: {response.status_code} - {response.text}")
        return []

def save_to_csv(registry_name, data):
    filename = f"{registry_name}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['chain_id', 'address', 'key1', 'key2', 'key3'])
        writer.writeheader()
        for item in data:
            chain_id, address = item['key0'].split(':')[-2:]  # Extract chain_id and address from key0
            item['chain_id'] = chain_id
            item['address'] = address
            del item['key0']  # Remove the original key0 column
            writer.writerow(item)

# Loop through each registry address
for registry_name, registry_address in registry_addresses.items():
    print(f"Fetching data for {registry_name}...")
    all_registry_data = []
    
    # Loop through each chain ID
    for chain_id in chain_ids:
        print(f"Fetching data for chain ID {chain_id}...")
        chain_data = fetch_data_for_registry(registry_address, chain_id)
        all_registry_data.extend(chain_data)
    
    # Save all data to CSV for this registry
    save_to_csv(registry_name, all_registry_data)
    print(f"Data saved to {registry_name}.csv")

