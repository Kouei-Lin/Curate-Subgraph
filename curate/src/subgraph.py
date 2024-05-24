import requests
import pandas as pd

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

