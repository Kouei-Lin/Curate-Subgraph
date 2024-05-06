import os
import csv
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
api_key = os.getenv('API_KEY')

# Define the GraphQL endpoint
endpoint = f'https://gateway-arbitrum.network.thegraph.com/api/{api_key}/subgraphs/id/ECENsJRfqi6Mwj6kF9diShPzFKkgyyo79aSCkSwAShHL'

# Define the GraphQL query with the parent ID included
query = """
query {
  courts {
    parent {
      id
    }
    subcourtID
    tokenStaked
  }
}
"""

def get_court_data():
    try:
        # Execute the GraphQL query
        response = requests.post(endpoint, json={'query': query})
        data = response.json()
        return data.get('data', {}).get('courts', [])
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return []

def write_to_csv(data):
    csv_file_path = '../data/mainnet.csv'
    fieldnames = ['parentID', 'subcourtID', 'tokenStaked']

    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write data rows
        for court in data:
            parent_id = court['parent']['id'] if court['parent'] else None
            subcourt_id = court['subcourtID']
            token_staked = float(court['tokenStaked']) / 1e18
            writer.writerow({'parentID': parent_id, 'subcourtID': subcourt_id, 'tokenStaked': token_staked})

    print("Data has been successfully written to", csv_file_path)

# Main function to orchestrate the process
def main():
    court_data = get_court_data()
    if court_data:
        write_to_csv(court_data)

if __name__ == "__main__":
    main()

