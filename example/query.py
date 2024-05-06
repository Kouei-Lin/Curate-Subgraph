import os
import requests
from dotenv import load_dotenv

class SubgraphQuery:
    def __init__(self, api_key, subgraph_url):
        self.api_key = api_key
        self.subgraph_url = subgraph_url

    def execute_query(self, query):
        endpoint = f"{self.subgraph_url}?api-key={self.api_key}"
        try:
            response = requests.post(endpoint, json={'query': query})
            data = response.json()
            return data.get('data', {})
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return {}

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
api_key = os.getenv('API_KEY')

# Define the subgraph URL
subgraph_url = 'https://gateway-arbitrum.network.thegraph.com/api/subgraphs/id/xxxxxxx'

# Define the GraphQL query
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

# Create an instance of SubgraphQuery
subgraph_query = SubgraphQuery(api_key, subgraph_url)

# Execute the GraphQL query
result = subgraph_query.execute_query(query)

print(result)

