# graphquery.py

import requests

class GraphQuery:
    def __init__(self, endpoint, api_key):
        self.endpoint = endpoint
        self.api_key = api_key

    def fetch_data(self, query_file_path):
        query = self._load_query_from_file(query_file_path)

        response = requests.post(self.endpoint, json={'query': query}, headers={'Authorization': f'Bearer {self.api_key}'})

        if response.status_code == 200:
            data = response.json().get('data', {})
            items = data.get('litems', [])
            return items
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return []

    def _load_query_from_file(self, query_file_path):
        with open(query_file_path, 'r') as query_file:
            query = query_file.read()
        return query
