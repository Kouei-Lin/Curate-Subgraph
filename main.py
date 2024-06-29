# main.py

import os
import csv
from dotenv import load_dotenv
from module.graphquery import GraphQuery

# Load environment variables from .env file
load_dotenv()

# Define environment variables
api_key = os.getenv('GRAPH_API_KEY')
subgraph_endpoint = os.getenv('SUBGRAPH_ENDPOINT')
query_file_path = os.getenv('QUERY_FILE_PATH')
output_csv_file = os.getenv('OUTPUT_FILE_PATH')

# Instantiate GraphQuery class
graph_query = GraphQuery(subgraph_endpoint, api_key)

# Fetch data using the specified query file
items = graph_query.fetch_data(query_file_path)

# Determine fieldnames dynamically from the first item
if items:
    fieldnames = list(items[0].keys())
else:
    fieldnames = []

# Write fetched items to CSV
with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for item in items:
        writer.writerow(item)

print(f"Successfully saved fetched items to {output_csv_file}.")

