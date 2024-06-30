# main.py

import os
import csv
from dotenv import load_dotenv
from module.graphquery import GraphQuery

def load_environment_variables():
    """Load environment variables from .env file."""
    load_dotenv()

def fetch_data_from_graph(api_key, subgraph_endpoint, query_file_path):
    """Fetch data using GraphQuery class."""
    # Instantiate GraphQuery class
    graph_query = GraphQuery(subgraph_endpoint, api_key)
    
    # Fetch data using the specified query file
    return graph_query.fetch_data(query_file_path)

def determine_fieldnames(items):
    """Determine fieldnames dynamically from the first item."""
    if items:
        return list(items[0].keys())
    else:
        return []

def write_items_to_csv(items, output_csv_file, fieldnames):
    """Write fetched items to CSV."""
    with open(output_csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for item in items:
            writer.writerow(item)

    print(f"Successfully saved fetched items to {output_csv_file}.")

def main():
    """Main function to orchestrate fetching and saving data."""
    # Load environment variables from .env file
    load_environment_variables()

    # Define environment variables
    api_key = os.getenv('GRAPH_API_KEY')
    subgraph_endpoint = os.getenv('SUBGRAPH_ENDPOINT')
    query_file_path = os.getenv('QUERY_FILE_PATH')
    output_csv_file = os.getenv('OUTPUT_FILE_PATH')

    # Fetch data using GraphQuery
    items = fetch_data_from_graph(api_key, subgraph_endpoint, query_file_path)

    # Determine fieldnames dynamically from the first item
    fieldnames = determine_fieldnames(items)

    # Write fetched items to CSV
    write_items_to_csv(items, output_csv_file, fieldnames)

if __name__ == "__main__":
    main()

