import os
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import requests
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from subgraph import Subgraph, extract_chain_and_address
from filter_google import GoogleSheetWriter

# Load environment variables from .env file
load_dotenv()

def run_subgraph():
    api_key = os.getenv('GRAPH_API_KEY')
    subgraph_endpoint = 'https://gateway-arbitrum.network.thegraph.com/api/[api-key]/subgraphs/id/2mi5zidQdekNdS6ojDj4tnKZe9MiKcseWQGAguTwcvBV'

    registry_addresses = {
        'atr': os.getenv('XDAI_REGISTRY_ADDRESS_TAGS'),
        'token': os.getenv('XDAI_REGISTRY_TOKENS'),
        'cdn': os.getenv('XDAI_REGISTRY_DOMAINS')
    }

    output_dir = os.getenv('OUTPUT_DIR')
    os.makedirs(output_dir, exist_ok=True)

    subgraph = Subgraph(subgraph_endpoint, api_key)

    for registry_name, registry_address in registry_addresses.items():
        print(f"Fetching data for {registry_name}...")
        all_registry_data = []

        registry_data = subgraph.fetch_data(registry_address)
        all_registry_data.extend(registry_data)

        df = pd.DataFrame(all_registry_data)
        df[['chain_id', 'address']] = df.apply(extract_chain_and_address, axis=1)

        df = df[['chain_id', 'address', 'key1', 'key2', 'key3']]

        output_file = os.path.join(output_dir, f"{registry_name}.csv")
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

def run_dune():
    class Dune:
        def __init__(self, blockchains, output_dir, query_id, creds_path):
            self.blockchains = blockchains
            self.output_dir = output_dir
            self.query_id = query_id
            self.creds_path = creds_path

        def run_query(self):
            load_dotenv()
            dune_api_key = os.getenv("DUNE_API_KEY")
            dune = DuneClient(api_key=dune_api_key)

            for blockchain, chain_id in self.blockchains.items():
                params = [
                    QueryParameter.text_type(name="blockchain", value=blockchain),
                    QueryParameter.text_type(name="chain_id", value=chain_id),
                ]
                
                query = QueryBase(
                    name=f"Query for {blockchain}",
                    query_id=self.query_id,
                    params=params,
                )
                
                print(f"Running query for {blockchain}...")
                results_df = dune.run_query_dataframe(query)
                
                mode = 'a' if os.path.exists(self.output_dir) else 'w'
                results_df.to_csv(self.output_dir, mode=mode, index=False, header=mode=='w')
            print("Results saved to", self.output_dir)

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

    output_dir = os.getenv("OUTPUT_DIR")
    csv_filename = os.path.join(output_dir, "raw_list.csv")

    if os.path.exists(csv_filename):
        os.remove(csv_filename)
        print("Existing CSV file removed.")

    dune_instance = Dune(blockchains, csv_filename, query_id=3713189, creds_path=os.getenv('GOOGLE_AUTH_JSON_PATH'))
    dune_instance.run_query()

def run_filter_google():
    output_dir = os.getenv("OUTPUT_DIR")
    curate_list_path = os.path.join(output_dir, "raw_list.csv")
    atr_path = os.path.join(output_dir, "atr.csv")

    curate_list = pd.read_csv(curate_list_path)
    atr = pd.read_csv(atr_path)

    curate_list["address"] = curate_list["address"].str.lower()
    atr["address"] = atr["address"].str.lower()

    merged = pd.merge(curate_list, atr, on="address", how="left", indicator=True)
    filtered_curate_list = merged[merged["_merge"] == "left_only"]
    filtered_curate_list = filtered_curate_list.drop(columns=["_merge"]).reset_index(drop=True)

    creds_path = os.getenv('GOOGLE_AUTH_JSON_PATH')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')

    sheet_writer = GoogleSheetWriter(creds_path, spreadsheet_id)
    sheet_writer.write_dataframe_to_sheet(filtered_curate_list, "hit_list")

    print("Filtered data pushed to Google Sheet.")

def main():
    run_subgraph()
    run_dune()
    run_filter_google()

if __name__ == "__main__":
    main()

