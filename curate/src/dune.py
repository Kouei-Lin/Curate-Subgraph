import os
import pandas as pd
from dotenv import load_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables from .env file
load_dotenv()

# Define the query ID
query_id = 3713189

# Define the blockchains and their corresponding chain IDs
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

# Define the filename for saving the CSV file
output_dir = os.getenv("OUTPUT_DIR")
csv_filename = os.path.join(output_dir, "raw_list.csv")

# Remove the existing CSV file if it exists
if os.path.exists(csv_filename):
    os.remove(csv_filename)
    print("Existing CSV file removed.")

# Initialize DuneClient with the API key loaded from environment variables
dune_api_key = os.getenv("DUNE_API_KEY")
dune = DuneClient(api_key=dune_api_key)

# Loop over each blockchain
for blockchain, chain_id in blockchains.items():
    # Define the query parameters
    params = [
        QueryParameter.text_type(name="blockchain", value=blockchain),
        QueryParameter.text_type(name="chain_id", value=chain_id),
    ]
    
    # Define the query
    query = QueryBase(
        name=f"Query for {blockchain}",
        query_id=query_id,
        params=params,
    )
    
    print(f"Running query for {blockchain}...")
    
    # Run the query and get results
    results_df = dune.run_query_dataframe(query)
    
    # Append the results to the CSV file
    mode = 'a' if os.path.exists(csv_filename) else 'w'
    results_df.to_csv(csv_filename, mode=mode, index=False, header=mode=='w')

print("Results saved to", csv_filename)

