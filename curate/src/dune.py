import os
import requests
import pandas as pd
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv

class Dune:
    def __init__(self, blockchains, output_dir, query_id, creds_path):
        self.blockchains = blockchains
        self.output_dir = output_dir
        self.query_id = query_id
        self.creds_path = creds_path

    def run_query(self):
        load_dotenv()  # Load environment variables
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

