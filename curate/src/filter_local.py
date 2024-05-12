import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the paths for the CSV files from the environment variables
output_dir = os.getenv("OUTPUT_DIR")
curate_list_path = os.path.join(output_dir, "raw_list.csv")
atr_path = os.path.join(output_dir, "atr.csv")
hit_list_path = os.path.join(output_dir, "hit_list.csv")

# Read the CSV files
curate_list = pd.read_csv(curate_list_path)
atr = pd.read_csv(atr_path)

# Convert address column to lowercase in both DataFrames
curate_list["address"] = curate_list["address"].str.lower()
atr["address"] = atr["address"].str.lower()

# Merge curate_list and atr on address column
merged = pd.merge(curate_list, atr, on="address", how="left", indicator=True)

# Filter rows that are only in curate_list
filtered_curate_list = merged[merged["_merge"] == "left_only"]

# Drop the indicator column and reset the index
filtered_curate_list = filtered_curate_list.drop(columns=["_merge"]).reset_index(drop=True)

# Save the filtered data as hit_list.csv
filtered_curate_list.to_csv(hit_list_path, index=False)

print("Filtered data saved to", hit_list_path)

