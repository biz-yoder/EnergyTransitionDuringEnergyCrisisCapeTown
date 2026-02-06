"""
Import and combine contract location data

Author: Elizabeth Yoder
Date: February 2026
"""

import os
import pandas as pd
from datetime import datetime

# Paths
location_folder = "data/ContractLocations"
output_path = "output/2a_out/new_location_total.parquet"

#######################################
# Import location files
print("\n=== Importing Location Files (Filtered for TSES) ===")

locations_dfs = []

for filename in os.listdir(location_folder):
    if filename.endswith(".csv"):
        file_path = os.path.join(location_folder, filename)
        print(f"Importing: {filename}")

        try:
            use_columns = [
                "contract_account_hashed",
                "contract_hashed",
                "move_in_timestamp",
                "move_out_timestamp",
                "active",
                "wkt",
                "absd_area",
                "ward2021",
                "official_suburb",
                "electricity_region",
                'device_serial_number_hashed',
                'business_area'
            ]

            #######################################
            # Find column names
            available_cols = pd.read_csv(file_path, nrows=0).columns
            valid_cols = [c for c in use_columns if c in available_cols]
            dtypes= {
                "move_in_timestamp": "string",
                "move_out_timestamp": "string",
                "active": "category",
                "wkt": "string",
                "absd_area": "category",
                "ward2021": "category",
                "official_suburb": "category",
                "electricity_region": "category",
                "contract_hashed": "string",
                "contract_account_hashed": "string",
                "device_serial_number_hashed": "string",
                "business_area": "string"
                }

            chunks = pd.read_csv(
                file_path,
                usecols=valid_cols,
                chunksize=200000,
                dtype={k: v for k, v in dtypes.items() if k in valid_cols},
                low_memory=True
            )


            filtered_chunks = []
            for chunk in chunks:
                if "business_area" not in chunk.columns:
                    continue

                filtered = chunk[chunk["business_area"] == "TSES"]

                if filtered.empty:
                    continue

                filtered = filtered.copy()

                for col in use_columns:
                    if col not in filtered.columns:
                        filtered[col] = pd.NA

                filtered_chunks.append(filtered[use_columns])


            if filtered_chunks:
                df = pd.concat(filtered_chunks, ignore_index=True)
                locations_dfs.append(df)

        except Exception as e:
            print(f"Skipping {filename} due to error: {e}")

#######################################
# Keep only non-empty DataFrames with at least one non-NA column
locations_dfs = [df for df in locations_dfs if not df.empty and df.dropna(how='all', axis=1).shape[1] > 0]

#######################################
# Combine
location_combined = pd.concat(locations_dfs, ignore_index=True) if locations_dfs else pd.DataFrame()

#######################################
# Save
location_combined.to_parquet(output_path, index=False)
print(f"✅ Final dataset saved to: {output_path}")