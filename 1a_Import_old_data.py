# Imports all postpaid data and prepaid data from March 2021 and earlier. 

import os
import pandas as pd
from datetime import datetime

# Paths
prepaid_folder = "/home/ey53/vscode-server-backup/CapeTown_Workflow/Old Prepaid"
postpaid_folder = "/home/ey53/vscode-server-backup/CapeTown_Workflow/Postpaid"
output_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/1_out/combined_electricity_data.parquet"

# Date of files for old prepaid transactions
startdate = datetime(2020, 1, 1)
cutoff = datetime(2021, 3, 1)

#######################################
# PREPAID IMPORT
print("=== Importing Prepaid Files ===")

prepaid_use_cols = ['contract_account_hashed', 'units_purchased', 'purchase_period_start', 'meter_serial_number_hashed']
prepaid_rename = {
    'units_purchased': 'totalunits',
    'purchase_period_start': 'transaction_timestamp'
}

prepaid_dfs = []

for filename in os.listdir(prepaid_folder):
    if filename.startswith("prepaid-electricity-purchases-") and filename.endswith(".csv"):
        try:
            # Extract date from filename
            date_str = filename.replace("prepaid-electricity-purchases-", "").replace(".csv", "")
            file_date = datetime.strptime(date_str, "%Y-%m")

            if startdate <= file_date <= cutoff:
                file_path = os.path.join(prepaid_folder, filename)
                print(f"Importing: {filename}")

                df = pd.read_csv(file_path, usecols=prepaid_use_cols)
                df = df.rename(columns=prepaid_rename)
                df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"], errors="coerce")

                # Keep only month and year (YYYY-MM)
                df["month_year"] = df["transaction_timestamp"].dt.strftime("%Y-%m")
                df["Type"] = "prepaid"

                df = df[["contract_account_hashed", "totalunits", "month_year", "transaction_timestamp", "Type"]]
                prepaid_dfs.append(df)
        except Exception as e:
            print(f"Skipping {filename} due to error: {e}")

prepaid_combined = pd.concat(prepaid_dfs, ignore_index=True) if prepaid_dfs else pd.DataFrame()
prepaid_combined = prepaid_combined.drop_duplicates()

#######################################
# POSTPAID IMPORT
print("\n=== Importing Postpaid Files (Filtered for units = W) ===")

postpaid_dfs = []

for filename in os.listdir(postpaid_folder):
    if filename.endswith(".csv"):
        file_path = os.path.join(postpaid_folder, filename)
        print(f"Importing: {filename}")

        try:
            use_columns = [
                "contract_hashed",
                "billing_period_start_month",
                "billing_period_start_year",
                "quantity_billed",
                "rate_category",
                "unit_of_measure_code"
            ]

            chunks = pd.read_csv(
                file_path,
                usecols=lambda c: c in use_columns,
                chunksize=200000,
                dtype={
                    "contract_hashed": "string",
                    "billing_period_start_month": "Int64",
                    "billing_period_start_year": "Int64",
                    "quantity_billed": "float32",
                    "rate_category": "category",
                    "unit_of_measure_code": "string"
                },
                low_memory=True
            )

            filtered_chunks = []
            for chunk in chunks:
                if "rate_category" not in chunk.columns:
                    continue

                filtered = chunk[chunk["unit_of_measure_code"].str.contains("W", case=False, na=False)
                                 ]

                if filtered.empty:
                    continue

                filtered = filtered[[
                    "contract_hashed",
                    "billing_period_start_month",
                    "billing_period_start_year",
                    "quantity_billed",
                    "rate_category"
                ]]

                filtered_chunks.append(filtered)

            if not filtered_chunks:
                continue

            df = pd.concat(filtered_chunks, ignore_index=True)
            df = df.rename(columns={
                "contract_hashed": "contract_hashed",
                "billing_period_start_month": "month",
                "billing_period_start_year": "year",
                "quantity_billed": "totalunits"
            })

            # Create unified month_year column
            df["month_year"] = df["year"].astype(str).str.zfill(2) + "-" + df["month"].astype(str) 
            df["Type"] = "postpaid"

            df = df[["contract_hashed", "totalunits", "month_year", "Type", "rate_category"]]
            postpaid_dfs.append(df)

        except Exception as e:
            print(f"Skipping {filename} due to error: {e}")

postpaid_combined = pd.concat(postpaid_dfs, ignore_index=True) if postpaid_dfs else pd.DataFrame()
postpaid_combined = postpaid_combined.drop_duplicates()

#######################################
# COMBINE PREPAID & POSTPAID
print("\n=== Combining Prepaid and Postpaid Data ===")

if not prepaid_combined.empty or not postpaid_combined.empty:
    combined = pd.concat([prepaid_combined, postpaid_combined], ignore_index=True)
    print(f"\n✅ Combined dataset created with {len(combined):,} total rows.")
    print(f"   Prepaid rows:  {len(prepaid_combined):,}")
    print(f"   Postpaid rows: {len(postpaid_combined):,}")
    print(f"   Date range: {combined['month_year'].min()} → {combined['month_year'].max()}")

    
    # SAVE FINAL COMBINED DATA
    combined.to_parquet(output_path, index=False)
    print(f"\n💾 Saved combined dataset to: {output_path}")
else:
    print("\n⚠️ No data imported from either source.")
