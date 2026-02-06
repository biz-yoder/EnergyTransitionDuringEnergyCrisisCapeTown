"""
Merge SHS registration data with existing energy dataset,
forward-fill key columns, compute PV capacities, and output cleaned Parquet.

Author: Elizabeth Yoder
Date: 02/2026
"""

import pandas as pd
import numpy as np

# Paths (update to your local files)
PARQUET_PATH = "data/combined.parquet"
CSV_PATH = "data/checked_01132026.csv"
OUTPUT_FILE = "output/with_sseg_reg.parquet"

# Load data
df_parquet = pd.read_parquet(PARQUET_PATH)
df_csv = pd.read_csv(CSV_PATH)

# Merge on 'contract_account_hashed'
df_merged = pd.merge(
    df_parquet,
    df_csv,
    on=['contract_account_hashed','year'],
    how='left', 
    suffixes=('_parquet', '_csv')
)

# Columns to keep
cols_to_keep = ['contract_ID', 'contract_account_hashed', 'Type', 'month_year', 'trfname',
                 'rate_category', 'kwh', 'contract_hashed', 'wkt_parquet', 
                 'contract_account_hashed_right', 'building_id','year', 'month', 'shs_label',
                 'shs_label_edit', 'shs_area_m2', 'shs_area_m2_edit', 'has_shs', 'shs_gps', 
                 'matched', 'installation_type', 'fake', 'total_capacity_va', 'start_year', 
                 'wkt_csv', 'geometry', 'Did not build', 'Built; NOT found by M2F', 
                 'Built; found by M2F', 'Notes', 'area_m2']

df_subset = df_merged[cols_to_keep].copy()

# Drop columns with identical data (even if names are different)
def drop_identical_columns(df):
    cols = df.columns.tolist()
    to_drop = set()
    for i in range(len(cols)):
        if cols[i] in to_drop:
            continue
        for j in range(i+1, len(cols)):
            if cols[j] in to_drop:
                continue
            if df[cols[i]].equals(df[cols[j]]):
                # Keep the first, drop the second
                to_drop.add(cols[j])
    return df.drop(columns=list(to_drop))

df_clean = drop_identical_columns(df_subset)
df_clean = df_clean.rename(columns=lambda x: x.replace(" ", "_").replace(";", ""))

# Rename 'wkt_csv' to 'wkt'
if 'wkt_csv' in df_clean.columns:
    df_clean = df_clean.rename(columns={'wkt_csv': 'wkt'})


# Columns to forward-fill after first occurrence
ff_cols = ['installation_type', 'total_capacity_va',
           'Built_NOT_found_by_M2F', 'Built_found_by_M2F']

ff_cols = [c for c in ff_cols if c in df_clean.columns]

# Ensure datetime for sorting
df_clean['month_year'] = pd.to_datetime(df_clean['month_year'], errors='coerce')

# Sort by household and time
df_clean = df_clean.sort_values(['contract_ID', 'month_year'])

# Forward-fill only after first valid value per household
for col in ff_cols:
    df_clean[col] = df_clean.groupby('contract_ID')[col].transform(lambda x: x.ffill())
    
# Get rid of shs predictions where visual evidence shows shs was not build
df_clean["shs_label_edit"] = df_clean["shs_label_edit"].where(
    df_clean["Did not build"] != 1,
    pd.NA
)

# Fill in registration shs from visual inspection
df_clean.loc[
    df_clean["shs_label_edit"].isna() &
    (
        (df_clean["Built; NOT found by M2F"] == 1) |
        (df_clean["Built; found by M2F"] == 1)
    ),
    "shs_label_edit"
] = "PV_normal"

#Define PV capacity metrics
panel_size = 1.7      # m² per panel
watt_per_panel = 400 # watts

#Calculate capacity from predicted SHS area or, if that isn't available, registered capacity
df_clean["Watt"] = (
    df_clean["shs_area_m2_edit"]
      .replace(0, np.nan)
      * watt_per_panel / panel_size
).fillna(df_clean["total_capacity_va"]).copy()


# Count unique contracts per year
unique_contracts_per_year = df_clean.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)
print("\n Unique contracts by year:")
print(unique_contracts_per_year)

# Filter for households with SHS
pv_df = df_clean[df_clean['shs_label_edit'] == "PV_normal"]

# Count unique contracts per year among PV households
unique_contracts_per_year = pv_df.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)

print("\n Unique PV_normal contracts by year:")
print(unique_contracts_per_year)

months_per_contract = df_clean.groupby('contract_ID')['month_year'].nunique()
print(months_per_contract.describe())

# Save
df_clean.to_parquet(OUTPUT_FILE, index=False)

print(f"Merged and cleaned data saved to: {OUTPUT_FILE}")
