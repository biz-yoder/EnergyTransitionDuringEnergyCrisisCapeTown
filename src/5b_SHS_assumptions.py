"""
Clean and consolidate SHS predictions based on constructed assumptions.

Author: Elizabeth Yoder
Date: February 2026
"""

import pandas as pd
import glob
import os
import numpy as np

# Paths
parquet_dir = "output/5a_out"
parquet_out = "output/5b_out"

# Find all parquet files
parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
print(f"Found {len(parquet_files)} parquet files")

# Read and combine
dfs = []
for f in parquet_files:
    print(f"🔹 Loading {os.path.basename(f)} ...")
    df = pd.read_parquet(f)
    print(f"   → {len(df):,} rows, {len(df.columns)} columns")
    dfs.append(df)

if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"\n✅ Combined DataFrame: {len(combined_df):,} total rows, {len(combined_df.columns)} columns")
else:
    raise ValueError("No Parquet files found in the directory.")

# Clean wkt
combined_df = combined_df.loc[
    combined_df["wkt"].notna() &
    (combined_df["wkt"] != "") &
    (combined_df["wkt"] != "<NA>") &
    (combined_df["wkt"].str.len() > 10)
]

# Unique contracts
if "contract_ID" in combined_df.columns:
    unique_contracts = combined_df["contract_ID"].nunique()
    print(f"Total unique contracts: {unique_contracts:,}")
if "Type" in combined_df.columns:
    unique_by_type = combined_df.groupby("Type")["contract_ID"].nunique().reset_index()
    print("\n Unique contracts by Type:")
    for _, row in unique_by_type.iterrows():
        print(f"   {row['Type']}: {row['contract_ID']:,}")

# Ensure datetime and extract year
combined_df['month_year'] = pd.to_datetime(combined_df['month_year'], errors='coerce')
combined_df['year'] = combined_df['month_year'].dt.year

#######################################
# Collapse to contract-year

shs_years = (
    combined_df
    .groupby(['contract_ID', 'year'], as_index=False)
    .agg(
        has_shs=('shs_label', lambda x: (x == "PV_normal").any()),
        shs_area_m2=('shs_area_m2', 'first'),  # make sure column exists
        shs_label=('shs_label', 'first')
    )
)

#######################################
# Create source flag (observed)
# -----------------------------
shs_years['shs_source'] = np.where(
    shs_years['has_shs'],
    'observed',
    pd.NA
)

# Keep a copy of original state
shs_years['has_shs_original'] = shs_years['has_shs']

#######################################
# Define function to implement SHS assumptions

def fix_shs_years(df):
    # years where SHS was observed
    observed_years = set(df.loc[df['shs_source'] == 'observed', 'year'])

    # RULE 1: DELETE single isolated year (except 2023)
    if len(observed_years) == 1:
        only_year = next(iter(observed_years))
        if only_year != 2023:
            df[['has_shs', 'shs_area_m2', 'shs_label', 'shs_source']] = [False, np.nan, np.nan, pd.NA]
            return df
        else:
            return df

    # RULE 2: Fill gaps between observed years
    if observed_years:
        min_y, max_y = min(observed_years), max(observed_years)
        for y in range(min_y, max_y + 1):
            if y not in observed_years:
                df.loc[df['year'] == y, 'has_shs'] = True
                df.loc[df['year'] == y, 'shs_source'] = 'gap_filled'

    # RULE 3: Forward-extend into 2023
    if 2023 not in observed_years and any(y in observed_years for y in [2020, 2021, 2022]):
        df.loc[df['year'] == 2023, 'has_shs'] = True
        df.loc[df['year'] == 2023, 'shs_source'] = 'forward_extended'

    return df

#######################################
# Apply function at contract level

shs_years = (
    shs_years
    .groupby('contract_ID', group_keys=False)
    .apply(fix_shs_years) 
)

#######################################
# Merge back into monthly data

combined_df = combined_df.drop(columns=['shs_area_m2', 'shs_label'], errors='ignore')

combined_df = combined_df.merge(
    shs_years[['contract_ID', 'year', 'has_shs', 'shs_area_m2', 'shs_label', 'shs_source']],
    on=['contract_ID', 'year'],
    how='left'
)

combined_df['shs_label_edit'] = combined_df['shs_label']
combined_df['shs_area_m2_edit'] = combined_df['shs_area_m2']

#######################################
# Remove SHS info where SHS doesn't exist

combined_df.loc[~combined_df['has_shs'], ['shs_label_edit', 'shs_area_m2_edit']] = np.nan

#######################################
# Forward-fill SHS attributes within contract

combined_df = combined_df.sort_values(['contract_ID', 'month_year'])
combined_df[['shs_label_edit', 'shs_area_m2_edit']] = (
    combined_df
    .groupby('contract_ID')[['shs_label_edit', 'shs_area_m2_edit']]
    .ffill()
)

#######################################
# Create binary imputed flag (imputed)

combined_df['shs_imputed'] = combined_df['shs_source'].isin(['gap_filled', 'forward_extended'])

#######################################
# Check

print(
    combined_df.groupby(['year', 'shs_source'])['contract_ID']
    .nunique()
)

# -----------------------------------------------------------------------------------
# Summary:
# - Each contract-year/month has a consistent SHS status.
# - Missing SHS years between observed years are filled (gap_filled).
# - 2023 is populated if SHS existed in prior years (forward_extended).
# - Single-year SHS is only allowed in 2023.
# - shs_source and shs_imputed flags allow distinguishing observed vs gap-filled vs forward-extended data.
# -----------------------------------------------------------------------------------

combined_df['shs_label_edit'] = np.where(
    combined_df['has_shs'] & combined_df['shs_label_edit'].isna(),
    'PV_normal',
    combined_df['shs_label_edit']
)

# Count unique contracts per year
unique_contracts_per_year = combined_df.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)
print("\n Unique contracts by year:")
print(unique_contracts_per_year)

# Filter for households with SHS
pv_df = combined_df[combined_df['shs_label_edit'] == "PV_normal"]

# Count unique contracts per year among PV households
unique_contracts_per_year = pv_df.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)

print("\n Unique PV_normal contracts by year:")
print(unique_contracts_per_year)

months_per_contract = combined_df.groupby('contract_ID')['month_year'].nunique()
print(months_per_contract.describe())

# Save
output_file = os.path.join(parquet_out, "combined.parquet")
combined_df.to_parquet(output_file, index=False)
print(f"\n Saved combined parquet to {output_file}")
