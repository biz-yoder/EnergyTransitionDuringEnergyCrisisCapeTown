# Adds SHS registration data

import pandas as pd

# Paths
parquet_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/5b_out/combined.parquet"
csv_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/checked_01132026.csv"
OUTPUT_FILE = "/home/ey53/vscode-server-backup/CapeTown_Workflow/5c_out/with_sseg_reg.parquet"

# Load data
df_parquet = pd.read_parquet(parquet_path)
df_csv = pd.read_csv(csv_path)

# Merge on 'contract_account_hashed'
df_merged = pd.merge(
    df_parquet,
    df_csv,
    on=['contract_account_hashed','year'],
    how='left',       # keeps all Parquet rows
    suffixes=('_parquet', '_csv')
)

# Columns to keep
cols_to_keep = ['contract_ID', 'contract_account_hashed', 'Type', 'month_year', 'trfname', 'rate_category', 'kwh',
                'contract_hashed', 'wkt_parquet', 'contract_account_hashed_right',
                'building_id','year', 'month', 'shs_label',
                'shs_area_m2', 'has_shs', 'shs_gps', 'matched', 'installation_type',
                'fake', 'total_capacity_va', 'start_year', 'wkt_csv', 'geometry', 
                'Did not build', 'Built; NOT found by M2F', 
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

# Rename 'wkt_csv' to 'wkt'
if 'wkt_csv' in df_clean.columns:
    df_clean = df_clean.rename(columns={'wkt_csv': 'wkt'})


# Columns to forward-fill after first occurrence
ff_cols = ['installation_type', 'total_capacity_va',
    'Built; NOT found by M2F', 'Built; found by M2F'
]
ff_cols = [c for c in ff_cols if c in df_clean.columns]

# Ensure datetime for sorting
df_clean['month_year'] = pd.to_datetime(df_clean['month_year'], errors='coerce')

# Sort by household and time
df_clean = df_clean.sort_values(['contract_ID', 'month_year'])

# Forward-fill only after first valid value per household
for col in ff_cols:
    df_clean[col] = df_clean.groupby('contract_ID')[col].transform(lambda x: x.ffill())
    
# Fill 'shs_label' with PV_normal if not set but there is a built record
df_clean['shs_label'] = df_clean.apply(
    lambda row: 'PV_normal' if pd.isna(row['shs_label']) and 
                (pd.notna(row.get('Built; NOT found by M2F')) or pd.notna(row.get('Built; found by M2F')))
                else row['shs_label'],
    axis=1
)

# Count unique contracts per year
unique_contracts_per_year = df_clean.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)
print("\n🔹 Unique contracts by year:")
print(unique_contracts_per_year)

# Filter for households with SHS
pv_df = df_clean[df_clean['shs_label'] == "PV_normal"]

# Count unique contracts per year among PV households
unique_contracts_per_year = pv_df.groupby('year')['contract_ID'].nunique().reset_index()
unique_contracts_per_year.rename(columns={'contract_ID': 'unique_contracts'}, inplace=True)

print("\n🔹 Unique PV_normal contracts by year:")
print(unique_contracts_per_year)

months_per_contract = df_clean.groupby('contract_ID')['month_year'].nunique()
print(months_per_contract.describe())

# Save
df_clean.to_parquet(OUTPUT_FILE, index=False)

print(f"Merged and cleaned data saved to: {OUTPUT_FILE}")
