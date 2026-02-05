#Match contracts with locations data

import pandas as pd
from datetime import datetime
import os
import polars as pl

# Paths
locations_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/2a_out/new_location_total.parquet"
monthly_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/1_out/final_monthly_new.parquet"
devices_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/devices_total.parquet"
old_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/1_out/final_monthly_old_efficient.parquet"
output_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/2b_out/out_contract_with_location.parquet"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

#######################################
# Load data

locations_df = pd.read_parquet(locations_path)
devices_df = pd.read_parquet(devices_path)
monthly_df = pd.read_parquet(monthly_path)
old_df = pd.read_parquet(old_path)

#######################################
# Baseline reference counts
baseline_rows = len(monthly_df) + len(old_df)
baseline_contracts = pd.concat([
    monthly_df["contract_account_hashed"], 
    old_df["contract_account_hashed"].combine_first(old_df["contract_hashed"])
]).nunique()

print(f"Baseline counts:")
print(f"  Total rows: {baseline_rows:,}")
print(f"  Unique contracts: {baseline_contracts:,}")
print("="*80)

#######################################
# Prepare columns

monthly_df['Type'] = 'prepaid'
monthly_df['contract_ID'] = monthly_df['contract_account_hashed']

old_df = old_df.rename(columns={"totalunits": "kwh"})

print("\nAfter column prep:")
print(f"  monthly_df unique contract_IDs: {monthly_df['contract_ID'].nunique():,} "
      f"({monthly_df['contract_ID'].nunique()/baseline_contracts*100:.1f}% of baseline)")
print(f"  old_df unique contract_IDs: {old_df['contract_hashed'].nunique():,} "
      f"({old_df['contract_hashed'].nunique()/baseline_contracts*100:.1f}% of baseline)")
print("="*80)

#######################################
# Combine data

required_monthly_cols = ['contract_ID', 'Type', 'month_year', 'trfname', 'kwh', 'contract_account_hashed']
required_old_cols = ['Type', 'month_year', 'kwh', 'contract_account_hashed', 'contract_hashed', 'rate_category']

before_concat = len(monthly_df) + len(old_df)
df_combined = pd.concat([
    monthly_df[required_monthly_cols],
    old_df[required_old_cols]
], ignore_index=True)

print(f"\nAfter concatenation: {len(df_combined):,} rows "
      f"({len(df_combined)/baseline_rows*100:.1f}% of baseline rows), "
      f"unique contracts: {df_combined['contract_ID'].nunique():,} "
      f"({df_combined['contract_ID'].nunique()/baseline_contracts*100:.1f}% of baseline contracts)")
print("="*80)

#######################################
# Fix dates

df_combined["month_year"] = pd.to_datetime(df_combined["month_year"], format="%Y-%m", errors="coerce")
valid_dates = df_combined["month_year"].notna().sum()
invalid_dates = df_combined["month_year"].isna().sum()
print(f"Date conversion: {valid_dates:,} valid, {invalid_dates:,} invalid "
      f"({valid_dates/baseline_rows*100:.1f}% of baseline rows)")
print("="*80)

#######################################
# Clean locations data

before_loc = len(locations_df)

locations_df["move_in_timestamp"] = pd.to_datetime(locations_df["move_in_timestamp"], errors="coerce").fillna(pd.Timestamp("1900-01-01"))
locations_df["move_out_timestamp"] = pd.to_datetime(locations_df["move_out_timestamp"], errors="coerce")
locations_df.loc[locations_df["move_out_timestamp"].isna(), "move_out_timestamp"] = pd.Timestamp(datetime.today().date())
locations_df.loc[locations_df["move_out_timestamp"].dt.year == 9999, "move_out_timestamp"] = pd.Timestamp(datetime.today().date())

mask_bad = locations_df["move_out_timestamp"] < locations_df["move_in_timestamp"]
if mask_bad.any():
    print(f"⚠️ Fixing {mask_bad.sum():,} rows where move_out < move_in")
    locations_df.loc[mask_bad, "move_out_timestamp"] = pd.Timestamp(datetime.today().date())

print(f"Cleaned locations_df: {len(locations_df):,} rows (was {before_loc:,}), "
      f"unique contracts: {locations_df['contract_account_hashed'].nunique():,} "
      f"({locations_df['contract_account_hashed'].nunique()/baseline_contracts*100:.1f}% of baseline)")
print("="*80)

#######################################
# Merge contract data with locations

df_combined_pl = pl.from_pandas(df_combined)
locations_pl = pl.from_pandas(locations_df)

#######################################
# Prepare col names for merge

locations_pl_account = (
    locations_pl.with_columns([
        pl.col("contract_account_hashed").alias("contract_account_hashed_loc")
    ])
)

locations_pl_contract = (
    locations_pl.with_columns([
        pl.col("contract_account_hashed").alias("contract_account_hashed_loc"),
        pl.col("contract_hashed").alias("contract_hashed_loc")
    ])
)

#######################################
# Merge: New data (with contract_account_hashed)

df1 = (
    df_combined_pl.join(
        locations_pl_account,
        left_on="contract_account_hashed",
        right_on="contract_account_hashed_loc",
        how="inner"
    )
    .filter(
        (pl.col("month_year") >= pl.col("move_in_timestamp")) &
        (pl.col("month_year") <= pl.col("move_out_timestamp"))
    )
    .with_columns(pl.lit("account_match").alias("match_type"))
)
print(f"After account join: {df1.height:,} rows")

#######################################
# Merge: Postpaid data (with contract_hashed)

df2 = (
    df_combined_pl.join(
        locations_pl_contract,
        left_on="contract_hashed",
        right_on="contract_hashed_loc",
        how="inner"
    )
    .filter(
        (pl.col("month_year") >= pl.col("move_in_timestamp")) &
        (pl.col("month_year") <= pl.col("move_out_timestamp"))
    )
    .with_columns(pl.lit("contract_match").alias("match_type"))
)
print(f"After contract join: {df2.height:,} rows")

#######################################
# Combine merges

df_merged = pl.concat([df1, df2], how="diagonal_relaxed")

#######################################
# Combine the two 'contract_account_hashed' sources into one

df_merged = df_merged.with_columns(
    pl.coalesce([
        pl.col("contract_account_hashed"),          # original
        pl.col("contract_account_hashed_loc"),      # from merge 1
    ]).alias("contract_account_hashed")
)

cols_to_drop = [c for c in ["contract_account_hashed_loc", "contract_hashed_loc", "contract_account_hashed_device"] if c in df_merged.columns]
df_merged = df_merged.drop(cols_to_drop)

#######################################
# Convert to pandas

df_merged = df_merged.to_pandas()

df_merged["contract_ID"] = df_merged["contract_account_hashed"].combine_first(df_merged["contract_hashed"])

#######################################
# Fill missing location data 

loc_cols = ["ward2021", "move_in_timestamp", "move_out_timestamp", "wkt"]
loc_cols = [c for c in loc_cols if c in df_merged.columns]
df_merged = df_merged.sort_values(["contract_ID", "month_year"])
df_merged[loc_cols] = df_merged.groupby("contract_ID")[loc_cols].ffill()
df_merged[loc_cols] = df_merged.groupby("contract_ID")[loc_cols].bfill()

print(f"Missing location values after fill: {df_merged[loc_cols].isna().sum().to_dict()} "
      f"({df_merged['wkt'].notna().mean()*100:.1f}% rows have location)")

#######################################
# Fix duplicates

print("\nResolving duplicates...")
df_merged = df_merged.sort_values("wkt", na_position="last")
dup_before = df_merged.duplicated(subset=["contract_ID", "month_year"], keep=False).sum()
print(f"Duplicate month/account combos before drop: {dup_before:,}")
#contracts pay on more than one tariff each month

# Drop duplicates
df_merged = df_merged.drop_duplicates()
dup_after = df_merged.duplicated(subset=["contract_ID", "month_year"]).sum()
print(f"Duplicate month/account combos after drop: {dup_after:,}")
print(f"Remaining rows: {len(df_merged):,} ({len(df_merged)/baseline_rows*100:.1f}% of baseline)")
print(f"Remaining unique contracts: {df_merged['contract_ID'].nunique():,} "
      f"({df_merged['contract_ID'].nunique()/baseline_contracts*100:.1f}% of baseline)")
 
#######################################
# Summarize location coverage

contracts_with_wkt = df_merged.loc[df_merged["wkt"].notna(), ["contract_ID", "Type"]].drop_duplicates()
contracts_total = df_merged[["contract_ID", "Type"]].drop_duplicates()

summary = (
    contracts_total.groupby("Type")
    .agg(total_contracts=("contract_ID", "count"))
    .reset_index()
    .merge(
        contracts_with_wkt.groupby("Type").agg(with_location=("contract_ID", "count")).reset_index(),
        on="Type",
        how="left"
    )
    .fillna(0)
)
summary["percent_with_location"] = (summary["with_location"] / summary["total_contracts"]) * 100
summary["percent_of_baseline_contracts"] = (summary["total_contracts"] / baseline_contracts) * 100
print("\nUnique contracts with location info by Type:")
print(summary.to_string(index=False))
print("="*80)

#######################################
# Save

df_merged.to_parquet(output_path, index=False)
print(f"\n✅ Saved final dataset to: {output_path}")
print(f"Final rows: {len(df_merged):,} ({len(df_merged)/baseline_rows*100:.1f}% of baseline)")
print(f"Final unique contracts: {df_merged['contract_ID'].nunique():,} "
      f"({df_merged['contract_ID'].nunique()/baseline_contracts*100:.1f}% of baseline)")
print("="*80)
