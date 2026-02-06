"""
Transforms all old transaction data (prepaid before April 2021 and all postpaid) into monthly panel

Author: Elizabeth Yoder
Date: February 2026
"""

import polars as pl
import os
from time import time

# Paths
parquet_file = "data/1_out/combined_electricity_data.parquet"
output_dir = "output/1_out"
final_file = os.path.join(output_dir, "final_monthly_old_efficient.parquet")
os.makedirs(output_dir, exist_ok=True)

# Set up
use_columns = ["totalunits", "month_year", "contract_account_hashed", "contract_hashed", "Type", "rate_category"]

t0 = time()
print("Loading data lazily...")

#######################################
# Load data (lazy)
df = pl.scan_parquet(parquet_file).select(use_columns).filter(pl.col("totalunits") != 0)

#######################################
# Process

# Create a single unique ID
df = df.with_columns([
    pl.coalesce(["contract_account_hashed", "contract_hashed"]).alias("account_id")
])

# Parse month/year to timestamp
split_col = pl.col("month_year").str.split_exact("-", 1)
df = df.with_columns([
    split_col.struct.field("field_0").alias("year_str"),
    split_col.struct.field("field_1").str.zfill(2).alias("month_str")
])
df = df.with_columns([
    (pl.col("year_str") + "-" + pl.col("month_str")).str.strptime(pl.Date, "%Y-%m").alias("timestamp_assume")
])
df = df.drop(["year_str", "month_str"])

# Sort accounts
df = df.sort(["account_id", "timestamp_assume"]).with_columns(
    pl.col("totalunits").cast(pl.Float32)
)

# Find NEXT timestamp
df = df.with_columns([
    pl.col("timestamp_assume").shift(-1).over("account_id").alias("next_timestamp")
])

# Get rid of last transaction (doesn't have a NEXT timestamp)
df = df.filter(pl.col("next_timestamp").is_not_null())

# Compute days between
df = df.with_columns([
    (pl.col("next_timestamp") - pl.col("timestamp_assume")).cast(pl.Float32).alias("days_between")
])
df = df.with_columns([
    pl.when(pl.col("days_between") <= 0)
      .then(1.0)
      .otherwise(pl.col("days_between"))
      .alias("days_between_safe")
])
df = df.with_columns([
    (pl.col("totalunits") / pl.col("days_between_safe")).alias("daily_kwh")
])

# Aggregate to monthly totals
df = df.with_columns([
    pl.col("timestamp_assume").dt.year().alias("year"),
    pl.col("timestamp_assume").dt.month().alias("month"),
    pl.datetime(pl.col("timestamp_assume").dt.year(), pl.col("timestamp_assume").dt.month(), 1).alias("month_start")
])

monthly = (
    df.group_by(["account_id", "contract_account_hashed", "contract_hashed", "Type", "month_start", "rate_category"])
      .agg([
          (pl.col("daily_kwh") * pl.col("days_between_safe")).sum().alias("kwh"),  # total kWh for the month
          pl.sum("days_between_safe").alias("num_days")
      ])
      .with_columns([
          pl.col("month_start").dt.strftime("%Y-%m").alias("month_year")
      ])
      .select([
          "account_id",
          "contract_account_hashed",
          "rate_category",
          "contract_hashed",
          "Type",
          "month_year",
          "kwh",
          "num_days"
      ])
)

#######################################
# Save

monthly.collect().write_parquet(final_file)
print(f"Final merged file saved: {final_file}")
print(f"Total runtime: {time() - t0:.1f}s")

#######################################
# Final checks

raw_df = pl.scan_parquet(parquet_file)

raw_by_type = raw_df.group_by("Type").agg(
    pl.sum("totalunits").alias("raw_totalunits")
).collect()

processed_by_type = monthly.group_by("Type").agg(
    pl.sum("kwh").alias("processed_kwh")
).collect()   # <- collect here to turn LazyFrame into DataFrame


sanity_df = raw_by_type.join(processed_by_type, on="Type", how="full").with_columns([
    (pl.col("raw_totalunits") - pl.col("processed_kwh")).alias("diff"),
    ((pl.col("raw_totalunits") - pl.col("processed_kwh")) / pl.col("raw_totalunits") * 100).alias("diff_pct")
])

print("\n Sanity Check by Type:")
print(sanity_df)
