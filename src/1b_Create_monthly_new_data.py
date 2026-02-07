"""
Transforms all new transaction data (prepaid from April 2021 on) into monthly panel

Author: Elizabeth Yoder
Date: February 2026
"""

import polars as pl
import os
from time import time
import pandas as pd

# Paths
parquet_path = "data/prepaid_parquet"  # folder with raw Parquet files
output_dir = "output/1_out"            # save output here
os.makedirs(output_dir, exist_ok=True)
final_file = os.path.join(output_dir, "final_monthly_new.parquet")

# Set up
use_columns = ["totalunits", "trfname", "transaction_timestamp", "contract_account_hashed"]

#######################################
# MAIN

if __name__ == "__main__":
    start = time()
    print("[INFO] Processing entire dataset...")

    #######################################
    # Load dataset
    
    df = (
        pl.scan_parquet(os.path.join(parquet_path, "*.parquet"))
        .select(use_columns)
        .with_columns(
            pl.col("transaction_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
        )
        .unique(subset=["contract_account_hashed", "transaction_timestamp", "totalunits"], keep="first")
    ).collect()

    print(f"[INFO] Loaded {df.height:,} rows after deduplication")

    #######################################
    # Clean
    
    df = df.sort(["contract_account_hashed", "transaction_timestamp"]).with_columns(
        pl.col("totalunits").cast(pl.Float32)
    )

    #######################################
    # Find NEXT timestamp
    
    df = df.with_columns([
        pl.col("transaction_timestamp").shift(-1).over("contract_account_hashed").alias("next_timestamp")
    ])

    # Remove last transaction (since no next_timestamp)
    df = df.filter(pl.col("next_timestamp").is_not_null())

    #######################################
    # Compute days_between and daily rate

    df = df.with_columns([
        (pl.col("next_timestamp") - pl.col("transaction_timestamp")).dt.total_days().alias("days_between")
    ])

    df = df.with_columns([
        pl.when(pl.col("days_between").is_null() | (pl.col("days_between") <= 0))
          .then(1.0)
          .otherwise(pl.col("days_between"))
          .alias("days_between_safe")
    ])

    df = df.with_columns([
        (pl.col("totalunits").fill_null(0.0) / pl.col("days_between_safe")).alias("daily_rate_safe")
    ])

    #######################################
    # Month expansion
    
    df = df.with_columns([
        (pl.col("transaction_timestamp").dt.year() * 12 + pl.col("transaction_timestamp").dt.month() - 1).alias("start_month"),
        (pl.col("next_timestamp").dt.year() * 12 + pl.col("next_timestamp").dt.month() - 1).alias("end_month")
    ])

    df = df.with_columns([
        pl.int_ranges(pl.col("start_month"), pl.col("end_month") + 1).alias("months_array")
    ]).explode("months_array")

    df = df.with_columns([
        (pl.col("months_array") // 12).cast(pl.Int32).alias("year"),
        (pl.col("months_array") % 12 + 1).cast(pl.Int32).alias("month"),
        pl.datetime(
            (pl.col("months_array") // 12).cast(pl.Int32),
            (pl.col("months_array") % 12 + 1).cast(pl.Int32),
            1
        ).alias("month_start")
    ])

    #######################################
    # Compute start and end of period

    df = df.with_columns([
        pl.max_horizontal(["transaction_timestamp", "month_start"]).alias("period_start"),
        pl.min_horizontal([
            "next_timestamp",
            (pl.col("month_start") + pl.duration(days=pl.col("month_start").dt.days_in_month()))
        ]).alias("period_end")
    ])

    #######################################
    # Compute days_in_period and kWh

    df = df.with_columns([
        (pl.col("period_end") - pl.col("period_start")).dt.total_days().alias("days_in_period")
    ])

    df = df.with_columns([
        pl.when(pl.col("days_in_period") <= 0)
          .then(1.0)
          .otherwise(pl.col("days_in_period"))
          .alias("days_in_period_safe")
    ])

    df = df.with_columns([
        (pl.col("daily_rate_safe") * pl.col("days_in_period_safe")).alias("kwh")
    ])

    #######################################
    # Monthly aggregation

    monthly = (
        df.group_by(["contract_account_hashed", "trfname", "month_start"])
          .agg([
              pl.col("kwh").sum(),
              pl.len().alias("num_transactions")
          ])
          .with_columns(pl.col("month_start").dt.strftime("%Y-%m").alias("month_year"))
          .select(["contract_account_hashed", "trfname", "month_year", "kwh", "num_transactions"])
    )

    #######################################
    # Save parquet
    
    monthly.write_parquet(final_file)
    print(f"Final merged file saved: {final_file}")

    #######################################
    # Final checks
    
    raw_total = pl.scan_parquet(os.path.join(parquet_path, "*.parquet")).select("totalunits").collect()["totalunits"].sum()
    processed_kwh = monthly["kwh"].sum()
    diff = raw_total - processed_kwh
    diff_pct = diff / raw_total * 100

    print("\n Sanity Check:")
    print(f"Raw totalunits: {raw_total:,.2f}")
    print(f"Processed kWh:  {processed_kwh:,.2f}")
    print(f"Difference: {diff:,.2f} ({diff_pct:.5f}%)")

    if abs(diff_pct) > 0.05:
        print("[WARNING] Significant discrepancy detected!")
    else:
        print("Sanity check passed!")

    print(f"Total runtime: {time() - start:.1f}s")
