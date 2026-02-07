"""
Add load shedding monthly duration data to each transaction .

Author: Elizabeth Yoder
Date: February 2026
"""

import os
import pandas as pd
import geopandas as gpd
import glob
import time
import re

#Paths
LOADSHED_FILE = "data/raw/Loadshedding_schedule.csv"
BLOCKS_DIR = "output/6_out"
OUTPUT_DIR = "output/7_out"

os.makedirs(OUTPUT_DIR, exist_ok=True)

#######################################
# Load load shedding schedule

print(f"Loading Load Shedding Schedule: {LOADSHED_FILE}")
shed_df = pd.read_csv(LOADSHED_FILE)

print(f"Loaded {len(shed_df):,} rows.")
print("Parsing dates...")
shed_df["Date"] = pd.to_datetime(shed_df["Date"], errors="coerce")

# Strip column whitespace
shed_df.columns = shed_df.columns.str.strip()

# Create month_year column
shed_df["month_year"] = shed_df["Date"].dt.to_period("M").astype(str)

def extract_area_number(row):
    """
    Extract the number following 'Area' from:
        Area column first
        Stage column if not found in Area
    """
    area_pattern = re.compile(r"area\s*([0-9]+)", flags=re.IGNORECASE)

    area_val = str(row.get("Area", ""))
    stage_val = str(row.get("Stage", ""))

    # Check Area column first
    if re.search(r"area", area_val, flags=re.IGNORECASE):
        match = area_pattern.search(area_val)
        if match:
            return int(match.group(1))

    # Fallback to Stage column
    if re.search(r"area", stage_val, flags=re.IGNORECASE):
        match = area_pattern.search(stage_val)
        if match:
            return int(match.group(1))

    return None

# Apply to the dataframe
shed_df["Area_number"] = shed_df.apply(extract_area_number, axis=1)
shed_df = shed_df[shed_df["Area_number"].notna()].copy()

# Summary
print("\n=== Extracted Area numbers ===")
print(shed_df[["Stage", "Area", "Area_number"]].head(20))
print(shed_df["Area_number"].value_counts(dropna=False).sort_index())
print(f"Extracted valid Area numbers for {shed_df['Area_number'].notna().sum():,} of {len(shed_df):,} rows")
print(shed_df.columns)

#######################################
# Summarize by Area and month_year

print("Summarizing load shedding by Area and month_year...")
shed_summary = shed_df.groupby(["Area_number", "month_year"], as_index=False)["Duration min"].sum()
shed_summary.rename(columns={"Duration min": "total_duration_min"}, inplace=True)
print("Summary ready:")
print(shed_summary.head())

#######################################
# Merge with yearly data

parquet_files = glob.glob(os.path.join(BLOCKS_DIR, "*.parquet"))
merged_list = []
output_path = os.path.join(OUTPUT_DIR, "combined_merged.parquet")

for file_path in parquet_files:
    print(f"\n Processing {file_path}...")
    try:
        gdf = gpd.read_parquet(file_path)
        print(f"   - Loaded {len(gdf):,} rows.")

        # Rename BlockID to Area
        if "BlockID" in gdf.columns:
            gdf = gdf.rename(columns={"BlockID": "Area_number"})
        gdf["Area_number"] = gdf["Area_number"].astype(str).str.upper().str.strip()

        # Ensure month_year is datetime first
        gdf["month_year"] = pd.to_datetime(gdf["month_year"], errors="coerce")
        gdf["month_year"] = gdf["month_year"].dt.strftime("%Y-%m")

        if "Area_number" not in gdf.columns:
            print(f"No Area column in {file_path}, skipping.")
            continue


        gdf["Area_number"] = pd.to_numeric(gdf["Area_number"], errors="coerce").astype("Float64")

        print(gdf[["Area_number", "month_year"]])
        print(shed_summary[["Area_number", "month_year"]])

        merged = pd.merge(gdf, shed_summary, how="left",
                        left_on=["Area_number", "month_year"],
                        right_on=["Area_number", "month_year"])

        print(f"Saved merged data: {output_path}")

        merged_list.append(merged)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

print("\n All files processed.")

# Save
if merged_list:
    combined_merged = pd.concat(merged_list, ignore_index=True)
    combined_merged.to_parquet(output_path, index=False)
    print(f"\n All files merged and saved to {output_path}")
else:
    print("No files were successfully merged.")
