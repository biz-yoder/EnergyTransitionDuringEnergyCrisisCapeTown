# Match contracts with SHS based on building matches

import os
import glob
import duckdb
import pandas as pd

# Paths
BUILD_SHS_DIR = "/home/ey53/vscode-server-backup/CapeTown_Workflow/4_out"
CONTRACT_BUILD_FILE = "/home/ey53/vscode-server-backup/CapeTown_Workflow/3_out/out_contractlocation_with_building.parquet"
OUTPUT_DIR = "/home/ey53/vscode-server-backup/CapeTown_Workflow/5a_out"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set up
YEARS = [2020, 2021, 2022, 2023]

#######################################
# Load contract data

print(f"📂 Loading contract_build: {CONTRACT_BUILD_FILE}")
contract_build = pd.read_parquet(CONTRACT_BUILD_FILE)

# Make sure 'building_id' column exists
if 'index__building' in contract_build.columns:
    contract_build = contract_build.rename(columns={'index__building': 'building_id'})
elif 'building_id' not in contract_build.columns:
    raise ValueError("Contract_build does not have 'index__building' or 'building_id' column!")

# Filter out contracts with missing building_id (cannot match SHS)
contract_build = contract_build[contract_build['building_id'].notna()]

# Extract year and month
contract_build["year"] = contract_build["month_year"].dt.year
contract_build["month"] = contract_build["month_year"].dt.month

# Diagnostics
print(f"Total rows in contract_build: {len(contract_build):,}")
print(f"Unique contracts: {contract_build['contract_account_hashed'].nunique():,}")
print(f"Unique buildings: {contract_build['building_id'].nunique():,}")

# Save temporary parquet for DuckDB
TEMP_CONTRACT_PARQUET = "/tmp/contract_build_temp.parquet"
contract_build.to_parquet(TEMP_CONTRACT_PARQUET, index=False)

#######################################
# Find SHS data

all_shs_files = glob.glob(os.path.join(BUILD_SHS_DIR, "*.parquet"))
all_shs_files.sort()
print(f"Found {len(all_shs_files)} SHS parquet files total")

#######################################
# Process per year

for year in YEARS:
    print(f"\n📌 Processing year {year}")
    output_file = os.path.join(OUTPUT_DIR, f"merged_contract_SHS_{year}.parquet")
    if os.path.exists(output_file):
        os.remove(output_file)

    con = duckdb.connect(database=":memory:")

    # Load contract_build for this year
    con.execute(f"""
        CREATE TABLE contract_build AS
        SELECT *
        FROM read_parquet('{TEMP_CONTRACT_PARQUET}')
        WHERE year = {year}
    """)

    # Contract diagnostics per year
    result_contract = con.execute("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT building_id) AS unique_buildings,
               COUNT(DISTINCT contract_account_hashed) AS unique_contracts
        FROM contract_build
    """).fetchdf()
    print(f"Contract rows for {year}: {result_contract['total_rows'][0]:,}")
    print(f"Unique buildings: {result_contract['unique_buildings'][0]:,}")
    print(f"Unique contracts: {result_contract['unique_contracts'][0]:,}")

    # Filter SHS files for this year
    year_shs_files = [f for f in all_shs_files if f"{year}_" in os.path.basename(f)]
    if not year_shs_files:
        print(f"⚠️ No SHS files found for year {year}. Saving contract_build as-is.")
        con.execute(f"COPY contract_build TO '{output_file}' (FORMAT PARQUET)")
        con.close()
        continue

    print(f"SHS files for {year}: {len(year_shs_files)}")
    year_pattern = os.path.join(BUILD_SHS_DIR, f"{year}_*.parquet")

    # Load SHS and deduplicate per building (largest area)
    con.execute(f"""
        CREATE TABLE shs_raw AS
        SELECT 
            id AS shs_id,
            image_id AS shs_image_id,
            prediction_id AS shs_prediction_id,
            label AS shs_label,
            area_m2 AS shs_area_m2,
            polygon_centroid_GPS_lat_lon_ AS shs_gps,
            index_right AS building_id
        FROM read_parquet('{year_pattern}')
        WHERE index_right IS NOT NULL
    """)

    con.execute("""
        CREATE TABLE shs AS
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY shs_area_m2 DESC) AS rn
            FROM shs_raw
        ) WHERE rn = 1
    """)

    # Diagnostics
    result_shs = con.execute("""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT building_id) AS unique_buildings
        FROM shs
    """).fetchdf()
    print(f"SHS rows: {result_shs['total_rows'][0]:,}")
    print(f"Unique buildings in SHS: {result_shs['unique_buildings'][0]:,}")

    # LEFT JOIN contract_build and SHS on building_id
    con.execute("""
        CREATE TABLE merged AS
        SELECT 
            c.*,
            s.shs_id,
            s.shs_image_id,
            s.shs_prediction_id,
            s.shs_label,
            s.shs_area_m2,
            s.shs_gps,
            CASE WHEN s.shs_id IS NOT NULL THEN 1 ELSE 0 END AS matched
        FROM contract_build c
        LEFT JOIN shs s
        ON c.building_id = s.building_id
    """)

    # Check merged table columns
    cols = con.execute("PRAGMA table_info(merged)").fetchdf()
    print(cols[['name', 'type']])

    # Count unique contracts matched/unmatched
    result = con.execute("""
        SELECT 
            COUNT(DISTINCT contract_account_hashed) AS total_unique_contracts,
            COUNT(DISTINCT CASE WHEN matched = 1 THEN contract_account_hashed END) AS matched_unique_contracts,
            COUNT(DISTINCT CASE WHEN matched = 0 THEN contract_account_hashed END) AS unmatched_unique_contracts
        FROM merged
    """).fetchdf()

    total_unique_contracts = int(result['total_unique_contracts'][0])
    matched_unique_contracts = int(result['matched_unique_contracts'][0])
    unmatched_unique_contracts = int(result['unmatched_unique_contracts'][0])
    match_pct = 100 * matched_unique_contracts / total_unique_contracts if total_unique_contracts else 0

    # Save
    con.execute(f"COPY merged TO '{output_file}' (FORMAT PARQUET)")

    print(f"\n📊 Year {year} summary (unique contracts):")
    print(f"   🔹 Total unique contracts: {total_unique_contracts:,}")
    print(f"   🔹 Matched unique contracts: {matched_unique_contracts:,}")
    print(f"   🔹 Unmatched unique contracts: {unmatched_unique_contracts:,}")
    print(f"   🔹 Percent matched: {match_pct:.2f}%")
    print("----------------------------------------------------\n")

    con.close()

print("✅ All years processed successfully.")
