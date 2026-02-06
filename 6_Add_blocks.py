"""
Merges dataset (contracts with transactions, location, and SHS data) with load shedding blocks

Author: Elizabeth Yoder
Date: February 2026
""" 

import os 
import time
import pandas as pd
import geopandas as gpd
from shapely import wkt

# Paths
COMBINED_FILE = "output/5c_out/with_sseg_reg.parquet"
BLOCKS_FILE = "data/Load_shedding_Blocks.geojson"
OUTPUT_FILE = "output/6_out/merged_with_blocks_combined.parquet"

#Set up
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
CHUNK_SIZE = 100_000

#######################################
# Load load shedding blocks

print(f"Loading load shedding blocks: {BLOCKS_FILE}")
blocks_gdf = gpd.read_file(BLOCKS_FILE).to_crs("EPSG:4326")
print(f"Loaded {len(blocks_gdf):,} load shedding blocks.")

#######################################
# Load combined parquet

print(f"\n Loading combined parquet: {COMBINED_FILE}")
df = pd.read_parquet(COMBINED_FILE)
total_rows = len(df)
print(f"Loaded {total_rows:,} rows, {len(df.columns)} columns.")

processed_chunks = []
start_time = time.time()

#######################################-
# Spatial join

for start in range(0, total_rows, CHUNK_SIZE):
    print(f"\n Processing chunk {start // CHUNK_SIZE + 1} — rows {start:,} to {min(start + CHUNK_SIZE, total_rows):,}")

    chunk_df = df.iloc[start:start + CHUNK_SIZE].copy()
    if chunk_df.empty:
        print("Chunk is empty — skipping")
        continue

    if "wkt" not in chunk_df.columns:
        print("'wkt' column missing — skipping chunk")
        continue

    # Convert WKT to geometry
    chunk_df["geometry"] = chunk_df["wkt"].apply(wkt.loads)

    # Avoid geometry name conflicts
    if "geometry_block" in chunk_df.columns:
        chunk_df = chunk_df.drop(columns=["geometry_block"])

    # Create GeoDataFrame
    chunk_gdf = gpd.GeoDataFrame(chunk_df, geometry="geometry", crs="EPSG:4326")

    # Spatial join
    merged_chunk = gpd.sjoin(
        chunk_gdf,
        blocks_gdf,
        how="left",
        predicate="intersects",
        rsuffix="_block"
    )

    processed_chunks.append(merged_chunk)

    elapsed = time.time() - start_time
    print(f"Chunk processed in {elapsed:.1f}s")

#######################################
# Save 

if processed_chunks:
    result_gdf = pd.concat(processed_chunks, ignore_index=True)
    result_gdf.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n Saved merged file to: {OUTPUT_FILE}")
    print(f"Total merged rows: {len(result_gdf):,}")
else:
    print("No chunks processed — no output saved.")

