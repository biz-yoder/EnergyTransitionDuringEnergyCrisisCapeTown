# Merge dataset (contracts with transactions, location, and SHS data) with load shedding blocks

import os 
import time
import pandas as pd
import geopandas as gpd
from shapely import wkt

# ------------------------------
# CONFIG
# ------------------------------
COMBINED_FILE = "/home/ey53/vscode-server-backup/CapeTown_Workflow/5c_out/with_sseg_reg.parquet"
BLOCKS_FILE = "/home/ey53/vscode-server-backup/CapeTown_Workflow/Load_shedding_Blocks.geojson"
OUTPUT_FILE = "/home/ey53/vscode-server-backup/CapeTown_Workflow/6_out/merged_with_blocks_combined.parquet"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
CHUNK_SIZE = 100_000

# ------------------------------
# Step 1 — Load load shedding blocks
# ------------------------------
print(f"📂 Loading load shedding blocks: {BLOCKS_FILE}")
blocks_gdf = gpd.read_file(BLOCKS_FILE)
blocks_gdf = blocks_gdf.to_crs("EPSG:4326")
print(f"✅ Loaded {len(blocks_gdf):,} load shedding blocks.")

# ------------------------------
# Step 2 — Load combined parquet
# ------------------------------
print(f"\n📂 Loading combined parquet: {COMBINED_FILE}")
df = pd.read_parquet(COMBINED_FILE)
total_rows = len(df)
print(f"✅ Loaded {total_rows:,} rows, {len(df.columns)} columns.")
print(df.columns)

processed_chunks = []
start_time = time.time()

# ------------------------------
# Step 3 — Chunked spatial join
# ------------------------------
for start in range(0, total_rows, CHUNK_SIZE):
    print(f"\n🟦 Processing chunk {start // CHUNK_SIZE + 1} — rows {start:,} to {min(start + CHUNK_SIZE, total_rows):,}")

    chunk_df = df.iloc[start:start + CHUNK_SIZE].copy()
    if chunk_df.empty:
        continue

    if "wkt" not in chunk_df.columns:
        print("⚠ 'wkt' column missing — skipping chunk")
        continue

    # Convert WKT to geometry
    chunk_df["geometry"] = chunk_df["wkt"].apply(wkt.loads)

    # Avoid geometry name conflicts
    if "geometry_block" in chunk_df.columns:
        chunk_df = chunk_df.drop(columns=["geometry_block"])

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
    print(f"   ✅ Chunk processed in {elapsed:.1f}s")

# ------------------------------
# Step 4 — Save result
# ------------------------------
if processed_chunks:
    result_gdf = pd.concat(processed_chunks, ignore_index=True)
    result_gdf.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n💾 Saved merged file to: {OUTPUT_FILE}")
    print(f"✅ Total merged rows: {len(result_gdf):,}")
else:
    print("⚠ No chunks processed — no output saved.")

