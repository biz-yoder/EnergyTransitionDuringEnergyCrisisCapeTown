# Merges yearly SHS panel data (location + capacity) with buildings

import pandas as pd
import geopandas as gpd
import os
import time

# Paths
CSV_PATHS = {
    2020: "/shared/data/climateplus2025/Postprocessing_EntireDataset_CapeTown_Image_2018_2023/2020/output_stage_4/prediction_merged_2023.csv",
    2021: "/shared/data/climateplus2025/Postprocessing_EntireDataset_CapeTown_Image_2018_2023_Mask2Former_1024_Nov29/2021/output_post_processing_polygonization_grouping_drop_small_objects/prediction_merged_2021_final.csv",
    2022: "/shared/data/climateplus2025/Postprocessing_EntireDataset_CapeTown_Image_2018_2023_Mask2Former_1024_Nov29/2022/output_post_processing_polygonization_grouping_drop_small_objects/prediction_merged_2022_final.csv",
    2023: "/shared/data/climateplus2025/Postprocessing_EntireDataset_CapeTown_Image_2018_2023_Mask2Former_1024_Nov29/2023/output_post_processing_polygonization_grouping_drop_small_objects/prediction_merged_2023_final.csv"
}

BUILDINGS_PATH = "/home/ey53/vscode-server-backup/CapeTown_Workflow/capetown_buildings2.parquet"
OUTPUT_DIR = "/home/ey53/vscode-server-backup/CapeTown_Workflow/4_out"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set up
CHUNK_SIZE = 50_000
LOG_FILE = os.path.join(OUTPUT_DIR, "merge_log.txt")

#######################################
# Logging helper

def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{ts}] {msg}"
    print(full_msg)
    with open(LOG_FILE, "a") as f:
        f.write(full_msg + "\n")

#######################################
# Clean col names

def clean_columns(df):
    """Ensure column names are safe for GeoPackage output."""
    old_cols = df.columns.tolist()
    new_cols = [
        c.replace("[", "_").replace("]", "_").replace(",", "_").replace("/", "_")[:30]
        for c in old_cols
    ]
    df.columns = new_cols
    return df

#######################################
# Load buildings

log("Loading building polygons...")
start = time.time()
buildings_gdf = gpd.read_parquet(BUILDINGS_PATH)
if buildings_gdf.crs != "EPSG:4326":
    buildings_gdf = buildings_gdf.to_crs("EPSG:4326")
_ = buildings_gdf.sindex  # build spatial index
log(f"Loaded {len(buildings_gdf):,} polygons in {time.time() - start:.1f}s")

#######################################
# MAIN

for year, csv_path in CSV_PATHS.items():
    log(f"\n--- Starting year {year} ---")

    checkpoint_file = os.path.join(OUTPUT_DIR, f"{year}_done_chunks.txt")
    done_chunks = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as f:
            done_chunks = set(int(line.strip()) for line in f if line.strip().isdigit())
        log(f"Resuming — {len(done_chunks)} chunks already completed.")

    # Year-level counters
    total_start_pv = total_dropped_area = total_dropped_nomatch = total_matched = 0

    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False), start=1):
        if i in done_chunks:
            log(f"Skipping chunk {i} (already done).")
            continue

        log(f"Processing chunk {i} ({len(chunk):,} rows)...")

        # Filter only PV_normal
        chunk = chunk[chunk["label"] == "PV_normal"]
        start_pv = len(chunk)
        total_start_pv += start_pv
        log(f"Initial PV_normal rows: {start_pv:,}")

        # Drop small area_m2 < 1.7
        if "area_m2" in chunk.columns:
            before_area = len(chunk)
            chunk = chunk[chunk["area_m2"] >= 1.7]
            dropped_area = before_area - len(chunk)
            total_dropped_area += dropped_area
            log(f"Dropped {dropped_area:,} rows (area_m2 < 1.7). Remaining: {len(chunk):,}")
        else:
            log("⚠️ No 'area_m2' column found — skipping area filter.")

        if chunk.empty:
            log(f"Chunk {i}: No PV_normal rows left after filtering, skipping.")
            continue

        expected_col = "polygon_centroid_GPS[lat,lon]"

        if expected_col not in chunk.columns:
            # Look for a similar column
            possible_cols = [c for c in chunk.columns if c.startswith("polygon_centroid_GPS_lat_lon")]
            if possible_cols:
                # Rename the first match
                chunk.rename(columns={possible_cols[0]: expected_col}, inplace=True)
                log(f"INFO: Renamed {possible_cols[0]} to {expected_col} for {year}, chunk {i}")
            else:
                log(f"ERROR: Missing GPS column for {year}, chunk {i}")
                continue
        
        # Robust GPS parsing
        gps_col = "polygon_centroid_GPS[lat,lon]"

        # Remove brackets/parentheses, strip spaces
        coord_clean = chunk[gps_col].str.replace(r"[\(\)\[\]]", "", regex=True).str.strip()

        # Split on comma into two columns
        gps = coord_clean.str.split(",", expand=True)
        chunk.loc[:, "lat"] = pd.to_numeric(gps[0].str.strip(), errors="coerce")
        chunk.loc[:, "lon"] = pd.to_numeric(gps[1].str.strip(), errors="coerce")

        # Drop rows with invalid coordinates
        chunk = chunk.dropna(subset=["lat", "lon"])
        if chunk.empty:
            log(f"Chunk {i}: no valid coordinates, skipping.")
            continue

        # Convert to GeoDataFrame
        gdf = gpd.GeoDataFrame(
            chunk,
            geometry=gpd.points_from_xy(chunk["lon"], chunk["lat"]),
            crs="EPSG:4326"
        )


        # Subset buildings for speed
        minx, miny, maxx, maxy = gdf.total_bounds
        b_subset = buildings_gdf.cx[minx:maxx, miny:maxy]
        log(f"Spatial join subset: {len(b_subset):,} buildings")

        # Spatial join
        start_join = time.time()
        merged = gpd.sjoin(gdf, b_subset, how="left", predicate="within")
        join_time = time.time() - start_join
        log(f"Join done in {join_time:.1f}s — {len(merged):,} rows")

        matched_rows = merged["index_right"].notna().sum()
        total_rows = len(merged)
        total_matched += matched_rows
        match_rate = 100 * matched_rows / total_rows if total_rows else 0
        unique_buildings = merged["index_right"].dropna().nunique()
        log(f"Matched {matched_rows:,}/{total_rows:,} rows ({match_rate:.2f}%) to {unique_buildings:,} unique buildings")

        # Compute change from baseline
        dropped_no_match = start_pv - matched_rows
        total_dropped_nomatch += dropped_no_match
        log(f"Dropped {dropped_no_match:,} PV_normal rows (no building match, relative to start baseline).")

        # Find unmatched PV
        unmatched = merged[merged["index_right"].isna()].copy()
        log(f"Unmatched PV points after sjoin: {len(unmatched):,}")

        if not unmatched.empty:
            # Drop existing 'index_right' to avoid conflict
            if 'index_right' in unmatched.columns:
                unmatched = unmatched.drop(columns=['index_right'])

            b_subset_free = b_subset[~b_subset.index.isin(merged["index_right"].dropna())].copy()
            if not b_subset_free.empty:
                unmatched = unmatched.to_crs("EPSG:32734")
                b_subset_proj = b_subset_free.to_crs("EPSG:32734")
                b_subset_proj = b_subset_proj.reset_index().rename(columns={'index': 'building_index'})

                nearest = gpd.sjoin_nearest(
                    unmatched,
                    b_subset_proj[['building_index', 'geometry']],
                    how='left',
                    distance_col='dist_m'
                )

                max_distance = 100
                nearest_within = nearest[nearest['dist_m'] <= max_distance].copy()
                merged.loc[nearest_within.index, 'index_right'] = nearest_within['building_index']

                # Compute distance summary
                dist_summary = nearest['dist_m'].describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99])
                log(f"Distance summary to nearest building (meters):\n{dist_summary}")
                log(f"Assigning only points within {max_distance:.1f} meters")

                # Keep only points within 99th percentile
                nearest_within = nearest[nearest['dist_m'] <= max_distance].copy()
                log(f"Number of points assigned via nearest building: {len(nearest_within):,}")

                # Update 'merged'
                merged.loc[nearest_within.index, 'index_right'] = nearest_within['building_index']

                # Continue processing
                merged["year"] = year
                merged = clean_columns(merged)


        # Save
        chunk_parquet = os.path.join(OUTPUT_DIR, f"{year}_chunk{i}.parquet")
        merged.to_parquet(chunk_parquet)
        log(f"✅ Saved chunk {i} → {chunk_parquet}")

        with open(checkpoint_file, "a") as f:
            f.write(f"{i}\n")

        del gdf, merged, b_subset

    # Yearly summary
    total_remaining = total_matched
    log(f"\n=== YEAR {year} SUMMARY ===")
    log(f"Total starting PV_normal rows: {total_start_pv:,}")
    log(f"Total dropped (area < 1.7): {total_dropped_area:,}")
    log(f"Total dropped (no building match): {total_dropped_nomatch:,}")
    log(f"Total remaining matched: {total_remaining:,}")
    # Avoid division by zero
    retention_rate = (100 * total_remaining / total_start_pv) if total_start_pv != 0 else None

    if retention_rate is None:
        log("Retention rate: N/A (total_start_pv is zero)")
    else:
        log(f"Retention rate: {retention_rate:.2f}%")
    log("=============================\n")

log("✅ All years processed successfully.")
