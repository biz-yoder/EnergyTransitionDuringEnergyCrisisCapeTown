# Merges contract location data with building data from overture maps

from shapely import wkt
import geopandas as gpd
import pandas as pd
import os


# Paths
merged_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/2b_out/out_contract_with_location.parquet"
buildings_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/capetown_buildings2.parquet"
output_path = "/home/ey53/vscode-server-backup/CapeTown_Workflow/3_out/out_contractlocation_with_building.parquet"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

#######################################
# Load contract location+transactions data and filter WKT

merged_df = pd.read_parquet(merged_path)

# Total unique contracts before filtering
total_unique_contracts_all = merged_df['contract_ID'].nunique()
print(f"🔢 Total unique contracts (ALL, before WKT filter): {total_unique_contracts_all:,}")

# Unique contracts by Type
unique_contracts_by_type = (
    merged_df.groupby("Type")["contract_ID"].nunique().reset_index(name="unique_contracts")
)
print("Unique contracts by Type:")
print(unique_contracts_by_type.to_string(index=False))

# Keep only rows with valid WKT
merged_df = merged_df.loc[
    merged_df["wkt"].notna() &
    (merged_df["wkt"] != "") &
    (merged_df["wkt"] != "<NA>") &
    (merged_df["wkt"].str.len() > 10)
]
print(f"[Step 1] Rows with WKT: {len(merged_df):,}")

# Count unique contracts with WKT by Type
contracts_with_wkt_by_type = (
    merged_df.groupby("Type")["contract_ID"].nunique().reset_index(name="contracts_with_wkt")
)
print("Unique contracts with WKT by Type:")
print(contracts_with_wkt_by_type.to_string(index=False))

#######################################
# Convert WKT to geometry

merged_df["wkt"] = merged_df["wkt"].astype(str)
merged_gdf = gpd.GeoDataFrame(
    merged_df,
    geometry=gpd.GeoSeries.from_wkt(merged_df["wkt"]),
    crs="EPSG:4326"
)
print(f"✅ Geometry conversion successful. Rows remaining: {len(merged_gdf):,}")

#######################################
# Load building data

buildings_gdf = gpd.read_parquet(buildings_path)
print(f"[Step 2] Loaded buildings_gdf with {len(buildings_gdf):,} rows")

# Ensure CRS match
if buildings_gdf.crs != merged_gdf.crs:
    buildings_gdf = buildings_gdf.to_crs(merged_gdf.crs)
    print(f"[Step 3] Reprojected buildings_gdf to {merged_gdf.crs}")

#######################################
#  Spatial join: put contract in building

# Filter buildings to bounding box
minx, miny, maxx, maxy = merged_gdf.total_bounds
buildings_subset = buildings_gdf.cx[minx:maxx, miny:maxy][['id', 'geometry']]

joined_gdf = gpd.sjoin(
    merged_gdf,
    buildings_subset,
    how="left",
    predicate="within",
    rsuffix="_building" #does this allow multiple household rows (bc diff tariffs) to match to a building
)
print(f"[Step 4] Spatial join complete — rows: {len(joined_gdf):,}")

# Contracts without a building assigned
contracts_unassigned = joined_gdf[joined_gdf['id'].isna()].copy()
print(f"Contracts without a building assigned: {len(contracts_unassigned):,}")

#######################################
# Assign nearest building within 100m for unassigned contracts

# Reproject for distance calculation
contracts_unassigned = contracts_unassigned.to_crs(32734)
buildings_subset = buildings_subset.to_crs(32734)

# Find nearest building
nearest = gpd.sjoin_nearest(
    contracts_unassigned,
    buildings_subset,
    how='left',
    distance_col='dist_m',
    max_distance=100
)

# Only assign where a nearest building exists
assigned_nearest = nearest[nearest["id_right"].notna()]
contracts_unassigned.loc[assigned_nearest.index, "id"] = assigned_nearest["id_right"]

# Free memory
del nearest, assigned_nearest, buildings_subset

#######################################
# Step 4b: Update main dataframe and drop geometry

joined_df = joined_gdf.drop(columns="geometry").copy()
joined_df.update(contracts_unassigned)

# Free memory
del joined_gdf, contracts_unassigned, merged_gdf, buildings_gdf

#######################################
# Find remaining unmatched UNIQUE contracts

unmatched_unique_contracts = joined_df.loc[joined_df["id"].isna(), "contract_ID"].nunique()
print(f"Remaining unmatched UNIQUE contracts: {unmatched_unique_contracts:,}")

unmatched_by_type = (
    joined_df.loc[joined_df["id"].isna()]
    .groupby("Type")["contract_ID"]
    .nunique()
    .reset_index(name="unmatched_unique_contracts")
)
print("Remaining unmatched UNIQUE contracts by Type:")
print(unmatched_by_type.to_string(index=False))

#######################################
# Keep only rows with assigned building

joined_df = joined_df[joined_df["id"].notna()]

# Unique contracts matched by Type
type_counts_merged = (
    joined_df.groupby('Type')['contract_ID']
    .nunique()
    .reset_index(name='unique_contracts_merged')
)
print("Unique contracts (by Type) that merged with buildings:")
print(type_counts_merged.to_string(index=False))

#######################################
# Percent matched

matched_unique_contracts = joined_df['contract_ID'].nunique()
percent_matched = 100 * matched_unique_contracts / total_unique_contracts_all
print(f"\n📊 Percent of ALL unique contracts matched with a building: {percent_matched:.2f}%")
print(f"   🔹 Total unique contracts (ALL): {total_unique_contracts_all:,}")
print(f"   🔹 Unique contracts matched: {matched_unique_contracts:,}")

#######################################
# Save

joined_df.to_parquet(output_path, engine="pyarrow", index=False)
print(f"[Step 5] ✅ Saved merged data with building assignments to {output_path}")
