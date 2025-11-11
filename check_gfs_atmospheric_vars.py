#!/usr/bin/env python3
"""
Check what wind variables are available in GFS Atmospheric model
"""
import xarray as xr
from gfs_atmospheric_handler_v2 import get_gfs_atmospheric_dataset_url, load_gfs_atmospheric_dataset
from config import logger

print("=" * 100)
print("CHECKING GFS ATMOSPHERIC MODEL VARIABLES")
print("=" * 100)

# Get the latest GFS Atmospheric dataset URL
logger.info("Finding GFS Atmospheric dataset...")
url = get_gfs_atmospheric_dataset_url()

if not url:
    print("ERROR: Could not find GFS Atmospheric dataset URL")
    exit(1)

print(f"\nDataset URL: {url}")

# Load the dataset
ds = load_gfs_atmospheric_dataset(url)

if not ds:
    print("ERROR: Could not load GFS Atmospheric dataset")
    exit(1)

print("\n" + "=" * 100)
print("ALL VARIABLES IN DATASET:")
print("=" * 100)

all_vars = sorted(ds.data_vars)
print(f"\nTotal variables: {len(all_vars)}\n")

# Show all variables
for var in all_vars:
    attrs = ds[var].attrs
    long_name = attrs.get('long_name', 'No description')
    units = attrs.get('units', 'N/A')
    print(f"{var:<20} - {long_name:<60} [{units}]")

print("\n" + "=" * 100)
print("WIND-RELATED VARIABLES:")
print("=" * 100)

wind_vars = [v for v in all_vars if any(keyword in v.lower() for keyword in ['wind', 'gust', 'ugrd', 'vgrd'])]

if not wind_vars:
    print("\nNo obvious wind variables found. Let me check for u/v components:")
    wind_vars = [v for v in all_vars if 'ugrd' in v.lower() or 'vgrd' in v.lower()]

if wind_vars:
    print(f"\nFound {len(wind_vars)} wind-related variables:\n")
    for var in wind_vars:
        attrs = ds[var].attrs
        long_name = attrs.get('long_name', 'No description')
        units = attrs.get('units', 'N/A')
        print(f"{var:<20} - {long_name:<60} [{units}]")

        # Sample some values
        try:
            sample = ds[var].isel(time=0, lat=0, lon=0).values
            print(f"                     Sample value: {sample}")
        except Exception as e:
            print(f"                     Could not sample: {e}")
        print()
else:
    print("\nNo wind variables found!")
    print("\nLet me check if there are any variables with '10m' in the name:")
    vars_10m = [v for v in all_vars if '10m' in v.lower()]
    if vars_10m:
        for var in vars_10m:
            attrs = ds[var].attrs
            print(f"{var:<20} - {attrs.get('long_name', 'No description')}")

ds.close()

print("\n" + "=" * 100)
print("RECOMMENDATIONS:")
print("=" * 100)
print("If u/v wind components (ugrd10m, vgrd10m) are available:")
print("  - We can calculate wind speed: sqrt(u^2 + v^2)")
print("  - This ensures wind_speed_mph and wind_gust_mph come from the same GFS model")
print("\nIf not available:")
print("  - We may need to use NOAA GFSwave's windsfc variable for consistency")
print("=" * 100)
