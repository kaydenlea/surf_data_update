#!/usr/bin/env python3
"""
Debug NOAA wind data extraction to verify we're reading the right variables
"""
from noaa_grid_handler import get_noaa_dataset_url, load_noaa_dataset, validate_noaa_dataset
from config import logger
import numpy as np

logger.info("Loading NOAA dataset...")
noaa_url = get_noaa_dataset_url()
ds = load_noaa_dataset(noaa_url)

if not validate_noaa_dataset(ds):
    print("NOAA dataset validation failed")
    exit(1)

print("=" * 100)
print("NOAA GFSWAVE DATASET VARIABLES:")
print("=" * 100)

# List all variables
print("\nAll variables in dataset:")
for var in sorted(ds.data_vars):
    print(f"  {var}: {ds[var].attrs.get('long_name', 'No description')}")

# Check wind-related variables specifically
print("\n" + "=" * 100)
print("WIND-RELATED VARIABLES:")
print("=" * 100)

wind_vars = [v for v in ds.data_vars if 'wind' in v.lower() or 'gust' in v.lower() or 'ugrd' in v.lower() or 'vgrd' in v.lower()]

if not wind_vars:
    print("No obvious wind variables found. Checking u/v grid components:")
    wind_vars = [v for v in ds.data_vars if v in ['ugrdsfc', 'vgrdsfc', 'gustsfc']]

for var in wind_vars:
    attrs = ds[var].attrs
    print(f"\n{var}:")
    print(f"  Long name: {attrs.get('long_name', 'N/A')}")
    print(f"  Units: {attrs.get('units', 'N/A')}")
    print(f"  Description: {attrs.get('description', 'N/A')}")

    # Sample some values
    sample_val = ds[var].isel(time=0, latitude=0, longitude=0).values
    print(f"  Sample value: {sample_val}")

# Extract wind data for first grid point and first few timesteps
print("\n" + "=" * 100)
print("SAMPLE WIND DATA (first grid point, first 5 timesteps):")
print("=" * 100)

try:
    # Get first grid point
    lat_idx = 0
    lon_idx = 0

    wind_u = ds['ugrdsfc'].isel(latitude=lat_idx, longitude=lon_idx).values[:5]
    wind_v = ds['vgrdsfc'].isel(latitude=lat_idx, longitude=lon_idx).values[:5]

    if 'gustsfc' in ds.data_vars:
        wind_gust = ds['gustsfc'].isel(latitude=lat_idx, longitude=lon_idx).values[:5]
    else:
        wind_gust = [np.nan] * 5

    print(f"\n{'Time':<5} {'U (m/s)':<12} {'V (m/s)':<12} {'Calculated Speed (m/s)':<25} {'Gust (m/s)':<15} {'Gust >= Speed?':<15}")
    print("-" * 100)

    for i in range(5):
        u = wind_u[i]
        v = wind_v[i]

        # Calculate wind speed from U and V components
        if not (np.isnan(u) or np.isnan(v)):
            speed_ms = np.sqrt(u**2 + v**2)
            speed_mph = speed_ms * 2.23694
        else:
            speed_ms = np.nan
            speed_mph = np.nan

        gust_ms = wind_gust[i]
        gust_mph = gust_ms * 2.23694 if not np.isnan(gust_ms) else np.nan

        # Check if gust >= speed
        if not np.isnan(speed_mph) and not np.isnan(gust_mph):
            gust_ok = "OK" if gust_mph >= speed_mph else "BAD"
        else:
            gust_ok = "N/A"

        print(f"{i:<5} {u:<12.2f} {v:<12.2f} {speed_mph:<25.1f} {gust_mph:<15.1f} {gust_ok:<15}")

except Exception as e:
    print(f"Error extracting sample data: {e}")
    import traceback
    traceback.print_exc()

ds.close()

print("\n" + "=" * 100)
print("CONCLUSION:")
print("=" * 100)
print("Check the table above:")
print("  - If 'Gust >= Speed' shows mostly 'BAD', then NOAA data itself has issues")
print("  - If it shows mostly 'OK', then the issue is in how we process/store the data")
