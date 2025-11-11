#!/usr/bin/env python3
"""
Trace wind data for Newport Beach through all data sources
to identify where wind_speed and wind_gust might be getting swapped
"""
from database import supabase
from config import logger
import pandas as pd

# Newport Beach coordinates (approximate)
NEWPORT_LAT = 33.6189
NEWPORT_LON = -117.9298

print("=" * 100)
print("TRACING WIND DATA FOR NEWPORT BEACH")
print("=" * 100)
print(f"Newport Beach coordinates: {NEWPORT_LAT}, {NEWPORT_LON}")

# Step 1: Find the grid point for Newport Beach
print("\nStep 1: Finding nearest grid point...")
response = supabase.table("grid_points").select("*").execute()
grid_points = response.data

min_dist = float('inf')
nearest_grid = None

for gp in grid_points:
    lat_diff = gp['latitude'] - NEWPORT_LAT
    lon_diff = gp['longitude'] - NEWPORT_LON
    dist = (lat_diff**2 + lon_diff**2) ** 0.5

    if dist < min_dist:
        min_dist = dist
        nearest_grid = gp

if nearest_grid:
    print(f"Nearest grid point: ID={nearest_grid['id']}, Lat={nearest_grid['latitude']}, Lon={nearest_grid['longitude']}")
    print(f"Distance: {min_dist:.4f} degrees")
else:
    print("ERROR: No grid points found!")
    exit(1)

grid_id = nearest_grid['id']

# Step 2: Get recent forecast data for this grid point
print("\nStep 2: Fetching recent forecast data for this grid point...")
response = supabase.table("grid_forecast_data").select(
    "*"
).eq("grid_id", grid_id).not_.is_("wind_speed_mph", "null").not_.is_("wind_gust_mph", "null").order("timestamp", desc=True).limit(10).execute()

records = response.data

if not records:
    print("No records found with wind data for this grid point")
    exit(0)

print(f"\nFound {len(records)} recent records with wind data:")
print("=" * 100)
print(f"{'Timestamp (UTC)':<30} {'Wind Speed (mph)':<20} {'Wind Gust (mph)':<20} {'Gust - Speed':<15} {'Status':<10}")
print("-" * 100)

issue_count = 0

for rec in records:
    timestamp = rec.get('timestamp')
    wind_speed = rec.get('wind_speed_mph')
    wind_gust = rec.get('wind_gust_mph')

    if wind_speed is not None and wind_gust is not None:
        diff = wind_gust - wind_speed
        status = "BAD" if wind_gust < wind_speed else "OK"
        if wind_gust < wind_speed:
            issue_count += 1
    else:
        diff = None
        status = "N/A"

    diff_str = f"{diff:.1f}" if diff is not None else "N/A"
    print(f"{timestamp:<30} {wind_speed:<20.1f} {wind_gust:<20.1f} {diff_str:<15} {status:<10}")

print("=" * 100)
print(f"\nSummary for Newport Beach area (Grid {grid_id}):")
print(f"  Records with wind data: {len(records)}")
print(f"  Records where gust < speed: {issue_count}")
print(f"  Percentage with issue: {(issue_count/len(records)*100) if len(records) > 0 else 0:.1f}%")

if issue_count > len(records) * 0.3:
    print("\n*** WARNING: More than 30% of records have gust < speed ***")
    print("*** This suggests a systematic data issue ***")

# Step 3: Check if we can identify the data source
print("\n" + "=" * 100)
print("Step 3: Analyzing data patterns to identify source...")
print("=" * 100)

# Check one specific record in detail
if records:
    sample_rec = records[0]
    print(f"\nSample record (most recent):")
    print(f"  Timestamp: {sample_rec.get('timestamp')}")
    print(f"  Wind Speed: {sample_rec.get('wind_speed_mph')} mph")
    print(f"  Wind Gust: {sample_rec.get('wind_gust_mph')} mph")
    print(f"  Primary Swell Height: {sample_rec.get('primary_swell_height_ft')} ft")
    print(f"  Water Temp: {sample_rec.get('water_temp_f')} °F")
    print(f"  Weather Code: {sample_rec.get('weather')}")

    # Determine likely sources
    sources = []
    if sample_rec.get('primary_swell_height_ft') is not None:
        sources.append("NOAA GFSwave (swell data present)")
    if sample_rec.get('water_temp_f') is not None:
        sources.append("Open Meteo Marine (water temp present)")
    if sample_rec.get('weather') is not None:
        sources.append("Open Meteo Weather (weather code present)")

    print(f"\n  Likely data sources for this record:")
    for source in sources:
        print(f"    - {source}")

    print("\n  Possible wind data sources (in order of precedence):")
    print("    1. NOAA GFSwave (ugrdsfc/vgrdsfc for speed, gustsfc for gust)")
    print("    2. Open Meteo Weather (windspeed_10m, windgusts_10m) - only if BOTH fields are NULL")
    print("    3. GFS Atmospheric (gustsfc) - only for wind_gust_mph if NULL")

print("\n" + "=" * 100)
print("NEXT STEPS TO DEBUG:")
print("=" * 100)
print("1. Run debug_noaa_wind.py to check if NOAA source data is correct")
print("2. Check if there's a swap in the database column definitions")
print("3. Check if the variable mapping in NOAA or Open Meteo handlers is incorrect")
print(f"4. Manually verify data for grid_id={grid_id} at a specific timestamp")
