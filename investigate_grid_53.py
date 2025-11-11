#!/usr/bin/env python3
"""
Investigate grid 53 which has 100% bad wind data
"""
from database import supabase

print("=" * 100)
print("INVESTIGATING GRID 53 (100% bad wind data)")
print("=" * 100)

# Get grid 53 info
response = supabase.table("grid_points").select("*").eq("id", 53).execute()
if response.data:
    grid = response.data[0]
    print(f"\nGrid 53 location:")
    print(f"  Latitude: {grid['latitude']}")
    print(f"  Longitude: {grid['longitude']}")
else:
    print("Grid 53 not found!")
    exit(1)

# Get all records for grid 53 with wind data
response = supabase.table("grid_forecast_data").select(
    "*"
).eq("grid_id", 53).not_.is_("wind_speed_mph", "null").not_.is_("wind_gust_mph", "null").execute()

records = response.data

print(f"\nFound {len(records)} records with wind data for grid 53:")
print("=" * 100)
print(f"{'Timestamp (UTC)':<30} {'Wind Speed':<15} {'Wind Gust':<15} {'Diff':<10} {'Status':<10} {'Has NOAA Data?':<15}")
print("-" * 100)

for rec in records:
    timestamp = rec.get('timestamp')
    wind_speed = rec.get('wind_speed_mph')
    wind_gust = rec.get('wind_gust_mph')

    # Check if this has NOAA wave data (suggests it's from NOAA, not Open Meteo)
    has_noaa = rec.get('primary_swell_height_ft') is not None

    if wind_speed is not None and wind_gust is not None:
        diff = wind_gust - wind_speed
        status = "BAD" if wind_gust < wind_speed else "OK"
    else:
        diff = None
        status = "N/A"

    diff_str = f"{diff:.1f}" if diff is not None else "N/A"
    has_noaa_str = "Yes (NOAA)" if has_noaa else "No (Open Meteo?)"

    print(f"{timestamp:<30} {wind_speed:<15.2f} {wind_gust:<15.2f} {diff_str:<10} {status:<10} {has_noaa_str:<15}")

print("\n" + "=" * 100)
print("ANALYSIS:")
print("=" * 100)
print("If 'Has NOAA Data?' shows 'Yes', then wind data likely came from NOAA GFSwave")
print("If 'Has NOAA Data?' shows 'No', then wind data likely came from Open Meteo supplement")
print("\nCheck if the bad data correlates with a specific data source.")
