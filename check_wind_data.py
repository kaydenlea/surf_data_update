#!/usr/bin/env python3
"""
Check wind data in the database to see if gust < speed
"""
from database import supabase
from config import logger

logger.info("Fetching recent grid forecast data to check wind values...")

# Get a sample of recent data
response = supabase.table("grid_forecast_data").select(
    "grid_id, timestamp, wind_speed_mph, wind_gust_mph"
).not_.is_("wind_speed_mph", "null").not_.is_("wind_gust_mph", "null").limit(1000).execute()

records = response.data

if not records:
    print("No records found with wind data")
    exit(0)

print(f"Analyzing {len(records)} records with wind data...")
print("=" * 100)

issue_count = 0
total_count = len(records)

# Show first 20 records
print(f"{'Grid ID':<10} {'Timestamp':<30} {'Wind Speed (mph)':<20} {'Wind Gust (mph)':<20} {'Status':<10}")
print("-" * 100)

for i, rec in enumerate(records[:20]):
    grid_id = rec.get('grid_id')
    timestamp = rec.get('timestamp')
    wind_speed = rec.get('wind_speed_mph')
    wind_gust = rec.get('wind_gust_mph')

    if wind_speed is not None and wind_gust is not None:
        status = "BAD" if wind_gust < wind_speed else "OK"
        if wind_gust < wind_speed:
            issue_count += 1
    else:
        status = "N/A"

    print(f"{grid_id:<10} {timestamp:<30} {wind_speed:<20.1f} {wind_gust:<20.1f} {status:<10}")

# Count all issues
issue_count = 0
for rec in records:
    wind_speed = rec.get('wind_speed_mph')
    wind_gust = rec.get('wind_gust_mph')

    if wind_speed is not None and wind_gust is not None:
        if wind_gust < wind_speed:
            issue_count += 1

print("\n" + "=" * 100)
print("SUMMARY:")
print(f"Total records analyzed: {total_count}")
print(f"Records where gust < speed: {issue_count}")
print(f"Percentage with issue: {(issue_count/total_count*100) if total_count > 0 else 0:.1f}%")

if issue_count > total_count * 0.5:
    print("\n*** WARNING: More than 50% of records have gust < speed ***")
    print("*** This strongly suggests the wind_speed_mph and wind_gust_mph columns are SWAPPED! ***")
    print("\nPossible solutions:")
    print("1. Check if the column names in the database are correct")
    print("2. Check if wind data sources are providing data in the wrong order")
    print("3. Swap the values when writing to the database")
