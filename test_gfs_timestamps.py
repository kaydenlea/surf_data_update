#!/usr/bin/env python3
"""
Quick test to check GFS timestamp alignment
"""
from gfs_atmospheric_handler import get_gfs_atmospheric_dataset_url, load_gfs_atmospheric_dataset
import pandas as pd
import pytz
from datetime import datetime

# Get GFS dataset
print("Fetching GFS dataset...")
url = get_gfs_atmospheric_dataset_url()
if not url:
    print("ERROR: Could not find GFS dataset")
    exit(1)

ds = load_gfs_atmospheric_dataset(url)
if not ds:
    print("ERROR: Could not load GFS dataset")
    exit(1)

# Get GFS times
time_vals = pd.to_datetime(ds.time.values)
if time_vals.tz is None:
    time_vals = time_vals.tz_localize("UTC")
else:
    time_vals = time_vals.tz_convert("UTC")

print(f"\nGFS Time Range: {time_vals[0]} to {time_vals[-1]}")
print(f"\nFirst 10 GFS times (UTC):")
for i, t in enumerate(time_vals[:10]):
    pacific = t.tz_convert("America/Los_Angeles")
    print(f"{i:2d}. UTC: {t}  |  Pacific: {pacific}")

# Test with some sample database timestamps (representing Pacific afternoon)
print("\n" + "="*80)
print("TESTING TIMESTAMP MATCHING")
print("="*80)

pacific_tz = pytz.timezone("America/Los_Angeles")
test_times_pacific = [
    "2025-10-27 00:00:00",  # Midnight Pacific
    "2025-10-27 03:00:00",  # 3 AM Pacific
    "2025-10-27 06:00:00",  # 6 AM Pacific
    "2025-10-27 09:00:00",  # 9 AM Pacific
    "2025-10-27 12:00:00",  # Noon Pacific (should be HOTTEST)
    "2025-10-27 15:00:00",  # 3 PM Pacific (should be hot)
    "2025-10-27 18:00:00",  # 6 PM Pacific
    "2025-10-27 21:00:00",  # 9 PM Pacific
]

print("\n--- OLD METHOD (BROKEN - matching UTC to UTC directly) ---")
for pt_str in test_times_pacific:
    # Create Pacific time
    pt = pacific_tz.localize(datetime.strptime(pt_str, "%Y-%m-%d %H:%M:%S"))
    # Convert to UTC (this is how it's stored in database)
    utc = pt.astimezone(pytz.UTC)

    # OLD WAY: Find closest GFS time by matching UTC to UTC
    import numpy as np
    time_diffs = np.abs(time_vals - utc)
    closest_idx = time_diffs.argmin()
    closest_gfs_time = time_vals[closest_idx]
    diff_hours = time_diffs[closest_idx] / pd.Timedelta(hours=1)

    closest_gfs_pacific = closest_gfs_time.tz_convert("America/Los_Angeles")

    print(f"\nPacific: {pt_str}")
    print(f"  DB stores as UTC: {utc}")
    print(f"  OLD: Matches GFS UTC:  {closest_gfs_time}")
    print(f"  OLD: GFS as Pacific:   {closest_gfs_pacific}  <-- WRONG!")
    print(f"  Difference: {diff_hours:.2f} hours")

print("\n" + "="*80)
print("\n--- NEW METHOD (FIXED - matching Pacific to Pacific) ---")
for pt_str in test_times_pacific:
    # Create Pacific time
    pt = pacific_tz.localize(datetime.strptime(pt_str, "%Y-%m-%d %H:%M:%S"))
    # Convert to UTC (this is how it's stored in database)
    utc = pt.astimezone(pytz.UTC)

    # NEW WAY: Convert database UTC back to Pacific, then match Pacific to Pacific
    import numpy as np
    ts_pacific = utc.astimezone(pacific_tz)
    gfs_times_pacific = time_vals.tz_convert("America/Los_Angeles")
    time_diffs = np.abs(gfs_times_pacific - ts_pacific)
    closest_idx = time_diffs.argmin()
    closest_gfs_time_utc = time_vals[closest_idx]
    closest_gfs_time_pacific = gfs_times_pacific[closest_idx]
    diff_hours = time_diffs[closest_idx] / pd.Timedelta(hours=1)

    print(f"\nPacific: {pt_str}")
    print(f"  DB stores as UTC: {utc}")
    print(f"  NEW: Matches GFS Pacific: {closest_gfs_time_pacific}  <-- CORRECT!")
    print(f"  NEW: Matches GFS UTC:     {closest_gfs_time_utc}")
    print(f"  Difference: {diff_hours:.2f} hours")

ds.close()
print("\nDone!")
