#!/usr/bin/env python3
"""
Check why specific null records can't be filled.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import normalize_timestamp, has_real_value
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

def check_unfillable_nulls():
    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get("id"))
        lat, lon = b.get("LATITUDE"), b.get("LONGITUDE")
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches")

    # Get records with nulls from last 7 days
    start_iso = (datetime.now(pytz.UTC) - timedelta(days=7)).isoformat()

    # Check weather nulls
    resp = supabase.table("forecast_data").select(
        "id,beach_id,timestamp,weather"
    ).is_("weather", "null").gte("timestamp", start_iso).limit(500).execute()

    null_records = resp.data or []
    print(f"\nFound {len(null_records)} records with null weather")

    if not null_records:
        print("No nulls found!")
        return

    # Group by timestamp
    by_timestamp = defaultdict(list)
    for rec in null_records:
        ts_key = normalize_timestamp(rec["timestamp"], "H")
        by_timestamp[ts_key].append(rec)

    print(f"Grouped into {len(by_timestamp)} timestamp buckets")

    # For each timestamp bucket, check if donors exist in ±6 hour window
    unfillable_timestamps = []

    for ts_key, nulls in list(by_timestamp.items())[:10]:  # Check first 10 buckets
        # Parse the timestamp
        dt = datetime.fromisoformat(ts_key).replace(tzinfo=pytz.UTC)

        # Build ±6 hour window
        timestamps_to_check = []
        for hour_offset in range(-6, 7):
            check_dt = dt + timedelta(hours=hour_offset)
            check_key = normalize_timestamp(check_dt, "H")
            timestamps_to_check.append(check_key)

        print(f"\nChecking timestamp: {ts_key} ({len(nulls)} nulls)")
        print(f"  Checking ±6 hour window: {timestamps_to_check[0]} to {timestamps_to_check[-1]}")

        # Check if ANY record in the window has weather data
        donors_in_window = 0
        for check_ts in timestamps_to_check:
            # Check database for records at this specific timestamp with weather data
            resp = supabase.table("forecast_data").select("id,weather").eq(
                "timestamp", check_ts
            ).not_.is_("weather", "null").limit(1).execute()

            if resp.data:
                donors_in_window += 1

        print(f"  Found donors in {donors_in_window}/13 buckets in window")

        if donors_in_window == 0:
            unfillable_timestamps.append(ts_key)
            print(f"  ⚠️  NO DONORS in entire ±6 hour window!")

    if unfillable_timestamps:
        print(f"\n{'='*60}")
        print(f"Found {len(unfillable_timestamps)} timestamp buckets with NO donors in ±6 hour window:")
        for ts in unfillable_timestamps:
            print(f"  {ts}")
        print("\nThese nulls are genuinely unfillable with current data.")
    else:
        print(f"\n{'='*60}")
        print("All checked timestamps have donors available within ±6 hours!")
        print("The fill script should be able to fill these nulls.")

if __name__ == "__main__":
    check_unfillable_nulls()
