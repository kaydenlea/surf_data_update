#!/usr/bin/env python3
"""
Diagnostic script to understand why neighbor filling is missing some nulls.
"""

from database import supabase
from collections import defaultdict

def check_null_distribution():
    """Check if nulls have potential donors available."""

    # Get a sample of records with nulls
    resp = supabase.table("forecast_data").select(
        "id,beach_id,timestamp,weather,wind_direction_deg,"
        "secondary_swell_height_ft,secondary_swell_period_s,"
        "tertiary_swell_height_ft,tertiary_swell_period_s,tertiary_swell_direction"
    ).gte("timestamp", "2025-10-11T00:00:00").limit(10000).execute()

    records = resp.data or []
    print(f"Fetched {len(records)} records")

    # Group by timestamp
    by_timestamp = defaultdict(list)
    for rec in records:
        ts = rec['timestamp']
        by_timestamp[ts].append(rec)

    # Check each timestamp bucket
    fields = ['weather', 'wind_direction_deg', 'secondary_swell_height_ft',
              'secondary_swell_period_s', 'tertiary_swell_height_ft',
              'tertiary_swell_period_s', 'tertiary_swell_direction']

    problem_buckets = defaultdict(list)

    for ts, bucket_records in by_timestamp.items():
        for field in fields:
            has_donor = any(rec[field] is not None for rec in bucket_records)
            has_null = any(rec[field] is None for rec in bucket_records)

            if has_null and not has_donor:
                problem_buckets[ts].append(field)

    if problem_buckets:
        print(f"\nFound {len(problem_buckets)} timestamp buckets with nulls but NO donors:")
        for ts, fields_list in list(problem_buckets.items())[:5]:
            print(f"  {ts}: {', '.join(fields_list)}")
    else:
        print("\nAll timestamp buckets with nulls have at least one donor!")
        print("This means the batch filling logic should work.")

    # Check for specific null records
    null_weather = [r for r in records if r['weather'] is None]
    print(f"\nTotal records with null weather: {len(null_weather)}")

    if null_weather:
        sample = null_weather[0]
        sample_ts = sample['timestamp']
        same_ts = [r for r in records if r['timestamp'] == sample_ts]
        donors = [r for r in same_ts if r['weather'] is not None]
        print(f"Sample null record: beach_id={sample['beach_id']}, timestamp={sample_ts}")
        print(f"  Same timestamp has {len(same_ts)} total records, {len(donors)} with non-null weather")

if __name__ == "__main__":
    check_null_distribution()
