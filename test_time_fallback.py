#!/usr/bin/env python3
"""
Test if time fallback is working correctly.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import fill_from_neighbors, normalize_timestamp, fetch_recent_forecast_records
from datetime import datetime, timedelta
import pytz

def test_time_fallback():
    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get("id"))
        lat, lon = b.get("LATITUDE"), b.get("LONGITUDE")
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches")

    # Get records from last 7 days
    start_iso = (datetime.now(pytz.UTC) - timedelta(days=7)).isoformat()

    fields = ["weather", "wind_direction_deg", "secondary_swell_height_ft"]

    records = fetch_recent_forecast_records(
        start_iso=start_iso,
        fields=fields,
        page_size=1000,
        limit=5000,
        verbose=False
    )

    print(f"Fetched {len(records)} records")

    # Count nulls before
    nulls_before = {}
    for field in fields:
        nulls_before[field] = sum(1 for r in records if r.get(field) is None)

    print(f"\nNulls BEFORE filling:")
    for field, count in nulls_before.items():
        print(f"  {field}: {count}")

    # Test WITHOUT time fallback
    print(f"\n{'='*60}")
    print("Testing WITHOUT time fallback (time_fallback=0)")
    print(f"{'='*60}")

    updates_no_fb, stats_no_fb = fill_from_neighbors(
        records.copy(),
        beach_meta,
        fields,
        verbose=False,
        time_fallback=0,
        cadence="H"
    )

    filled_no_fb = stats_no_fb.get("field_filled", {})
    no_donor_no_fb = stats_no_fb.get("field_no_donor", {})

    print(f"\nFilled counts:")
    for field in fields:
        print(f"  {field}: {filled_no_fb.get(field, 0)}")

    print(f"\nNo donor counts:")
    for field in fields:
        print(f"  {field}: {no_donor_no_fb.get(field, 0)}")

    # Test WITH time fallback
    print(f"\n{'='*60}")
    print("Testing WITH time fallback (time_fallback=6)")
    print(f"{'='*60}")

    updates_fb, stats_fb = fill_from_neighbors(
        records.copy(),
        beach_meta,
        fields,
        verbose=False,
        time_fallback=6,
        cadence="H"
    )

    filled_fb = stats_fb.get("field_filled", {})
    no_donor_fb = stats_fb.get("field_no_donor", {})

    print(f"\nFilled counts:")
    for field in fields:
        print(f"  {field}: {filled_fb.get(field, 0)}")

    print(f"\nNo donor counts:")
    for field in fields:
        print(f"  {field}: {no_donor_fb.get(field, 0)}")

    # Compare
    print(f"\n{'='*60}")
    print("COMPARISON")
    print(f"{'='*60}")

    for field in fields:
        improvement = filled_fb.get(field, 0) - filled_no_fb.get(field, 0)
        print(f"{field}:")
        print(f"  Without fallback: {filled_no_fb.get(field, 0)} filled, {no_donor_no_fb.get(field, 0)} no donor")
        print(f"  With fallback:    {filled_fb.get(field, 0)} filled, {no_donor_fb.get(field, 0)} no donor")
        print(f"  Improvement:      +{improvement}")

if __name__ == "__main__":
    test_time_fallback()
