#!/usr/bin/env python3
"""
Debug script to understand why nulls increase between iterations.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import fetch_recent_forecast_records, fill_from_neighbors
from datetime import datetime, timedelta
import pytz
from collections import Counter

def debug_iterations():
    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get("id"))
        lat, lon = b.get("LATITUDE"), b.get("LONGITUDE")
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches")

    # Use a specific time range
    start_iso = (datetime.now(pytz.UTC) - timedelta(days=7)).isoformat()
    fields = ["weather", "tertiary_swell_height_ft", "tertiary_swell_period_s"]

    print(f"\nStarting debug with fields: {fields}")

    # Iteration 1
    print(f"\n{'='*60}")
    print("ITERATION 1")
    print(f"{'='*60}")

    records_1 = fetch_recent_forecast_records(
        start_iso=start_iso,
        fields=fields,
        page_size=1000,
        limit=2000,
        verbose=False
    )

    print(f"Fetched {len(records_1)} records")

    # Count nulls BEFORE filling
    nulls_before_1 = {}
    for field in fields:
        nulls_before_1[field] = sum(1 for r in records_1 if r.get(field) is None)

    print(f"\nNulls BEFORE iteration 1:")
    for field, count in nulls_before_1.items():
        print(f"  {field}: {count}")

    # Fill
    updates_1, stats_1 = fill_from_neighbors(
        records_1,
        beach_meta,
        fields,
        verbose=False,
        time_fallback=6,
        cadence="H"
    )

    print(f"\nFilled in iteration 1:")
    for field in fields:
        filled = stats_1.get("field_filled", {}).get(field, 0)
        print(f"  {field}: {filled}")

    # Count nulls AFTER filling (in memory)
    nulls_after_fill_1 = {}
    for field in fields:
        nulls_after_fill_1[field] = sum(1 for r in records_1 if r.get(field) is None)

    print(f"\nNulls AFTER filling (in memory):")
    for field, count in nulls_after_fill_1.items():
        print(f"  {field}: {count}")

    # Check the updates being prepared
    print(f"\nPrepared {len(updates_1)} update payloads")

    # Sample a few updates to see what they contain
    print(f"\nSample updates:")
    for update in updates_1[:3]:
        fields_in_update = [k for k in update.keys() if k not in ('id', 'beach_id', 'timestamp')]
        values = {k: update[k] for k in fields_in_update}
        print(f"  Record {update['id'][:8]}...: {values}")

    # Check if any updates contain None values (they shouldn't!)
    problematic_updates = []
    for update in updates_1:
        for field in fields:
            if field in update and update[field] is None:
                problematic_updates.append((update['id'], field))

    if problematic_updates:
        print(f"\n⚠️  FOUND {len(problematic_updates)} UPDATES WITH NULL VALUES!")
        for record_id, field in problematic_updates[:5]:
            print(f"  Record {record_id}: {field} = None")
    else:
        print(f"\n✓ No problematic updates found (all non-null)")

    # Now simulate what happens in iteration 2
    print(f"\n{'='*60}")
    print("ITERATION 2 (simulated)")
    print(f"{'='*60}")

    # In real script, this would re-fetch from DB after writing updates_1
    # For testing, let's see what we'd get
    records_2 = fetch_recent_forecast_records(
        start_iso=start_iso,
        fields=fields,
        page_size=1000,
        limit=2000,
        verbose=False
    )

    print(f"Fetched {len(records_2)} records")

    # Count nulls BEFORE filling iteration 2
    nulls_before_2 = {}
    for field in fields:
        nulls_before_2[field] = sum(1 for r in records_2 if r.get(field) is None)

    print(f"\nNulls BEFORE iteration 2 (from fresh DB fetch):")
    for field, count in nulls_before_2.items():
        change = count - nulls_before_1[field]
        print(f"  {field}: {count} (change: {change:+d})")

    # This is the bug - if nulls INCREASED, something is wrong!
    for field in fields:
        if nulls_before_2[field] > nulls_before_1[field]:
            print(f"\n⚠️  BUG DETECTED: {field} nulls increased from {nulls_before_1[field]} to {nulls_before_2[field]}!")
            print(f"    This means iteration 1's fills were not persisted, OR new nulls were inserted")

if __name__ == "__main__":
    debug_iterations()
