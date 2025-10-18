#!/usr/bin/env python3
"""
Test the batch filling logic to identify edge cases.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import normalize_timestamp, has_real_value, haversine_distance
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

def test_specific_timestamp():
    """Test filling logic for a specific timestamp where nulls exist."""

    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get("id"))
        lat, lon = b.get("LATITUDE"), b.get("LONGITUDE")
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches with coordinates")

    # Get recent data - check last 7 days
    start_iso = (datetime.now(pytz.UTC) - timedelta(days=7)).isoformat()

    resp = supabase.table("forecast_data").select(
        "id,beach_id,timestamp,weather,wind_direction_deg,secondary_swell_height_ft"
    ).gte("timestamp", start_iso).limit(20000).execute()

    records = resp.data or []
    print(f"\nFetched {len(records)} records")

    # Group by timestamp
    grouped = defaultdict(list)
    for idx, rec in enumerate(records):
        ts_key = normalize_timestamp(rec.get("timestamp"), "H")
        if ts_key and str(rec.get("beach_id")) in beach_meta:
            grouped[ts_key].append(idx)

    print(f"Grouped into {len(grouped)} time buckets")

    # Find a bucket with nulls - try multiple fields
    for field in ["weather", "wind_direction_deg", "secondary_swell_height_ft"]:
        print(f"\n{'='*60}")
        print(f"Testing field: {field}")
        print(f"{'='*60}")

        found_issue = False
        for ts_key, indices in grouped.items():
            donors = []
            missing = []

            for idx in indices:
                rec = records[idx]
                bid = str(rec.get("beach_id"))
                if bid not in beach_meta:
                    continue

                lat, lon = beach_meta[bid]
                val = rec.get(field)

                if has_real_value(val):
                    donors.append((lat, lon, val, bid))
                else:
                    missing.append((idx, lat, lon, bid))

            # Look for a bucket with both donors and missing
            if donors and missing and len(missing) >= 3:
                print(f"\nAnalyzing timestamp: {ts_key}")
                print(f"Donors: {len(donors)}, Missing: {len(missing)}")
                print(f"Donor beaches: {[d[3] for d in donors[:5]]}")
                print(f"Missing beaches: {[m[3] for m in missing[:10]]}")

                # Simulate the filling process
                donor_list = [(d[0], d[1], d[2]) for d in donors]
                filled_count = 0

                for idx, lat, lon, bid in missing:
                    best_value, best_distance = None, float("inf")
                    for d_lat, d_lon, d_val in donor_list:
                        d = haversine_distance(lat, lon, d_lat, d_lon)
                        if d < best_distance:
                            best_distance, best_value = d, d_val

                    if best_value is not None:
                        filled_count += 1
                        # Add to donors for next iteration
                        donor_list.append((lat, lon, best_value))

                print(f"Simulation: Would fill {filled_count}/{len(missing)} records")

                if filled_count < len(missing):
                    print(f"⚠️  MISSED {len(missing) - filled_count} records!")
                    found_issue = True
                else:
                    print(f"✓ All records filled successfully")

                # Only show first bucket
                break

        if not found_issue:
            print(f"No issues found for {field}")

if __name__ == "__main__":
    test_specific_timestamp()
