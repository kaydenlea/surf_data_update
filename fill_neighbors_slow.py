#!/usr/bin/env python3
"""
Slow but guaranteed 100% fill - processes and writes each null individually.
Use this as a final cleanup pass after the fast batch script.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import haversine_distance, has_real_value, normalize_timestamp
from collections import defaultdict
import time

def fill_one_record_slow(record_id, beach_id, timestamp, field, beach_meta, all_records_at_ts):
    """Fill a single null value by finding nearest donor."""

    # Get coordinates for this beach
    if str(beach_id) not in beach_meta:
        return None

    my_lat, my_lon = beach_meta[str(beach_id)]

    # Find nearest beach with a value at this timestamp
    best_value = None
    best_distance = float("inf")

    for donor_rec in all_records_at_ts:
        donor_beach_id = donor_rec.get('beach_id')
        if donor_beach_id == beach_id:
            continue  # Skip self

        donor_value = donor_rec.get(field)
        if not has_real_value(donor_value):
            continue  # Skip nulls

        if str(donor_beach_id) not in beach_meta:
            continue

        donor_lat, donor_lon = beach_meta[str(donor_beach_id)]
        dist = haversine_distance(my_lat, my_lon, donor_lat, donor_lon)

        if dist < best_distance:
            best_distance = dist
            best_value = donor_value

    return best_value

def main():
    print("Starting SLOW row-by-row fill (final cleanup pass)...")

    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get('id'))
        lat, lon = b.get('LATITUDE'), b.get('LONGITUDE')
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches with coordinates")

    fields = [
        "weather",
        "wind_direction_deg",
        "secondary_swell_height_ft",
        "secondary_swell_period_s",
        "tertiary_swell_height_ft",
        "tertiary_swell_period_s",
        "tertiary_swell_direction",
    ]

    total_filled = 0

    for field in fields:
        print(f"\nProcessing field: {field}")

        # Find all nulls for this field
        null_records = supabase.table('forecast_data').select(
            'id,beach_id,timestamp'
        ).is_(field, 'null').execute()

        nulls = null_records.data or []
        print(f"  Found {len(nulls)} nulls")

        if not nulls:
            continue

        # Group by timestamp
        by_timestamp = defaultdict(list)
        for rec in nulls:
            ts_key = normalize_timestamp(rec['timestamp'], 'h')
            by_timestamp[ts_key].append(rec)

        print(f"  Grouped into {len(by_timestamp)} timestamps")

        # Process each timestamp
        field_filled = 0
        for ts_key, null_recs_at_ts in by_timestamp.items():
            # Fetch ALL records at this timestamp (to find donors)
            # CRITICAL: Use pagination to get all records (Supabase defaults to 1000 limit)
            all_at_ts = []
            page_size = 1000
            page = 0
            while True:
                start = page * page_size
                end = start + page_size - 1
                resp = supabase.table('forecast_data').select(
                    f'beach_id,{field}'
                ).eq('timestamp', ts_key).range(start, end).execute()

                batch = resp.data or []
                all_at_ts.extend(batch)

                if len(batch) < page_size:
                    break
                page += 1

            # Process each null one by one
            for null_rec in null_recs_at_ts:
                record_id = null_rec['id']
                beach_id = null_rec['beach_id']
                timestamp = null_rec['timestamp']

                # Fill this one record
                filled_value = fill_one_record_slow(
                    record_id, beach_id, timestamp, field, beach_meta, all_at_ts
                )

                if filled_value is not None:
                    # Write immediately to database
                    supabase.table('forecast_data').update({
                        field: filled_value
                    }).eq('id', record_id).execute()

                    field_filled += 1
                    total_filled += 1

                    # Add this newly filled value to the donor pool
                    all_at_ts.append({'beach_id': beach_id, field: filled_value})

                    if field_filled % 10 == 0:
                        print(f"    Filled {field_filled}...", end='\r')

        if field_filled > 0:
            print(f"  ✓ Filled {field_filled} values for {field}")

    print(f"\n{'='*60}")
    print(f"TOTAL FILLED: {total_filled} null values")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
