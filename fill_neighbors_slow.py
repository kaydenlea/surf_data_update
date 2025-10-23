#!/usr/bin/env python3
"""
Slow but guaranteed 100% fill - processes and writes each null individually.
Use this as a final cleanup pass after the fast batch script.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import haversine_distance, has_real_value, normalize_timestamp
from collections import defaultdict
import time

def should_skip_filling(rec, field):
    """
    Check if a field should be skipped for filling based on other field values.

    RULE: Skip surf_height_min_ft if surf_height_max_ft is 1 (no surf condition)
    """
    if field == "surf_height_min_ft":
        max_height = rec.get("surf_height_max_ft")
        if max_height is not None:
            try:
                if float(max_height) == 1.0:
                    return True  # Skip filling min if max is 1 (no surf)
            except (ValueError, TypeError):
                pass

    return False


def fill_one_record_slow(record_id, beach_id, timestamp, field, beach_meta, all_records_at_ts, current_record):
    """Fill a single null value by finding nearest donor."""

    # Check if we should skip filling this field for this record
    if should_skip_filling(current_record, field):
        return None

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

def get_all_fillable_fields():
    """
    Auto-detect all fillable columns from the forecast_data table.
    Excludes primary keys, identifiers, and timestamps.
    """
    EXCLUDED_FIELDS = {"id", "beach_id", "timestamp", "created_at", "updated_at"}

    try:
        # Get a sample record to find all column names
        result = supabase.table("forecast_data").select("*").limit(1).execute()

        if not result.data:
            # Fallback to known fields if table is empty
            return [
                "primary_swell_height_ft", "primary_swell_period_s", "primary_swell_direction",
                "secondary_swell_height_ft", "secondary_swell_period_s", "secondary_swell_direction",
                "tertiary_swell_height_ft", "tertiary_swell_period_s", "tertiary_swell_direction",
                "surf_height_min_ft", "surf_height_max_ft", "wave_energy_kj",
                "wind_speed_mph", "wind_direction_deg", "wind_gust_mph",
                "water_temp_f", "tide_level_ft", "temperature", "weather", "pressure_inhg",
            ]

        # Extract all field names except excluded ones
        all_fields = set(result.data[0].keys())
        fillable_fields = sorted(all_fields - EXCLUDED_FIELDS)

        return fillable_fields

    except Exception as e:
        print(f"WARNING: Could not auto-detect fields: {e}, using fallback list")
        return [
            "primary_swell_height_ft", "primary_swell_period_s", "primary_swell_direction",
            "secondary_swell_height_ft", "secondary_swell_period_s", "secondary_swell_direction",
            "tertiary_swell_height_ft", "tertiary_swell_period_s", "tertiary_swell_direction",
            "surf_height_min_ft", "surf_height_max_ft", "wave_energy_kj",
            "wind_speed_mph", "wind_direction_deg", "wind_gust_mph",
            "water_temp_f", "tide_level_ft", "temperature", "weather", "pressure_inhg",
        ]


def main():
    print("="*80)
    print("SLOW ROW-BY-ROW FILL - AUTO-DETECTS ALL COLUMNS (Final Cleanup Pass)")
    print("="*80)

    # Get beach metadata
    beaches = fetch_all_beaches()
    beach_meta = {}
    for b in beaches:
        bid = str(b.get('id'))
        lat, lon = b.get('LATITUDE'), b.get('LONGITUDE')
        if lat is not None and lon is not None:
            beach_meta[bid] = (float(lat), float(lon))

    print(f"Loaded {len(beach_meta)} beaches with coordinates")

    # Auto-detect all fillable fields
    fields = get_all_fillable_fields()

    # IMPORTANT: Process surf_height_max_ft before surf_height_min_ft
    if "surf_height_max_ft" in fields and "surf_height_min_ft" in fields:
        max_idx = fields.index("surf_height_max_ft")
        min_idx = fields.index("surf_height_min_ft")
        if min_idx < max_idx:
            # Swap them so max comes first
            fields[max_idx], fields[min_idx] = fields[min_idx], fields[max_idx]
            print("Reordered: surf_height_max_ft will be processed before surf_height_min_ft")

    print(f"\nAuto-detected {len(fields)} fillable fields:")
    print(f"  {', '.join(fields)}")
    print("="*80)

    total_filled = 0

    for field in fields:
        print(f"\nProcessing field: {field}")

        # Find all nulls for this field
        # IMPORTANT: Also fetch surf_height_max_ft if we're processing surf_height_min_ft
        # so we can check the skip condition
        select_fields = 'id,beach_id,timestamp'
        if field == 'surf_height_min_ft':
            select_fields += ',surf_height_max_ft'

        # Retry logic for initial null record fetch
        max_retries = 3
        retry_delay = 1.0
        null_records = None

        for attempt in range(max_retries):
            try:
                null_records = supabase.table('forecast_data').select(
                    select_fields
                ).is_(field, 'null').execute()
                break  # Success
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"\n  ⚠️ Database read error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}")
                    print(f"  Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"\n  ❌ Failed to fetch null records after {max_retries} attempts: {str(e)[:100]}")
                    raise  # Re-raise after all retries exhausted

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

                # Retry logic for database reads
                max_retries = 3
                retry_delay = 1.0
                resp = None

                for attempt in range(max_retries):
                    try:
                        resp = supabase.table('forecast_data').select(
                            f'beach_id,{field}'
                        ).eq('timestamp', ts_key).range(start, end).execute()
                        break  # Success
                    except Exception as e:
                        if attempt < max_retries - 1:
                            print(f"\n    ⚠️ Database read error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}")
                            print(f"    Retrying in {retry_delay}s...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            print(f"\n    ❌ Failed to read data after {max_retries} attempts: {str(e)[:100]}")
                            raise  # Re-raise after all retries exhausted

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

                # Fill this one record (pass null_rec so we can check skip conditions)
                filled_value = fill_one_record_slow(
                    record_id, beach_id, timestamp, field, beach_meta, all_at_ts, null_rec
                )

                if filled_value is not None:
                    # Write immediately to database with retry logic
                    max_retries = 3
                    retry_delay = 1.0

                    for attempt in range(max_retries):
                        try:
                            supabase.table('forecast_data').update({
                                field: filled_value
                            }).eq('id', record_id).execute()
                            break  # Success - exit retry loop
                        except Exception as e:
                            if attempt < max_retries - 1:
                                print(f"\n    ⚠️ Database write error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}")
                                print(f"    Retrying in {retry_delay}s...")
                                time.sleep(retry_delay)
                                retry_delay *= 2  # Exponential backoff
                            else:
                                print(f"\n    ❌ Failed to write record {record_id} after {max_retries} attempts: {str(e)[:100]}")
                                print(f"    Skipping this record and continuing...")
                                filled_value = None  # Mark as failed so we don't count it

                    if filled_value is not None:
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
