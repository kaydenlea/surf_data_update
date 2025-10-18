#!/usr/bin/env python3
"""
Debug why row-by-row filling is missing some records.
"""

from database import supabase, fetch_all_beaches
from fill_neighbors import (
    fetch_recent_forecast_records,
    normalize_timestamp,
    has_real_value,
    haversine_distance,
    _seed_donors_from_window
)
from collections import defaultdict

# Get a specific null record to trace
resp = supabase.table('forecast_data').select('id,beach_id,timestamp,weather').is_('weather', 'null').limit(1).execute()
if not resp.data:
    print("No null records found!")
    exit()

target_record = resp.data[0]
target_id = target_record['id']
target_beach_id = target_record['beach_id']
target_ts = target_record['timestamp']

print(f"=== Tracing Record ===")
print(f"ID: {target_id}")
print(f"Beach ID: {target_beach_id}")
print(f"Timestamp: {target_ts}")
print(f"Weather: {target_record['weather']}")

# Get beach metadata
beaches = fetch_all_beaches()
beach_meta = {}
for b in beaches:
    bid = str(b.get('id'))
    lat, lon = b.get('LATITUDE'), b.get('LONGITUDE')
    if lat is not None and lon is not None:
        beach_meta[bid] = (float(lat), float(lon))

# Fetch all records
fields = ['weather']
records = fetch_recent_forecast_records(
    start_iso='1970-01-01T00:00:00',
    fields=fields,
    page_size=1000,
    limit=None,
    verbose=False
)

print(f"\nFetched {len(records)} total records")

# Build grouped structure (exactly like fill_from_neighbors_rowwise)
grouped = defaultdict(list)
meta_by_index = {}

target_idx = None
for idx, rec in enumerate(records):
    ts_key = normalize_timestamp(rec.get('timestamp'), 'H')
    if not ts_key:
        continue

    bid = str(rec.get('beach_id')) if rec.get('beach_id') is not None else None
    if bid not in beach_meta:
        continue

    grouped[ts_key].append(idx)
    meta_by_index[idx] = beach_meta[bid]

    if rec['id'] == target_id:
        target_idx = idx
        target_ts_key = ts_key
        print(f"\n✓ Found target record at index {idx}")
        print(f"  Timestamp key: {ts_key}")

if target_idx is None:
    print("\n⚠️  Target record not found in fetched data!")
    exit()

# Simulate the row-by-row fill for this specific timestamp bucket
print(f"\n=== Processing Bucket {target_ts_key} ===")

indices = grouped[target_ts_key]
print(f"Records in bucket: {len(indices)}")

# Find donors and missing (exactly like the script)
field = 'weather'
donors = []
missing = []

for idx in indices:
    meta = meta_by_index.get(idx)
    if not meta:
        continue

    lat, lon = meta
    val = records[idx].get(field)

    if has_real_value(val):
        donors.append((lat, lon, val))
    else:
        missing.append((idx, lat, lon))

print(f"Initial donors: {len(donors)}")
print(f"Initial missing: {len(missing)}")

# Check if target is in missing
target_in_missing = any(idx == target_idx for idx, _, _ in missing)
print(f"Target in missing list: {target_in_missing}")

if not donors:
    print("\n⚠️  NO donors in bucket, checking time fallback...")
    sorted_keys = sorted(grouped.keys())
    donors = _seed_donors_from_window(
        target_ts_key, field, grouped, records, meta_by_index, sorted_keys, 6
    )
    print(f"Donors from time fallback: {len(donors)}")

if not donors:
    print("\n⚠️  Still no donors available!")
    exit()

# Now simulate the EXACT row-by-row filling logic
print(f"\n=== Simulating Row-by-Row Fill ===")

filled_count = 0
iterations = 0
max_iterations = len(missing) + 10  # Safety limit

while missing and iterations < max_iterations:
    iterations += 1

    # Find the null record closest to any donor
    best_null_idx = None
    best_null_dist = float("inf")
    best_null_coords = None

    for null_idx, (idx, lat, lon) in enumerate(missing):
        min_dist_to_donor = min(
            haversine_distance(lat, lon, d_lat, d_lon)
            for d_lat, d_lon, _ in donors
        )
        if min_dist_to_donor < best_null_dist:
            best_null_dist = min_dist_to_donor
            best_null_idx = null_idx
            best_null_coords = (idx, lat, lon)

    if best_null_idx is None:
        print(f"  Iteration {iterations}: No more records to fill")
        break

    # Fill this specific null
    idx, lat, lon = best_null_coords

    # Find nearest donor value
    best_value = None
    best_distance = float("inf")
    for d_lat, d_lon, d_val in donors:
        d = haversine_distance(lat, lon, d_lat, d_lon)
        if d < best_distance:
            best_distance = d
            best_value = d_val

    if best_value is not None:
        # Fill the value
        records[idx][field] = best_value
        filled_count += 1

        if idx == target_idx:
            print(f"\n✓✓✓ TARGET RECORD FILLED on iteration {iterations}!")
            print(f"    Filled with value: {best_value}")
            print(f"    Distance: {best_distance:.1f} km")

        # Add as donor
        donors.append((lat, lon, best_value))

    # Remove from missing list
    missing.pop(best_null_idx)

print(f"\n=== Results ===")
print(f"Iterations: {iterations}")
print(f"Records filled: {filled_count}")
print(f"Records remaining: {len(missing)}")

if len(missing) > 0:
    print(f"\n⚠️  {len(missing)} records were NOT filled!")

    # Check if target is among unfilled
    unfilled_target = any(idx == target_idx for idx, _, _ in missing)
    if unfilled_target:
        print(f"⚠️⚠️⚠️  TARGET RECORD WAS NOT FILLED!")
    else:
        print(f"✓ Target was filled")
else:
    print(f"✓ All records filled successfully")

# Check final value of target record
final_value = records[target_idx].get(field)
print(f"\nTarget record final value: {final_value}")
