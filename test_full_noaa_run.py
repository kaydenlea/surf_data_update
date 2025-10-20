#!/usr/bin/env python3
"""
Full integration test for NOAA stack
Tests the complete pipeline with a small subset of beaches
"""

import sys
from datetime import datetime
import pytz

from config import logger

print("=" * 80)
print("FULL NOAA STACK INTEGRATION TEST")
print("=" * 80)
print()

# Import all handlers
from database import fetch_all_beaches, fetch_all_counties, supabase
from noaa_handler import get_noaa_dataset_url, load_noaa_dataset, get_noaa_data_bulk_optimized, validate_noaa_dataset
from nws_handler import get_nws_supplement_data
from noaa_tides_handler import get_noaa_tides_supplement_data
from usno_handler import update_daily_conditions_usno

print("Step 1: Fetching beach data from database...")
print("-" * 80)

all_beaches = fetch_all_beaches()
print(f"Total beaches in database: {len(all_beaches)}")

# Select a small test subset (5 COASTAL beaches from different regions)
# Avoid SF Bay beaches which aren't covered by ocean wave models
test_beaches = []
target_keywords = [
    ("Beach", 32.0, 33.0),  # San Diego area beach
    ("Beach", 33.5, 34.5),  # LA/OC area beach
    ("Beach", 36.0, 37.0),  # Monterey/Santa Cruz area beach
    ("Beach", 37.5, 38.5),  # SF Ocean Beach area
    ("Beach", 38.5, 41.0),  # North Coast beach
]

for keyword, lat_min, lat_max in target_keywords:
    for beach in all_beaches:
        name = beach.get("Name", "")
        lat = beach.get("LATITUDE")

        # Must have "Beach" in name and be in the right lat range (coastal, not bay)
        if keyword.lower() in name.lower() and lat_min <= lat <= lat_max:
            test_beaches.append(beach)
            print(f"  Selected: {beach['Name']} (Lat: {lat:.3f}, ID: {beach['id'][:8]}...)")
            break

    if len(test_beaches) >= 5:
        break

# Fallback: if still no beaches, get some with good coordinates
if len(test_beaches) < 3:
    print("\n  Using fallback selection...")
    for beach in all_beaches:
        lat = beach.get("LATITUDE")
        lon = beach.get("LONGITUDE")
        name = beach.get("Name", "")

        # Coastal criteria: must be west of -118 (ocean side) and have decent name
        if lon < -118 and "Beach" in name:
            test_beaches.append(beach)
            print(f"  Selected: {beach['Name']} (Lat: {lat:.3f}, Lon: {lon:.3f})")

            if len(test_beaches) >= 5:
                break

print(f"\nTesting with {len(test_beaches)} beaches")
print()

# ============================================================================
# STEP 2: NOAA GFSwave (Primary Wave Data)
# ============================================================================

print("Step 2: Fetching NOAA GFSwave data...")
print("-" * 80)

try:
    noaa_url = get_noaa_dataset_url()
    print(f"NOAA URL: {noaa_url}")

    ds = load_noaa_dataset(noaa_url)

    if not validate_noaa_dataset(ds):
        print("[FAIL] NOAA dataset validation failed")
        sys.exit(1)

    print("[OK] NOAA dataset loaded and validated")

    # Extract data for test beaches only
    noaa_records = get_noaa_data_bulk_optimized(ds, test_beaches)
    ds.close()

    print(f"[OK] Retrieved {len(noaa_records)} NOAA records")

    # Show sample
    if noaa_records:
        sample = noaa_records[0]
        print(f"\nSample NOAA record:")
        print(f"  Beach ID: {sample.get('beach_id')}")
        print(f"  Timestamp: {sample.get('timestamp')}")
        print(f"  Primary swell height: {sample.get('primary_swell_height_ft')} ft")
        print(f"  Wind speed: {sample.get('wind_speed_mph')} mph")

except Exception as e:
    print(f"[FAIL] NOAA GFSwave failed: {e}")
    sys.exit(1)

print()

# ============================================================================
# STEP 3: NOAA NWS Supplement (Weather Data)
# ============================================================================

print("Step 3: Adding NOAA NWS weather data...")
print("-" * 80)

try:
    nws_enhanced = get_nws_supplement_data(test_beaches, noaa_records)

    # Count how many fields were filled
    filled_count = 0
    for rec in nws_enhanced:
        if rec.get('temperature') is not None:
            filled_count += 1

    print(f"[OK] NWS supplemented {len(nws_enhanced)} records")
    print(f"     Temperature filled in {filled_count} records")

    # Show sample with weather data
    for rec in nws_enhanced:
        if rec.get('temperature') is not None:
            print(f"\nSample NWS-enhanced record:")
            print(f"  Beach ID: {rec.get('beach_id')}")
            print(f"  Timestamp: {rec.get('timestamp')}")
            print(f"  Temperature: {rec.get('temperature')}°F")
            print(f"  Weather code: {rec.get('weather')}")
            print(f"  Wind speed: {rec.get('wind_speed_mph')} mph")
            print(f"  Wind gust: {rec.get('wind_gust_mph')} mph")
            break

except Exception as e:
    print(f"[FAIL] NWS supplement failed: {e}")
    nws_enhanced = noaa_records
    print("[WARN] Continuing without NWS data...")

print()

# ============================================================================
# STEP 4: NOAA CO-OPS Tides Supplement
# ============================================================================

print("Step 4: Adding NOAA CO-OPS tides data...")
print("-" * 80)

try:
    fully_enhanced = get_noaa_tides_supplement_data(test_beaches, nws_enhanced)

    # Count how many fields were filled
    tide_filled = 0
    water_temp_filled = 0
    for rec in fully_enhanced:
        if rec.get('tide_level_ft') is not None:
            tide_filled += 1
        if rec.get('water_temp_f') is not None:
            water_temp_filled += 1

    print(f"[OK] Tides supplemented {len(fully_enhanced)} records")
    print(f"     Tide level filled in {tide_filled} records")
    print(f"     Water temp filled in {water_temp_filled} records")

    # Show sample with tide data
    for rec in fully_enhanced:
        if rec.get('tide_level_ft') is not None:
            print(f"\nSample tide-enhanced record:")
            print(f"  Beach ID: {rec.get('beach_id')}")
            print(f"  Timestamp: {rec.get('timestamp')}")
            print(f"  Tide level: {rec.get('tide_level_ft')} ft")
            print(f"  Water temp: {rec.get('water_temp_f')}°F")
            break

except Exception as e:
    print(f"[FAIL] Tides supplement failed: {e}")
    fully_enhanced = nws_enhanced
    print("[WARN] Continuing without tide data...")

print()

# ============================================================================
# STEP 5: USNO Daily Conditions (Sun/Moon)
# ============================================================================

print("Step 5: Fetching USNO sun/moon data...")
print("-" * 80)

try:
    # Get a small subset of counties
    all_counties = fetch_all_counties()
    test_counties = all_counties[:3]  # Just test 3 counties

    print(f"Testing with {len(test_counties)} counties:")
    for county in test_counties:
        print(f"  {county['county']}")

    daily_records = update_daily_conditions_usno(test_counties)

    print(f"\n[OK] Retrieved {len(daily_records)} daily condition records")

    # Show sample
    if daily_records:
        sample = daily_records[0]
        print(f"\nSample daily record:")
        print(f"  County: {sample.get('county')}")
        print(f"  Date: {sample.get('date')}")
        print(f"  Sunrise: {sample.get('sunrise')}")
        print(f"  Sunset: {sample.get('sunset')}")
        print(f"  Moon phase: {sample.get('moon_phase')}")

except Exception as e:
    print(f"[FAIL] USNO daily conditions failed: {e}")
    daily_records = []
    print("[WARN] Continuing without daily data...")

print()

# ============================================================================
# STEP 6: Data Quality Summary
# ============================================================================

print("Step 6: Data Quality Summary")
print("-" * 80)

# Analyze completeness
if fully_enhanced:
    total_records = len(fully_enhanced)

    completeness = {
        "primary_swell_height_ft": 0,
        "temperature": 0,
        "weather": 0,
        "wind_speed_mph": 0,
        "wind_gust_mph": 0,
        "wind_direction_deg": 0,
        "tide_level_ft": 0,
        "water_temp_f": 0,
    }

    for rec in fully_enhanced:
        for field in completeness.keys():
            if rec.get(field) is not None:
                completeness[field] += 1

    print(f"\nField completeness (out of {total_records} records):")
    for field, count in completeness.items():
        pct = (count / total_records * 100) if total_records > 0 else 0
        status = "[OK]" if pct > 80 else "[WARN]" if pct > 50 else "[FAIL]"
        print(f"  {status} {field:<30} {count:>4}/{total_records} ({pct:>5.1f}%)")

print()

# ============================================================================
# STEP 7: Optional Database Write Test (DRY RUN)
# ============================================================================

print("Step 7: Database Write Test (DRY RUN)")
print("-" * 80)
print("This would write to database:")
print(f"  Forecast records: {len(fully_enhanced)}")
print(f"  Daily records: {len(daily_records)}")
print()
print("To actually write to database, use: python main_noaa.py")
print()

# Show what would be written
print("Sample records that would be written:")
print()

# Show 3 sample forecast records
for i, rec in enumerate(fully_enhanced[:3]):
    print(f"Forecast Record {i+1}:")
    for key, value in sorted(rec.items()):
        if value is not None:
            print(f"  {key}: {value}")
    print()

# Show 1 sample daily record
if daily_records:
    print("Daily Condition Record 1:")
    for key, value in sorted(daily_records[0].items()):
        if value is not None:
            print(f"  {key}: {value}")
    print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("=" * 80)
print("INTEGRATION TEST COMPLETE")
print("=" * 80)
print()
print("Summary:")
print(f"  [OK] Tested {len(test_beaches)} beaches")
print(f"  [OK] Retrieved {len(fully_enhanced)} forecast records")
print(f"  [OK] Retrieved {len(daily_records)} daily records")
print()
print("Next steps:")
print("  1. Review the data quality summary above")
print("  2. If satisfied, run: python main_noaa.py")
print("  3. Or test with --dry-run first")
print()
print("=" * 80)
