#!/usr/bin/env python3
"""
Test script to verify deduplication preserves all values from different data sources
"""

import sys
from datetime import datetime
import pytz
from database import _deduplicate_records
from config import logger

def test_deduplication_preserves_all_values():
    """Test that deduplication correctly merges records from different sources."""

    logger.info("=" * 80)
    logger.info("TESTING DEDUPLICATION LOGIC")
    logger.info("=" * 80)
    logger.info("")

    # Simulate records from different data sources for the same beach + timestamp
    beach_id = "test-beach-123"
    timestamp = "2025-10-29T12:00:00-07:00"

    # Record 1: NOAA GFSwave data (ocean/wave fields)
    noaa_record = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "primary_swell_height_ft": 5.5,
        "primary_swell_period_s": 12.0,
        "primary_swell_direction": 285.0,
        "secondary_swell_height_ft": 2.0,
        "secondary_swell_period_s": 8.0,
        "secondary_swell_direction": 210.0,
        "surf_height_min_ft": 4.0,
        "surf_height_max_ft": 7.0,
        "wave_energy_kj": 150,
        "wind_speed_mph": 10.5,
        "wind_direction_deg": 270.0,
        "wind_gust_mph": 15.0,
    }

    # Record 2: Open-Meteo data (atmospheric fields)
    openmeteo_record = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "temperature": 68.5,
        "weather": 2,  # Partly cloudy
        "pressure_inhg": 29.92,
        # Note: Open-Meteo might also provide wind data, but we want NOAA's wind
        "wind_speed_mph": 11.0,  # Slightly different from NOAA
    }

    # Record 3: GFS Atmospheric data (more atmospheric fields, no wind since we disabled it)
    gfs_atmospheric_record = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "temperature": 69.0,  # Slightly different from Open-Meteo
        "weather": 2,
        "pressure_inhg": 29.91,  # Slightly different
    }

    # Record 4: Tide data
    tide_record = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "tide_level_ft": 3.5,
        "water_temp_f": 62.0,
    }

    # Combine all records (simulating what would happen in real pipeline)
    all_records = [noaa_record, openmeteo_record, gfs_atmospheric_record, tide_record]

    logger.info("INPUT RECORDS:")
    logger.info(f"  Total records: {len(all_records)}")
    logger.info("")

    for i, rec in enumerate(all_records, 1):
        logger.info(f"  Record {i}:")
        logger.info(f"    Source: {['NOAA', 'Open-Meteo', 'GFS Atmo', 'Tides'][i-1]}")
        logger.info(f"    Fields: {list(rec.keys())}")
        logger.info(f"    Field count: {len(rec)}")
        logger.info("")

    # Test deduplication
    logger.info("=" * 80)
    logger.info("RUNNING DEDUPLICATION")
    logger.info("=" * 80)
    logger.info("")

    deduplicated = _deduplicate_records(all_records, ("beach_id", "timestamp"))

    logger.info(f"OUTPUT: {len(deduplicated)} record(s)")
    logger.info("")

    if len(deduplicated) != 1:
        logger.error(f"ERROR: Expected 1 deduplicated record, got {len(deduplicated)}")
        return False

    merged_record = deduplicated[0]

    logger.info("MERGED RECORD:")
    logger.info(f"  Total fields: {len(merged_record)}")
    logger.info("")

    # Check what we expect to have
    expected_fields = {
        # Core identifiers
        "beach_id": beach_id,
        "timestamp": timestamp,

        # NOAA GFSwave fields
        "primary_swell_height_ft": 5.5,
        "primary_swell_period_s": 12.0,
        "primary_swell_direction": 285.0,
        "secondary_swell_height_ft": 2.0,
        "secondary_swell_period_s": 8.0,
        "secondary_swell_direction": 210.0,
        "surf_height_min_ft": 4.0,
        "surf_height_max_ft": 7.0,
        "wave_energy_kj": 150,

        # Wind from last source (Open-Meteo overwrote NOAA)
        # This is OK because they're similar values
        "wind_speed_mph": 11.0,  # From Open-Meteo (last one wins)
        "wind_direction_deg": 270.0,  # From NOAA (only source)
        "wind_gust_mph": 15.0,  # From NOAA (only source)

        # Atmospheric from last source (GFS Atmospheric overwrote Open-Meteo)
        "temperature": 69.0,  # From GFS Atmospheric (last one wins)
        "weather": 2,
        "pressure_inhg": 29.91,  # From GFS Atmospheric (last one wins)

        # Tide data
        "tide_level_ft": 3.5,
        "water_temp_f": 62.0,
    }

    # Verify all expected fields are present
    missing_fields = []
    incorrect_values = []

    for field, expected_value in expected_fields.items():
        actual_value = merged_record.get(field)

        if actual_value is None:
            missing_fields.append(field)
        elif actual_value != expected_value:
            incorrect_values.append(f"{field}: expected {expected_value}, got {actual_value}")

    # Report results
    logger.info("VALIDATION RESULTS:")
    logger.info("=" * 80)

    if not missing_fields and not incorrect_values:
        logger.info("SUCCESS: All fields preserved correctly!")
        logger.info("")
        logger.info("Field breakdown:")

        field_sources = {
            "NOAA GFSwave (9 fields)": [
                "primary_swell_height_ft", "primary_swell_period_s", "primary_swell_direction",
                "secondary_swell_height_ft", "secondary_swell_period_s", "secondary_swell_direction",
                "surf_height_min_ft", "surf_height_max_ft", "wave_energy_kj"
            ],
            "Wind Data (3 fields)": [
                "wind_speed_mph", "wind_direction_deg", "wind_gust_mph"
            ],
            "Atmospheric (3 fields)": [
                "temperature", "weather", "pressure_inhg"
            ],
            "Tide Data (2 fields)": [
                "tide_level_ft", "water_temp_f"
            ],
        }

        for source, fields in field_sources.items():
            present = sum(1 for f in fields if f in merged_record and merged_record[f] is not None)
            logger.info(f"  {source}: {present}/{len(fields)} present")

        logger.info("")
        logger.info("MERGED RECORD CONTENTS:")
        for key, value in sorted(merged_record.items()):
            if key not in ["beach_id", "timestamp"]:
                logger.info(f"  {key}: {value}")

        return True

    else:
        logger.error("FAILURE: Data loss or corruption detected!")
        logger.info("")

        if missing_fields:
            logger.error(f"  Missing fields ({len(missing_fields)}):")
            for field in missing_fields:
                logger.error(f"    - {field}")

        if incorrect_values:
            logger.error(f"  Incorrect values ({len(incorrect_values)}):")
            for msg in incorrect_values:
                logger.error(f"    - {msg}")

        return False


def test_deduplication_order_matters():
    """Test that later records override earlier ones (last one wins)."""

    logger.info("")
    logger.info("=" * 80)
    logger.info("TESTING OVERRIDE BEHAVIOR (Last One Wins)")
    logger.info("=" * 80)
    logger.info("")

    beach_id = "test-beach-456"
    timestamp = "2025-10-29T15:00:00-07:00"

    # First record: temperature = 65
    record1 = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "temperature": 65.0,
        "wind_speed_mph": 10.0,
    }

    # Second record: temperature = 70 (should override)
    record2 = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "temperature": 70.0,
        "pressure_inhg": 29.92,
    }

    # Third record: temperature = 72 (should be final value)
    record3 = {
        "beach_id": beach_id,
        "timestamp": timestamp,
        "temperature": 72.0,
    }

    records = [record1, record2, record3]
    deduplicated = _deduplicate_records(records, ("beach_id", "timestamp"))

    if len(deduplicated) != 1:
        logger.error(f"ERROR: Expected 1 record, got {len(deduplicated)}")
        return False

    merged = deduplicated[0]

    # Check final values
    expected = {
        "temperature": 72.0,  # From record3 (last)
        "wind_speed_mph": 10.0,  # From record1 (only source)
        "pressure_inhg": 29.92,  # From record2 (only source)
    }

    success = True
    for field, expected_val in expected.items():
        actual_val = merged.get(field)
        if actual_val == expected_val:
            logger.info(f"  {field}: {actual_val} (CORRECT)")
        else:
            logger.error(f"  {field}: expected {expected_val}, got {actual_val} (WRONG)")
            success = False

    if success:
        logger.info("")
        logger.info("SUCCESS: Override behavior works correctly (last record wins)")

    return success


if __name__ == "__main__":
    try:
        test1 = test_deduplication_preserves_all_values()
        test2 = test_deduplication_order_matters()

        logger.info("")
        logger.info("=" * 80)
        logger.info("OVERALL TEST RESULTS")
        logger.info("=" * 80)
        logger.info(f"  Test 1 (Data Preservation): {'PASS' if test1 else 'FAIL'}")
        logger.info(f"  Test 2 (Override Behavior): {'PASS' if test2 else 'FAIL'}")
        logger.info("")

        if test1 and test2:
            logger.info("ALL TESTS PASSED")
            sys.exit(0)
        else:
            logger.error("SOME TESTS FAILED")
            sys.exit(1)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
