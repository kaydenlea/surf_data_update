#!/usr/bin/env python3
"""
Debug script to identify what's causing the 500 error in forecast data upsert
"""

import sys
from database import fetch_all_beaches
from noaa_handler import get_noaa_dataset_url, load_noaa_dataset, get_noaa_data_bulk_optimized
from config import logger
import json

def debug_upsert_data():
    """Check the data structure being sent to database."""

    logger.info("=" * 80)
    logger.info("DEBUGGING FORECAST DATA UPSERT ERROR")
    logger.info("=" * 80)

    # Get sample data
    logger.info("Fetching beaches...")
    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("No beaches found")
        return False

    # Take just 2 beaches for testing
    test_beaches = beaches[:2]
    logger.info(f"Using {len(test_beaches)} test beaches")

    # Get NOAA data
    logger.info("Loading NOAA dataset...")
    noaa_url = get_noaa_dataset_url()
    if not noaa_url:
        logger.error("Could not get NOAA URL")
        return False

    ds = load_noaa_dataset(noaa_url)
    if not ds:
        logger.error("Could not load NOAA dataset")
        return False

    logger.info("Extracting NOAA data for test beaches...")
    records = get_noaa_data_bulk_optimized(ds, test_beaches)
    ds.close()

    if not records:
        logger.error("No records generated")
        return False

    logger.info(f"Generated {len(records)} records")
    logger.info("")

    # Analyze first few records
    logger.info("=" * 80)
    logger.info("SAMPLE RECORD ANALYSIS")
    logger.info("=" * 80)

    for i, rec in enumerate(records[:3]):
        logger.info(f"\nRecord {i+1}:")
        logger.info(f"  Keys: {list(rec.keys())}")
        logger.info(f"  beach_id: {rec.get('beach_id')} (type: {type(rec.get('beach_id'))})")
        logger.info(f"  timestamp: {rec.get('timestamp')} (type: {type(rec.get('timestamp'))})")

        # Check for problematic values
        problems = []
        for key, value in rec.items():
            if value is None:
                continue

            # Check for NaN or Inf
            if isinstance(value, float):
                import math
                if math.isnan(value):
                    problems.append(f"{key}=NaN")
                elif math.isinf(value):
                    problems.append(f"{key}=Inf")

            # Check for very large numbers
            if isinstance(value, (int, float)) and abs(value) > 1e10:
                problems.append(f"{key}={value} (too large)")

        if problems:
            logger.warning(f"  PROBLEMS: {', '.join(problems)}")
        else:
            logger.info(f"  No obvious problems detected")

    # Check field types
    logger.info("")
    logger.info("=" * 80)
    logger.info("FIELD TYPE ANALYSIS")
    logger.info("=" * 80)

    field_types = {}
    for rec in records[:10]:
        for key, value in rec.items():
            if value is not None:
                vtype = type(value).__name__
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(vtype)

    for field, types in sorted(field_types.items()):
        types_str = ", ".join(sorted(types))
        logger.info(f"  {field}: {types_str}")

    # Try to serialize to JSON (database format)
    logger.info("")
    logger.info("=" * 80)
    logger.info("JSON SERIALIZATION TEST")
    logger.info("=" * 80)

    try:
        sample_rec = records[0]
        json_str = json.dumps(sample_rec, indent=2)
        logger.info("✓ First record serializes to JSON successfully")
        logger.info(f"Sample JSON:\n{json_str[:500]}...")
    except Exception as e:
        logger.error(f"✗ JSON serialization failed: {e}")
        return False

    # Check for required fields
    logger.info("")
    logger.info("=" * 80)
    logger.info("REQUIRED FIELDS CHECK")
    logger.info("=" * 80)

    required_fields = ["beach_id", "timestamp"]
    missing_required = 0
    for rec in records:
        for field in required_fields:
            if rec.get(field) is None:
                missing_required += 1
                logger.warning(f"  Missing {field} in record")
                break

    if missing_required == 0:
        logger.info(f"✓ All {len(records)} records have required fields")
    else:
        logger.error(f"✗ {missing_required} records missing required fields")

    return True

if __name__ == "__main__":
    try:
        success = debug_upsert_data()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
