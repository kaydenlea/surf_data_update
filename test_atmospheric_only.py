#!/usr/bin/env python3
"""
Test script to run ONLY the GFS Atmospheric data handler
Useful for testing rate limits without running the full pipeline
"""

import sys
import time
from datetime import datetime, timezone
import pytz

# Import configuration and handlers
from config import logger
from database import fetch_all_beaches, check_database_connection
from gfs_atmospheric_handler import (
    get_gfs_atmospheric_supplement_data,
    test_gfs_atmospheric_connection
)


def main():
    """Run only the GFS Atmospheric data collection."""
    start_time = time.time()

    logger.info("=" * 80)
    logger.info("GFS ATMOSPHERIC DATA TEST - STANDALONE")
    logger.info("=" * 80)

    # Step 1: Skip connection test to avoid duplicate dataset loading
    # (The actual data fetch will test the connection anyway)
    logger.info("\n[1/3] Skipping connection test (will test during data fetch)...")
    logger.info("✓ Connection test skipped")

    # Step 2: Test database connection
    logger.info("\n[2/3] Testing database connection...")
    if not check_database_connection():
        logger.error("CRITICAL: Database connection failed. Aborting.")
        return False
    logger.info("✓ Database connected")

    # Step 3: Fetch beaches
    logger.info("\n[3/3] Fetching beach data...")
    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("CRITICAL: No beaches found. Cannot proceed.")
        return False
    logger.info(f"✓ Loaded {len(beaches)} beaches")

    # Step 4: Create mock forecast records (with timestamps but no atmospheric data)
    logger.info("\nCreating mock forecast records for testing...")
    pacific = pytz.timezone('America/Los_Angeles')
    now_pacific = datetime.now(pacific)

    # Test with ALL beaches (change to beaches[:5] to test with just 5)
    test_beaches = beaches  # Testing with ALL beaches
    existing_records = []

    for beach in test_beaches:
        # Create records for next 24 hours (8 records at 3-hour intervals)
        for hour_offset in range(0, 24, 3):
            ts = now_pacific.replace(hour=hour_offset, minute=0, second=0, microsecond=0)
            record = {
                "beach_id": beach["id"],
                "timestamp": ts.isoformat(),
                # These fields will be filled by atmospheric handler:
                "temperature": None,
                "weather": None,
                "wind_speed_mph": None,
                "wind_direction_deg": None,
                "wind_gust_mph": None,
                "pressure_inhg": None,
            }
            existing_records.append(record)

    logger.info(f"✓ Created {len(existing_records)} mock records for {len(test_beaches)} beaches")

    # Step 5: Run atmospheric data enhancement
    logger.info("\n" + "=" * 80)
    logger.info("RUNNING GFS ATMOSPHERIC DATA ENHANCEMENT")
    logger.info("=" * 80)
    logger.info(f"Processing {len(test_beaches)} beaches with {len(existing_records)} records...")
    logger.info("Watch for rate limiting messages - delays should be working correctly\n")

    enhanced_records = get_gfs_atmospheric_supplement_data(test_beaches, existing_records)

    # Step 6: Show results
    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    filled_fields = 0
    for rec in enhanced_records:
        if rec.get("temperature") is not None:
            filled_fields += 1
        if rec.get("weather") is not None:
            filled_fields += 1
        if rec.get("wind_speed_mph") is not None:
            filled_fields += 1
        if rec.get("pressure_inhg") is not None:
            filled_fields += 1

    logger.info(f"Total records: {len(enhanced_records)}")
    logger.info(f"Fields filled: {filled_fields}")
    logger.info(f"Success rate: {filled_fields / (len(enhanced_records) * 4) * 100:.1f}%")

    # Show sample record
    if enhanced_records:
        sample = enhanced_records[0]
        logger.info("\nSample enhanced record:")
        logger.info(f"  Beach ID: {sample.get('beach_id')}")
        logger.info(f"  Timestamp: {sample.get('timestamp')}")
        logger.info(f"  Temperature: {sample.get('temperature')}°F")
        logger.info(f"  Weather Code: {sample.get('weather')}")
        logger.info(f"  Wind Speed: {sample.get('wind_speed_mph')} mph")
        logger.info(f"  Wind Direction: {sample.get('wind_direction_deg')}°")
        logger.info(f"  Wind Gust: {sample.get('wind_gust_mph')} mph")
        logger.info(f"  Pressure: {sample.get('pressure_inhg')} inHg")

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 80)
    logger.info(f"COMPLETED in {elapsed:.1f} seconds ({elapsed/60:.2f} minutes)")
    logger.info("=" * 80)

    return True


if __name__ == "__main__":
    try:
        success = main()
        exit_code = 0 if success else 1

        if success:
            logger.info("\n✓ Test completed successfully")
        else:
            logger.info("\n✗ Test completed with errors")

        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.info("\n\nTest interrupted by user (Ctrl+C)")
        sys.exit(1)

    except Exception as e:
        logger.error(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
