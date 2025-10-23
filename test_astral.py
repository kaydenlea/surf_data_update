#!/usr/bin/env python3
"""
Test script to verify Astral astronomical calculations and daily conditions table updates.
This script only updates the daily_county_conditions table.
"""

import sys
import time
from datetime import datetime
import pytz

from config import logger
from database import fetch_all_counties, upsert_daily_conditions, get_database_stats, check_database_connection
from astral_handler import update_daily_conditions_astral, test_astral_calculation


def main():
    """Test Astral calculations and update daily conditions table."""

    print("=" * 80)
    print("ASTRAL ASTRONOMICAL CALCULATIONS TEST")
    print("=" * 80)

    pacific = pytz.timezone("America/Los_Angeles")
    now_pst = datetime.now(pacific)
    print(f"Test time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()

    # Step 1: Check database connection
    print("STEP 1: Testing database connection...")
    if not check_database_connection():
        print("❌ Database connection failed!")
        return False
    print("✅ Database connection successful")
    print()

    # Step 2: Test Astral calculations
    print("STEP 2: Testing Astral astronomical calculations...")
    if not test_astral_calculation():
        print("❌ Astral calculation test failed!")
        return False
    print("✅ Astral calculations working correctly")
    print()

    # Step 3: Fetch counties
    print("STEP 3: Fetching county data...")
    counties = fetch_all_counties()
    print(f"✅ Found {len(counties)} counties")
    print()

    # Step 4: Calculate daily conditions
    print("STEP 4: Calculating daily conditions using Astral...")
    print(f"   Calculating sunrise, sunset, and moon phase for 7 days ahead...")
    calc_start = time.time()
    daily_records = update_daily_conditions_astral(counties)
    calc_time = time.time() - calc_start
    print(f"   ✅ Calculated {len(daily_records)} daily records")
    print(f"   ⏱️  Calculation took: {calc_time:.2f} seconds")
    print()

    # Show sample of calculated data
    if daily_records:
        print("   Sample of calculated data:")
        for i, record in enumerate(daily_records[:3]):
            print(f"      {i+1}. {record['county']} on {record['date']}:")
            print(f"         Sunrise: {record['sunrise']}, Sunset: {record['sunset']}, Moon Phase: {record['moon_phase']:.3f}")
        print()

    # Step 5: Upload to database
    print("STEP 5: Uploading to daily_county_conditions table...")
    upload_start = time.time()
    daily_count = upsert_daily_conditions(daily_records)
    upload_time = time.time() - upload_start
    print(f"   ✅ Successfully upserted {daily_count} records")
    print(f"   ⏱️  Upload took: {upload_time:.2f} seconds")
    print()

    # Step 6: Verify database
    print("STEP 6: Verifying database records...")
    stats = get_database_stats()
    print(f"   Total records in daily_county_conditions: {stats.get('daily_county_conditions', 0):,}")
    print()

    # Summary
    total_time = calc_time + upload_time
    print("=" * 80)
    print("TEST COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    print(f"   Total execution time: {total_time:.2f} seconds")
    print(f"   Records calculated: {len(daily_records)}")
    print(f"   Records upserted: {daily_count}")
    print(f"   Calculation speed: {len(daily_records) / calc_time:.1f} records/second")
    print()
    print("✅ Astral library is working correctly!")
    print("✅ No external API calls required!")
    print("✅ Daily conditions table updated successfully!")
    print("=" * 80)

    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception("Test failed with error:")
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
