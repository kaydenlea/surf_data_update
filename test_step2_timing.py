#!/usr/bin/env python3
"""
TEST VERSION OF STEP 2: Data Enhancement Script
This version runs the NWS data retrieval WITHOUT writing to the database.
Use this to test performance and timing.

Usage:
    python test_step2_timing.py
"""

import sys
import time
from datetime import datetime, timezone
import pytz

# Import our custom modules
from config import logger, DAYS_FORECAST
from utils import log_step
from database import (
    fetch_all_beaches, fetch_all_counties,
    check_database_connection, get_database_stats
)
from nws_handler import get_nws_supplement_data, test_nws_connection
from noaa_tides_handler import get_noaa_tides_supplement_data, test_noaa_tides_connection
from usno_handler import update_daily_conditions_usno, test_usno_connection


def fetch_existing_forecast_data():
    """
    Fetch existing forecast data from database to enhance.
    Returns list of forecast records with at least beach_id and timestamp.
    """
    from database import supabase

    logger.info("   Fetching existing forecast data from database...")

    try:
        # Fetch all forecast data from today onwards
        from datetime import date
        today_str = date.today().isoformat()

        result = supabase.table("forecast_data") \
            .select("*") \
            .gte("timestamp", today_str) \
            .order("beach_id", desc=False) \
            .order("timestamp", desc=False) \
            .execute()

        if not result or not result.data:
            logger.warning("   No existing forecast data found in database!")
            logger.warning("   Please run step1_gfs_fetch.py first to populate GFS data.")
            return []

        # Convert to list of dictionaries (result.data is already a list of dicts)
        records = []
        for row in result.data:
            record = {
                "beach_id": row.get("beach_id"),
                "timestamp": row.get("timestamp"),
            }

            # Include existing GFS data fields if present
            for field in [
                "primary_swell_height_ft", "primary_swell_period_s", "primary_swell_direction",
                "secondary_swell_height_ft", "secondary_swell_period_s", "secondary_swell_direction",
                "tertiary_swell_height_ft", "tertiary_swell_period_s", "tertiary_swell_direction",
                "surf_height_min_ft", "surf_height_max_ft", "wave_energy_kj",
                "wind_direction_deg", "wind_gust_mph"
            ]:
                value = row.get(field)
                if value is not None:
                    record[field] = value

            records.append(record)

        logger.info(f"   Loaded {len(records)} existing forecast records from database")
        return records

    except Exception as e:
        logger.error(f"   Error fetching existing forecast data: {e}")
        return []


def test_enhance_forecast_data(beaches):
    """
    TEST VERSION: Enhance existing forecast data with NWS and CO-OPS data.
    DOES NOT WRITE TO DATABASE - only tests timing.
    """
    log_step("TEST MODE: Enhancing forecast data (NO DATABASE WRITES)", 1)

    try:
        # Fetch existing forecast data from database
        existing_records = fetch_existing_forecast_data()

        if not existing_records:
            logger.error("   No existing forecast data to enhance!")
            logger.error("   Run step1_gfs_fetch.py first to populate GFS wave data.")
            return 0

        # --- NOAA NWS SUPPLEMENT (Weather, Temp, Wind) ---
        logger.info("   Enhancing with NOAA NWS data (weather/temp/wind)...")
        nws_start = time.time()
        nws_enhanced = get_nws_supplement_data(beaches, existing_records)
        nws_time = time.time() - nws_start
        logger.info(f"   >>> NWS enhancement took: {nws_time:.2f} seconds ({nws_time/60:.2f} minutes)")

        # --- NOAA CO-OPS SUPPLEMENT (Tides, Water Temp) ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides/water temp)...")
        tides_start = time.time()
        fully_enhanced = get_noaa_tides_supplement_data(beaches, nws_enhanced)
        tides_time = time.time() - tides_start
        logger.info(f"   >>> CO-OPS enhancement took: {tides_time:.2f} seconds ({tides_time/60:.2f} minutes)")

        # --- SKIP DATABASE WRITE IN TEST MODE ---
        logger.info("   [TEST MODE] Skipping database write")
        logger.info(f"   [TEST MODE] Would have written {len(fully_enhanced)} records")

        log_step(
            f"TEST: Forecast enhancement completed: {len(beaches)} beaches, "
            f"{len(fully_enhanced)} records enhanced (NOT written to DB)"
        )
        return len(fully_enhanced)

    except Exception as e:
        logger.error(f"ERROR: Forecast enhancement failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0


def run_system_checks():
    """Run pre-flight system checks for all enhancement services."""
    logger.info("Running system checks...")

    checks_passed = 0
    total_checks = 4

    if check_database_connection():
        logger.info("[OK] Database connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] Database connection failed")

    if test_nws_connection():
        logger.info("[OK] NOAA NWS API connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] NOAA NWS API connection failed")

    if test_noaa_tides_connection():
        logger.info("[OK] NOAA CO-OPS API connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] NOAA CO-OPS API connection failed")

    if test_usno_connection():
        logger.info("[OK] USNO API connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] USNO API connection failed")

    logger.info(f"System checks: {checks_passed}/{total_checks} passed")
    return checks_passed >= 3  # Need at least DB + 2 APIs


def print_startup_banner():
    """Print startup banner with configuration info."""
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("TEST MODE: STEP 2 DATA ENHANCEMENT - TIMING TEST (NO DB WRITES)")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("ENHANCEMENT SOURCES:")
    logger.info("  - NOAA NWS - Weather, temperature, wind, pressure")
    logger.info("  - NOAA CO-OPS - Tides, water temperature")
    logger.info("")
    logger.info("NOTE: This test will NOT write data to the database")
    logger.info("=" * 80)


def print_completion_summary(start_time, beaches_count, forecast_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("TEST COMPLETED!" if success else "TEST COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"   - Beaches processed: {beaches_count:,}")
    logger.info(f"   - Forecast records enhanced: {forecast_records:,}")

    if total_time > 0 and forecast_records > 0:
        records_per_second = forecast_records / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("")
    logger.info("NOTE: No data was written to the database (test mode)")
    logger.info("=" * 80)


def main():
    """Main execution function for test."""
    start_time = time.time()
    print_startup_banner()

    try:
        # Step 0: System checks
        if not run_system_checks():
            logger.error("CRITICAL: System checks failed. Aborting.")
            return False

        # Step 1: Fetch beaches
        log_step("Fetching location data", 1)
        beaches = fetch_all_beaches()
        if not beaches:
            logger.error("CRITICAL: No beaches found. Cannot proceed.")
            return False

        logger.info(f"Loaded {len(beaches)} beaches")

        # Step 2: Test enhance forecast data (NO DB WRITES)
        log_step("Testing forecast data enhancement", 2)
        forecast_count = test_enhance_forecast_data(beaches)
        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was enhanced successfully")
            logger.error("Make sure step1_gfs_fetch.py was run first!")
            return False

        print_completion_summary(start_time, len(beaches), forecast_count, True)
        return True

    except KeyboardInterrupt:
        logger.info("Script interrupted by user (Ctrl+C)")
        return False

    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error(f"Full error details: {str(e)}")

        try:
            print_completion_summary(
                start_time,
                len(beaches) if 'beaches' in locals() else 0,
                forecast_count if 'forecast_count' in locals() else 0,
                False
            )
        except Exception:
            pass

        return False


if __name__ == "__main__":
    try:
        success = main()
        exit_code = 0 if success else 1

        if success:
            logger.info("[OK] Test completed successfully")
        else:
            logger.info("[FAIL] Test completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
