#!/usr/bin/env python3
"""
STEP 2: Data Enhancement Script
Enhances existing GFS forecast data with additional sources:
  - NOAA NWS (weather, temperature, wind, pressure)
  - NOAA CO-OPS (tides, water temperature)
  - USNO (sun/moon astronomical data)

Run this script AFTER step1_gfs_fetch.py has populated base GFS data.

Usage:
    python step2_enhance_data.py
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
    upsert_forecast_data, upsert_daily_conditions,
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


def enhance_forecast_data(beaches):
    """
    Enhance existing forecast data with NWS and CO-OPS data.
    """
    log_step("Enhancing forecast data with supplemental sources", 1)

    try:
        # Fetch existing forecast data from database
        existing_records = fetch_existing_forecast_data()

        if not existing_records:
            logger.error("   No existing forecast data to enhance!")
            logger.error("   Run step1_gfs_fetch.py first to populate GFS wave data.")
            return 0

        # --- NOAA NWS SUPPLEMENT (Weather, Temp, Wind) ---
        logger.info("   Enhancing with NOAA NWS data (weather/temp/wind)…")
        nws_enhanced = get_nws_supplement_data(beaches, existing_records)

        # --- NOAA CO-OPS SUPPLEMENT (Tides, Water Temp) ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides/water temp)…")
        fully_enhanced = get_noaa_tides_supplement_data(beaches, nws_enhanced)

        # --- UPSERT TO DATABASE ---
        logger.info("   Uploading enhanced records to database…")
        total_inserted = upsert_forecast_data(fully_enhanced)

        log_step(
            f"Forecast enhancement completed: {len(beaches)} beaches, "
            f"{total_inserted} records updated"
        )
        return total_inserted

    except Exception as e:
        logger.error(f"ERROR: Forecast enhancement failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0


def run_system_checks():
    """Run pre-flight system checks for all enhancement services."""
    logger.info("Running system checks…")

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
    logger.info("STEP 2: DATA ENHANCEMENT - NWS/CO-OPS/USNO")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("ENHANCEMENT SOURCES:")
    logger.info("  - NOAA NWS - Weather, temperature, wind, pressure")
    logger.info("  - NOAA CO-OPS - Tides, water temperature")
    logger.info("  - USNO - Sun/moon rise/set, moon phase")
    logger.info("")
    logger.info("PREREQUISITE: step1_gfs_fetch.py must have been run first")
    logger.info("=" * 80)


def print_completion_summary(start_time, beaches_count, counties_count, forecast_records, daily_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("STEP 2 COMPLETED!" if success else "STEP 2 COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds")
    logger.info(f"   - Beaches processed: {beaches_count:,}")
    logger.info(f"   - Counties processed: {counties_count:,}")
    logger.info(f"   - Forecast records enhanced: {forecast_records:,}")
    logger.info(f"   - Daily condition records: {daily_records:,}")

    if total_time > 0:
        records_per_second = (forecast_records + daily_records) / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("=" * 80)
    logger.info("ENHANCEMENT DATA ADDED:")
    logger.info("   - Weather conditions and descriptions")
    logger.info("   - Air temperature")
    logger.info("   - Wind speed (supplemented)")
    logger.info("   - Atmospheric pressure")
    logger.info("   - Tide levels")
    logger.info("   - Water temperature")
    logger.info("   - Sunrise/sunset times")
    logger.info("   - Moonrise/moonset times")
    logger.info("   - Moon phase")
    logger.info("=" * 80)
    logger.info("ALL DATA SOURCES (100% Public Domain):")
    logger.info("   - NOAA GFSwave - Wave/swell/wind (from Step 1)")
    logger.info("   - NOAA NWS - Weather/temperature/wind/pressure")
    logger.info("   - NOAA CO-OPS - Tides/water temperature")
    logger.info("   - USNO - Sun/moon astronomical data")
    logger.info("=" * 80)


def main():
    """Main execution function for Step 2."""
    start_time = time.time()
    print_startup_banner()

    try:
        # Step 0: System checks
        if not run_system_checks():
            logger.error("CRITICAL: System checks failed. Aborting.")
            return False

        # Step 1: Fetch beaches and counties
        log_step("Fetching location data", 1)
        beaches = fetch_all_beaches()
        if not beaches:
            logger.error("CRITICAL: No beaches found. Cannot proceed.")
            return False

        counties = fetch_all_counties()
        if not counties:
            logger.error("CRITICAL: No counties found. Cannot proceed.")
            return False

        logger.info(f"Loaded {len(beaches)} beaches and {len(counties)} counties")

        # Step 2: Enhance forecast data with NWS and CO-OPS
        log_step("Enhancing forecast data", 2)
        forecast_count = enhance_forecast_data(beaches)
        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was enhanced successfully")
            logger.error("Make sure step1_gfs_fetch.py was run first!")
            return False

        # Step 3: Daily conditions (USNO)
        log_step("Processing daily conditions (USNO)", 3)
        daily_records = update_daily_conditions_usno(counties)
        daily_count = upsert_daily_conditions(daily_records)

        # Step 4: Final DB statistics
        log_step("Generating final statistics", 4)
        _ = get_database_stats()

        print_completion_summary(
            start_time, len(beaches), len(counties),
            forecast_count, daily_count, True
        )
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
                len(counties) if 'counties' in locals() else 0,
                forecast_count if 'forecast_count' in locals() else 0,
                daily_count if 'daily_count' in locals() else 0,
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
            logger.info("[OK] Step 2 completed successfully")
        else:
            logger.info("[FAIL] Step 2 completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
