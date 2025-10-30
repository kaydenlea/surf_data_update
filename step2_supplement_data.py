#!/usr/bin/env python3
"""
Step 2: Supplemental Data Update (Weather, Tides, Astronomical)
Enhances existing wave data with weather, tides, and sun/moon information
This is the slower script - runs in ~15-20 minutes due to rate limiting

Prerequisites: Run step1_wave_data.py FIRST to populate the wave data
This script reads existing forecast records and adds supplemental fields.
"""

import sys
import time
from datetime import datetime, timezone

# Import our custom modules
from config import (
    logger, NOAA_ATMOSPHERIC_REQUEST_DELAY, NOAA_ATMOSPHERIC_BATCH_DELAY,
    DAYS_FORECAST
)
from utils import log_step
from database import (
    fetch_all_beaches, fetch_all_counties,
    upsert_forecast_data, upsert_daily_conditions,
    check_database_connection, get_database_stats,
    fetch_existing_forecast_records
)
from gfs_atmospheric_handler_v2 import get_gfs_atmospheric_supplement_data, test_gfs_atmospheric_connection
from noaa_tides_handler import get_noaa_tides_supplement_data, test_noaa_tides_connection
from usno_handler import update_daily_conditions_usno, test_usno_connection


# --------------------------------------------------------------------------------------
# SUPPLEMENT DATA UPDATE
# --------------------------------------------------------------------------------------

def update_supplement_data(beaches, counties):
    """
    Enhance existing forecast data with supplemental information:
      1) NOAA GFS Atmospheric (temperature, weather, wind, pressure)
      2) NOAA CO-OPS (tides, water temperature)
      3) USNO (sun/moon astronomical data)
    """
    log_step("Step 2: Updating SUPPLEMENTAL DATA", 1)
    step_start = time.time()

    try:
        # --- FETCH EXISTING FORECAST RECORDS ---
        logger.info("   Fetching existing forecast records from database…")
        fetch_start = time.time()
        existing_records = fetch_existing_forecast_records()
        fetch_time = time.time() - fetch_start

        if not existing_records:
            logger.error("   ERROR: No existing forecast records found in database!")
            logger.error("   You must run step1_wave_data.py FIRST to populate wave data.")
            return 0, 0

        logger.info(f"   Found {len(existing_records)} existing forecast records")
        logger.info(f"   >>> Record fetch took: {fetch_time:.2f} seconds ({fetch_time/60:.2f} minutes)")

        # --- NOAA GFS ATMOSPHERIC SUPPLEMENT (Weather, Temp, Wind, Pressure) ---
        logger.info("   Enhancing with NOAA GFS Atmospheric data (weather/temp/wind/pressure)...")
        gfs_start = time.time()
        gfs_enhanced = get_gfs_atmospheric_supplement_data(beaches, existing_records)
        gfs_time = time.time() - gfs_start
        logger.info(f"   >>> GFS Atmospheric enhancement took: {gfs_time:.2f} seconds ({gfs_time/60:.2f} minutes)")

        # --- NOAA CO-OPS SUPPLEMENT (Tides, Water Temp) ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides/water temp)…")
        tides_start = time.time()
        fully_enhanced = get_noaa_tides_supplement_data(beaches, gfs_enhanced)
        tides_time = time.time() - tides_start
        logger.info(f"   >>> CO-OPS enhancement took: {tides_time:.2f} seconds ({tides_time/60:.2f} minutes)")

        # --- UPSERT ENHANCED FORECAST DATA TO DATABASE ---
        logger.info("   Uploading enhanced forecast records to database…")
        db_start = time.time()
        forecast_inserted = upsert_forecast_data(fully_enhanced)
        db_time = time.time() - db_start
        logger.info(f"   >>> Forecast database upsert took: {db_time:.2f} seconds ({db_time/60:.2f} minutes)")

        # --- USNO DAILY CONDITIONS (Sun/Moon) ---
        logger.info("   Processing USNO daily conditions (sun/moon)…")
        usno_start = time.time()
        daily_records = update_daily_conditions_usno(counties)
        daily_count = upsert_daily_conditions(daily_records)
        usno_time = time.time() - usno_start
        logger.info(f"   >>> USNO processing took: {usno_time:.2f} seconds ({usno_time/60:.2f} minutes)")

        step_time = time.time() - step_start
        logger.info(f"   >>> TOTAL supplement update time: {step_time:.2f} seconds ({step_time/60:.2f} minutes)")

        log_step(
            f"Supplement update completed: {len(beaches)} beaches, "
            f"{forecast_inserted} forecast records, {daily_count} daily records"
        )
        return forecast_inserted, daily_count

    except Exception as e:
        logger.error(f"ERROR: Supplement update failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0, 0


# --------------------------------------------------------------------------------------
# SYSTEM CHECKS
# --------------------------------------------------------------------------------------

def run_system_checks():
    """Run pre-flight system checks for supplemental data sources."""
    logger.info("Running system checks…")

    checks_passed = 0
    total_checks = 4

    if check_database_connection():
        logger.info("[OK] Database connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] Database connection failed")

    if test_gfs_atmospheric_connection():
        logger.info("[OK] GFS Atmospheric dataset connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] GFS Atmospheric dataset connection failed")

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
    return checks_passed >= 3  # Need at least DB + 2 supplemental APIs


# --------------------------------------------------------------------------------------
# STARTUP BANNER
# --------------------------------------------------------------------------------------

def print_startup_banner():
    """Print startup banner with configuration info."""
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("STEP 2: SUPPLEMENTAL DATA UPDATE (ATMOSPHERIC/TIDES/ASTRONOMICAL)")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("DATA SOURCES (All Public Domain - Free for Commercial Use):")
    logger.info("  - NOAA GFS Atmospheric - Weather, temperature, wind, pressure")
    logger.info("  - NOAA CO-OPS - Tides, water temperature")
    logger.info("  - USNO - Sun/moon rise/set, moon phase")
    logger.info("")
    logger.info("FIELDS POPULATED:")
    logger.info("  - Temperature (°F)")
    logger.info("  - Weather code (WMO)")
    logger.info("  - Wind speed/direction/gust (enhanced)")
    logger.info("  - Pressure (inHg)")
    logger.info("  - Tide level (ft)")
    logger.info("  - Water temperature (°F)")
    logger.info("  - Sunrise/sunset times")
    logger.info("  - Moonrise/moonset times")
    logger.info("  - Moon phase")
    logger.info("")
    logger.info(f"Rate limiting (Atmospheric): {NOAA_ATMOSPHERIC_REQUEST_DELAY}s per request, {NOAA_ATMOSPHERIC_BATCH_DELAY}s between batches")
    logger.info("")
    logger.info("NOTE: This script enhances data created by step1_wave_data.py")
    logger.info("      Run step1_wave_data.py FIRST if you haven't already!")
    logger.info("=" * 80)


def print_completion_summary(start_time, beaches_count, counties_count, forecast_records, daily_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("STEP 2: SUPPLEMENT UPDATE COMPLETED!" if success else "STEP 2: SUPPLEMENT UPDATE COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"   - Beaches processed: {beaches_count:,}")
    logger.info(f"   - Counties processed: {counties_count:,}")
    logger.info(f"   - Forecast records enhanced: {forecast_records:,}")
    logger.info(f"   - Daily condition records: {daily_records:,}")
    logger.info(f"   - Total records: {forecast_records + daily_records:,}")

    if total_time > 0:
        records_per_second = (forecast_records + daily_records) / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("=" * 80)
    logger.info("COMPLETE DATABASE UPDATE:")
    logger.info("   ✓ Step 1: Wave data (NOAA GFSwave)")
    logger.info("   ✓ Step 2: Supplemental data (Atmospheric/Tides/Astronomical)")
    logger.info("   All forecast data is now complete!")
    logger.info("=" * 80)


# --------------------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------------------

def main():
    """Main execution function."""
    start_time = time.time()
    print_startup_banner()

    try:
        # Step 0: System checks
        if not run_system_checks():
            logger.error("CRITICAL: System checks failed. Aborting.")
            return False

        # Step 1: Fetch beaches and counties
        log_step("Fetching location data", 1)
        fetch_start = time.time()
        beaches = fetch_all_beaches()
        if not beaches:
            logger.error("CRITICAL: No beaches found. Cannot proceed.")
            return False

        counties = fetch_all_counties()
        if not counties:
            logger.error("CRITICAL: No counties found. Cannot proceed.")
            return False
        fetch_time = time.time() - fetch_start

        logger.info(f"Loaded {len(beaches)} beaches and {len(counties)} counties")
        logger.info(f"   >>> Location fetch took: {fetch_time:.2f} seconds")

        # Step 2: Supplement data update
        log_step("Processing supplemental data", 2)
        forecast_count, daily_count = update_supplement_data(beaches, counties)

        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was enhanced")
            logger.error("Make sure you ran step1_wave_data.py first!")
            return False

        # Step 3: Final DB statistics
        log_step("Generating final statistics", 3)
        stats_start = time.time()
        _ = get_database_stats()
        stats_time = time.time() - stats_start
        logger.info(f"   >>> Statistics generation took: {stats_time:.2f} seconds")

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
            logger.info("[OK] Step 2 completed successfully - supplemental data added")
        else:
            logger.info("[FAIL] Step 2 completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
