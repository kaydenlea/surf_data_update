#!/usr/bin/env python3
"""
Main execution script for 100% NOAA-based Surf Database Update
Uses only free, public domain government data sources:
  - NOAA GFSwave (primary wave/swell data)
  - NOAA GFS Atmospheric (weather, temperature, pressure)
  - NOAA CO-OPS (tides, water temperature)
  - Astral library (sun/moon astronomical calculations using NOAA algorithms)

NO commercial APIs required - all data is public domain and free for commercial use.
"""

import sys
import time
from datetime import datetime, timezone, timedelta

# Import our custom modules
from config import (
    logger, NOAA_REQUEST_DELAY, NOAA_OCEAN_BATCH_DELAY, NOAA_ATMOSPHERIC_BATCH_DELAY,
    DAYS_FORECAST, MAX_WORKERS
)
from utils import log_step
from database import (
    cleanup_old_data, fetch_all_beaches, fetch_all_counties,
    upsert_forecast_data, upsert_daily_conditions, check_database_connection,
    get_database_stats
)
from noaa_handler import (
    get_noaa_dataset_url, load_noaa_dataset, get_noaa_data_bulk_optimized,
    validate_noaa_dataset
)
from gfs_atmospheric_handler_v2 import get_gfs_atmospheric_supplement_data, test_gfs_atmospheric_connection
from noaa_tides_handler import get_noaa_tides_supplement_data, test_noaa_tides_connection
from astral_handler import update_daily_conditions_astral, test_astral_calculation

# Keep Open-Meteo as optional fallback (not deleted, just not used by default)
try:
    from openmeteo_handler import get_openmeteo_supplement_data, test_openmeteo_connection
    OPENMETEO_AVAILABLE = True
except ImportError:
    OPENMETEO_AVAILABLE = False

# --------------------------------------------------------------------------------------
# HELPER FUNCTIONS (from original main.py)
# --------------------------------------------------------------------------------------

def _ensure_today_midnight_start(records, beaches):
    """Ensure each beach has records starting from today's 12:00 AM (Pacific)."""
    if not records:
        return records

    import pytz
    from datetime import datetime, timedelta, time as dtime

    pacific = pytz.timezone('America/Los_Angeles')
    today = datetime.now(pacific).date()
    midnight = pacific.localize(datetime.combine(today, dtime(0, 0)))

    by_beach = {}
    for r in records:
        bid = r.get("beach_id")
        if bid is None:
            continue
        by_beach.setdefault(bid, []).append(r)

    beach_ids = {b["id"] for b in beaches if b.get("id") is not None}
    all_out = list(records)

    for bid in beach_ids:
        recs = by_beach.get(bid, [])
        earliest_ts = None
        for r in recs:
            ts_str = r.get("timestamp")
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                continue
            if ts.tzinfo is None:
                continue
            if ts >= midnight and (earliest_ts is None or ts < earliest_ts):
                earliest_ts = ts

        if earliest_ts is None:
            target_end = midnight
        else:
            target_end = earliest_ts

        t = midnight
        placeholders = []
        while t < target_end:
            ts_iso = t.isoformat()
            if not any(r.get("timestamp") == ts_iso for r in recs):
                # Create placeholder with only required fields
                # Don't explicitly set other fields to None - let supplement handlers fill them
                # or preserve existing DB values (via default_to_null=False on upsert)
                placeholders.append({
                    "beach_id": bid,
                    "timestamp": ts_iso,
                    # No other fields - supplement handlers will add them if data is available
                })
            t += timedelta(hours=3)

        if placeholders:
            all_out.extend(placeholders)

    return all_out


def _drop_records_before_today(records):
    """Remove any forecast rows that fall before today's Pacific midnight."""
    if not records:
        return records

    import pytz
    from datetime import time as dtime

    pacific = pytz.timezone('America/Los_Angeles')
    now_pacific = datetime.now(pacific)
    # Get midnight using localize() instead of replace() to handle DST correctly
    today_date = now_pacific.date()
    midnight_today = pacific.localize(datetime.combine(today_date, dtime(0, 0)))

    filtered = []
    removed = 0
    for rec in records:
        ts_str = rec.get("timestamp")
        if not ts_str:
            filtered.append(rec)
            continue
        try:
            cleaned = ts_str.replace('Z', '+00:00') if ts_str.endswith('Z') else ts_str
            ts = datetime.fromisoformat(cleaned)
        except Exception:
            filtered.append(rec)
            continue
        if ts.tzinfo is None:
            ts = pacific.localize(ts)
        if ts >= midnight_today:
            filtered.append(rec)
        else:
            removed += 1
    if removed:
        logger.info(f"   Dropped {removed} NOAA forecast records before today's midnight")
    return filtered


# --------------------------------------------------------------------------------------
# FORECAST UPDATE (100% NOAA STACK)
# --------------------------------------------------------------------------------------

def update_forecast_data_noaa_stack(beaches):
    """
    Update forecast data using 100% NOAA/Government sources:
      1) NOAA GFSwave (primary: swell, surf, wind speed/dir)
      2) NOAA GFS Atmospheric (supplement: temperature, weather, pressure)
      3) NOAA CO-OPS (supplement: tides, water temperature)
    """
    log_step("Updating forecast data with 100% NOAA sources", 4)
    step_start = time.time()

    try:
        # --- NOAA GFSWAVE PRIMARY ---
        logger.info("   Loading NOAA GFSwave dataset (rate-limited)…")
        gfs_start = time.time()
        noaa_url = get_noaa_dataset_url()
        ds = load_noaa_dataset(noaa_url)

        if not validate_noaa_dataset(ds):
            raise Exception("NOAA dataset validation failed")

        all_noaa_records = get_noaa_data_bulk_optimized(ds, beaches)
        ds.close()
        gfs_time = time.time() - gfs_start

        logger.info(f"   NOAA GFSwave complete: {len(all_noaa_records)} hourly records")
        logger.info(f"   >>> GFSwave processing took: {gfs_time:.2f} seconds ({gfs_time/60:.2f} minutes)")

        if all_noaa_records:
            sample_ts = all_noaa_records[0].get("timestamp")
            logger.info(f"   Sample timestamp: {sample_ts}")

        # Ensure we start from today's midnight
        all_noaa_records = _drop_records_before_today(all_noaa_records)
        all_noaa_records = _ensure_today_midnight_start(all_noaa_records, beaches)

        # --- NOAA NWS SUPPLEMENT (Weather, Temp, Wind, Pressure) ---
        logger.info("   Enhancing with GFS Atmospheric data (weather/temp/pressure)…")
        gfs_atmo_start = time.time()
        gfs_enhanced = get_gfs_atmospheric_supplement_data(beaches, all_noaa_records)
        gfs_atmo_time = time.time() - gfs_atmo_start
        logger.info(f"   >>> GFS Atmospheric enhancement took: {gfs_atmo_time:.2f} seconds ({gfs_atmo_time/60:.2f} minutes)")

        # --- NOAA CO-OPS SUPPLEMENT (Tides, Water Temp) ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides/water temp)…")
        tides_start = time.time()
        fully_enhanced = get_noaa_tides_supplement_data(beaches, gfs_enhanced)
        tides_time = time.time() - tides_start
        logger.info(f"   >>> CO-OPS enhancement took: {tides_time:.2f} seconds ({tides_time/60:.2f} minutes)")

        # --- UPSERT TO DATABASE ---
        logger.info("   Uploading enhanced records to database…")
        db_start = time.time()
        total_inserted = upsert_forecast_data(fully_enhanced)
        db_time = time.time() - db_start
        logger.info(f"   >>> Database upsert took: {db_time:.2f} seconds ({db_time/60:.2f} minutes)")

        step_time = time.time() - step_start
        logger.info(f"   >>> TOTAL forecast update time: {step_time:.2f} seconds ({step_time/60:.2f} minutes)")

        log_step(
            f"NOAA stack forecast update completed: {len(beaches)} beaches, "
            f"{total_inserted} records upserted"
        )
        return total_inserted

    except Exception as e:
        logger.error(f"ERROR: NOAA stack forecast update failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0


# --------------------------------------------------------------------------------------
# SYSTEM CHECKS
# --------------------------------------------------------------------------------------

def run_system_checks():
    """Run pre-flight system checks for all NOAA services."""
    logger.info("Running system checks…")

    checks_passed = 0
    total_checks = 4

    if check_database_connection():
        logger.info("[OK] Database connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] Database connection failed")

    if test_gfs_atmospheric_connection():
        logger.info("[OK] GFS Atmospheric connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] GFS Atmospheric connection failed")

    if test_noaa_tides_connection():
        logger.info("[OK] NOAA CO-OPS API connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] NOAA CO-OPS API connection failed")

    if test_astral_calculation():
        logger.info("[OK] Astral astronomical calculations working")
        checks_passed += 1
    else:
        logger.error("[FAIL] Astral astronomical calculations failed")

    # Optional: Check Open-Meteo if available
    if OPENMETEO_AVAILABLE:
        try:
            if test_openmeteo_connection():
                logger.info("[OK] Open-Meteo API connection successful (optional fallback)")
            else:
                logger.warning("[WARN] Open-Meteo API connection failed (optional fallback)")
        except Exception:
            pass

    logger.info(f"System checks: {checks_passed}/{total_checks} passed")
    return checks_passed >= 3  # Need at least DB + 2 data sources


def print_startup_banner():
    """Print startup banner with configuration info."""
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("SURF DATABASE UPDATE - 100% NOAA/GOVERNMENT SOURCES")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("DATA SOURCES (All Public Domain - Free for Commercial Use):")
    logger.info("  - NOAA GFSwave - Wave/swell forecasts")
    logger.info("  - NOAA GFS Atmospheric - Weather, temperature, pressure")
    logger.info("  - NOAA CO-OPS - Tides, water temperature")
    logger.info("  - Astral library - Sun/moon rise/set, moon phase (using NOAA algorithms)")
    logger.info("")
    logger.info(f"Rate limiting: {NOAA_REQUEST_DELAY}s per request")
    logger.info(f"  - Ocean data (GFSwave): {NOAA_OCEAN_BATCH_DELAY}s between batches")
    logger.info(f"  - Atmospheric data (GFS): {NOAA_ATMOSPHERIC_BATCH_DELAY}s between batches")
    logger.info("=" * 80)


def print_completion_summary(start_time, beaches_count, counties_count, forecast_records, daily_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("DATABASE UPDATE COMPLETED!" if success else "DATABASE UPDATE COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"   - Beaches processed: {beaches_count:,}")
    logger.info(f"   - Counties processed: {counties_count:,}")
    logger.info(f"   - Forecast records: {forecast_records:,}")
    logger.info(f"   - Daily condition records: {daily_records:,}")
    logger.info(f"   - Total records: {forecast_records + daily_records:,}")

    if beaches_count > 0:
        records_per_beach = forecast_records / beaches_count
        logger.info(f"   - Records per beach: {records_per_beach:.1f}")

    if total_time > 0:
        records_per_second = (forecast_records + daily_records) / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("=" * 80)
    logger.info("DATA SOURCES USED (100% Public Domain):")
    logger.info("   - NOAA GFSwave - Wave/swell/wind")
    logger.info("   - NOAA GFS Atmospheric - Weather/temperature/pressure")
    logger.info("   - NOAA CO-OPS - Tides/water temperature")
    logger.info("   - Astral library - Sun/moon astronomical data (NOAA algorithms)")
    logger.info("   - All data converted to imperial units")
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

        # Step 1: Cleanup old data
        log_step("Starting data cleanup", 1)
        cleanup_start = time.time()
        if not cleanup_old_data():
            logger.warning("Cleanup had issues, continuing with upsert mode…")
        cleanup_time = time.time() - cleanup_start
        logger.info(f"   >>> Cleanup took: {cleanup_time:.2f} seconds")

        # Step 2: Fetch beaches and counties
        log_step("Fetching location data", 2)
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

        # Step 3: Forecast update (100% NOAA stack)
        log_step("Processing forecast data (NOAA stack)", 3)
        forecast_count = update_forecast_data_noaa_stack(beaches)
        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was processed successfully")
            return False

        # Step 4: Daily conditions (Astral - local calculations)
        log_step("Processing daily conditions (Astral)", 4)
        astral_start = time.time()
        daily_records = update_daily_conditions_astral(counties)
        daily_count = upsert_daily_conditions(daily_records)
        astral_time = time.time() - astral_start
        logger.info(f"   >>> Astral processing took: {astral_time:.2f} seconds")

        # Step 5: Final DB statistics
        log_step("Generating final statistics", 5)
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
            logger.info("[OK] Script completed successfully")
        else:
            logger.info("[FAIL] Script completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
