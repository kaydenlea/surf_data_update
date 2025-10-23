#!/usr/bin/env python3
"""
Step 1: NOAA GFSwave Ocean/Wave Data Update
Fetches ONLY the core wave/swell/surf data from NOAA GFSwave
This is the fast script - runs in ~1-3 minutes

Run this first to get the primary wave data into the database.
Then run step2_supplement_data.py to add weather, tides, and astronomical data.
"""

import sys
import time
from datetime import datetime, timezone, timedelta

# Import our custom modules
from config import (
    logger, NOAA_OCEAN_REQUEST_DELAY, NOAA_OCEAN_BATCH_DELAY,
    DAYS_FORECAST, MAX_WORKERS
)
from utils import log_step
from database import (
    cleanup_old_data, fetch_all_beaches,
    upsert_forecast_data, check_database_connection,
    get_database_stats
)
from noaa_handler import (
    get_noaa_dataset_url, load_noaa_dataset, get_noaa_data_bulk_optimized,
    validate_noaa_dataset
)


# --------------------------------------------------------------------------------------
# HELPER FUNCTIONS
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
                placeholders.append({
                    "beach_id": bid,
                    "timestamp": ts_iso,
                    "primary_swell_height_ft": None,
                    "primary_swell_period_s": None,
                    "primary_swell_direction": None,
                    "secondary_swell_height_ft": None,
                    "secondary_swell_period_s": None,
                    "secondary_swell_direction": None,
                    "tertiary_swell_height_ft": None,
                    "tertiary_swell_period_s": None,
                    "tertiary_swell_direction": None,
                    "surf_height_min_ft": None,
                    "surf_height_max_ft": None,
                    "wave_energy_kj": None,
                    "wind_speed_mph": None,
                    "wind_direction_deg": None,
                    "wind_gust_mph": None,
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

    pacific = pytz.timezone('America/Los_Angeles')
    midnight_today = datetime.now(pacific).replace(hour=0, minute=0, second=0, microsecond=0)

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
# WAVE DATA UPDATE
# --------------------------------------------------------------------------------------

def update_wave_data(beaches):
    """
    Update forecast data with NOAA GFSwave (ocean/wave data only).
    This script does NOT add atmospheric, tide, or astronomical data.
    """
    log_step("Step 1: Updating WAVE DATA from NOAA GFSwave", 1)
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

        # --- UPSERT TO DATABASE ---
        logger.info("   Uploading wave records to database…")
        db_start = time.time()
        total_inserted = upsert_forecast_data(all_noaa_records)
        db_time = time.time() - db_start
        logger.info(f"   >>> Database upsert took: {db_time:.2f} seconds ({db_time/60:.2f} minutes)")

        step_time = time.time() - step_start
        logger.info(f"   >>> TOTAL wave data update time: {step_time:.2f} seconds ({step_time/60:.2f} minutes)")

        log_step(
            f"NOAA GFSwave update completed: {len(beaches)} beaches, "
            f"{total_inserted} records upserted"
        )
        return total_inserted

    except Exception as e:
        logger.error(f"ERROR: NOAA GFSwave update failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0


# --------------------------------------------------------------------------------------
# STARTUP BANNER
# --------------------------------------------------------------------------------------

def print_startup_banner():
    """Print startup banner with configuration info."""
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("STEP 1: NOAA GFSWAVE OCEAN/WAVE DATA UPDATE")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("DATA SOURCE:")
    logger.info("  - NOAA GFSwave - Wave/swell/surf forecasts")
    logger.info("")
    logger.info("FIELDS POPULATED:")
    logger.info("  - Primary/secondary/tertiary swell (height, period, direction)")
    logger.info("  - Surf height range (min/max)")
    logger.info("  - Wave energy")
    logger.info("  - Wind speed/direction/gust (basic)")
    logger.info("")
    logger.info(f"Rate limiting: {NOAA_OCEAN_REQUEST_DELAY}s per request, {NOAA_OCEAN_BATCH_DELAY}s between batches")
    logger.info("")
    logger.info("NOTE: Run step2_supplement_data.py after this to add:")
    logger.info("  - Weather/temperature/pressure (GFS Atmospheric)")
    logger.info("  - Tides/water temperature (NOAA CO-OPS)")
    logger.info("  - Sun/moon data (USNO)")
    logger.info("=" * 80)


def print_completion_summary(start_time, beaches_count, forecast_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("STEP 1: WAVE DATA UPDATE COMPLETED!" if success else "STEP 1: WAVE DATA UPDATE COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"   - Beaches processed: {beaches_count:,}")
    logger.info(f"   - Wave records upserted: {forecast_records:,}")

    if beaches_count > 0:
        records_per_beach = forecast_records / beaches_count
        logger.info(f"   - Records per beach: {records_per_beach:.1f}")

    if total_time > 0:
        records_per_second = forecast_records / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("=" * 80)
    logger.info("NEXT STEP:")
    logger.info("   Run: python step2_supplement_data.py")
    logger.info("   To add weather, tides, and astronomical data")
    logger.info("=" * 80)


# --------------------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------------------

def main():
    """Main execution function."""
    start_time = time.time()
    print_startup_banner()

    try:
        # Step 0: Database check
        if not check_database_connection():
            logger.error("CRITICAL: Database connection failed. Aborting.")
            return False

        # Step 1: Cleanup old data (optional - can comment out if you want to keep old data)
        log_step("Cleaning up old forecast data", 1)
        cleanup_start = time.time()
        if not cleanup_old_data():
            logger.warning("Cleanup had issues, continuing with upsert mode…")
        cleanup_time = time.time() - cleanup_start
        logger.info(f"   >>> Cleanup took: {cleanup_time:.2f} seconds")

        # Step 2: Fetch beaches
        log_step("Fetching beach data", 2)
        fetch_start = time.time()
        beaches = fetch_all_beaches()
        if not beaches:
            logger.error("CRITICAL: No beaches found. Cannot proceed.")
            return False
        fetch_time = time.time() - fetch_start

        logger.info(f"Loaded {len(beaches)} beaches")
        logger.info(f"   >>> Beach fetch took: {fetch_time:.2f} seconds")

        # Step 3: Wave data update
        log_step("Processing wave data (NOAA GFSwave)", 3)
        forecast_count = update_wave_data(beaches)
        if forecast_count == 0:
            logger.error("CRITICAL: No wave data was processed successfully")
            return False

        # Step 4: Final DB statistics
        log_step("Generating statistics", 4)
        stats_start = time.time()
        _ = get_database_stats()
        stats_time = time.time() - stats_start
        logger.info(f"   >>> Statistics generation took: {stats_time:.2f} seconds")

        print_completion_summary(
            start_time, len(beaches), forecast_count, True
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
            logger.info("[OK] Step 1 completed successfully - wave data updated")
        else:
            logger.info("[FAIL] Step 1 completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
