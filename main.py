#!/usr/bin/env python3
"""
Main execution script for Hybrid Surf Database Update
Orchestrates multiple data sources:
  - NOAA GFSwave (primary wave/swell data)
  - Open-Meteo (atmospheric supplement: temperature, weather, wind gust, pressure)
  - NOAA CO-OPS (tides and water temperature)
  - Astral library (sun/moon astronomical calculations using NOAA algorithms)
"""

import sys
import time
from datetime import datetime, timezone, timedelta

# Import our custom modules
from config import (
    logger, TIDE_ADJUSTMENT_FT, NOAA_REQUEST_DELAY, OPENMETEO_REQUEST_DELAY,
    NOAA_BATCH_DELAY, OPENMETEO_BATCH_DELAY, DAYS_FORECAST
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
from openmeteo_handler import (
    get_openmeteo_supplement_data,
    test_openmeteo_connection
)
from noaa_tides_handler import get_noaa_tides_supplement_data, test_noaa_tides_connection
from astral_handler import update_daily_conditions_astral, test_astral_calculation

# --------------------------------------------------------------------------------------
# HYBRID FORECAST UPDATE
# --------------------------------------------------------------------------------------

def _ensure_today_midnight_start(records, beaches):
    """Ensure each beach has records starting from today's 12:00 AM (Pacific).

    If NOAA starts later (e.g., first timestep is 06:00), create placeholder
    rows at 00:00, 03:00, etc., up to (but not including) the earliest existing
    timestamp. Open-Meteo supplement will fill target fields on these rows.
    """
    if not records:
        return records

    import pytz
    from datetime import datetime, timedelta, time as dtime

    pacific = pytz.timezone('America/Los_Angeles')
    today = datetime.now(pacific).date()
    midnight = pacific.localize(datetime.combine(today, dtime(0, 0)))

    # Build quick lookups
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
        # Find earliest timestamp for today for this beach
        earliest_ts = None
        for r in recs:
            ts_str = r.get("timestamp")
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                continue
            # Only consider timestamps on or after today's midnight
            if ts.tzinfo is None:
                continue  # expect tz-aware local ISO
            if ts >= midnight and (earliest_ts is None or ts < earliest_ts):
                earliest_ts = ts

        # If no records at/after midnight, we will backfill the entire day start
        if earliest_ts is None:
            # No records for today yet; start at midnight and let supplement fill
            target_end = midnight
        else:
            target_end = earliest_ts

        # Generate 3-hour steps from midnight up to target_end (exclusive)
        t = midnight
        placeholders = []
        while t < target_end:
            # Avoid duplicating if a record already exists at this timestamp
            ts_iso = t.isoformat()
            if not any(r.get("timestamp") == ts_iso for r in recs):
                placeholders.append({
                    "beach_id": bid,
                    "timestamp": ts_iso,
                    # NOAA fields left None; Open-Meteo will fill supplement fields
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
                    "water_temp_f": None,
                    "tide_level_ft": None,
                    "temperature": None,
                    "weather": None,
                    "pressure_inhg": None,
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


def update_forecast_data_hybrid(beaches):
    """
    Update forecast data using:
      1) NOAA GFSwave (primary: swell, surf, wind speed/dir) -> records with local timestamps
      2) Open-Meteo supplement (fills: temperature, weather, wind_speed_mph, wind_gust_mph, pressure_inhg)
      3) NOAA CO-OPS supplement (fills: tide_level_ft, water_temp_f)
    """
    log_step("Updating forecast data with hybrid NOAA + Open-Meteo", 4)

    try:
        # --- NOAA PRIMARY ---
        logger.info("   Loading NOAA GFSwave dataset (rate-limited)…")
        noaa_url = get_noaa_dataset_url()
        ds = load_noaa_dataset(noaa_url)

        if not validate_noaa_dataset(ds):
            raise Exception("NOAA dataset validation failed")

        # Extract NOAA records (already restricted to [now_utc, now_utc+7d) and
        # timestamps converted to America/Los_Angeles in noaa_handler)
        all_noaa_records = get_noaa_data_bulk_optimized(ds, beaches)
        ds.close()

        logger.info(f"   NOAA extract complete: {len(all_noaa_records)} hourly records")

        # Quick observability: confirm tz form
        if all_noaa_records:
            sample_ts = all_noaa_records[0].get("timestamp")
            logger.info(f"   Sample NOAA timestamp (should be America/Los_Angeles ISO): {sample_ts}")

        # Ensure we start from today's 12:00 AM (Pacific) per-beach
        all_noaa_records = _drop_records_before_today(all_noaa_records)
        all_noaa_records = _ensure_today_midnight_start(all_noaa_records, beaches)

        # --- OPEN-METEO SUPPLEMENT ---
        logger.info("   Enhancing with Open-Meteo supplement (temperature, weather, wind, pressure)…")
        openmeteo_enhanced = get_openmeteo_supplement_data(beaches, all_noaa_records)

        # --- NOAA CO-OPS SUPPLEMENT (Tides, Water Temp) ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides/water temp)…")
        fully_enhanced = get_noaa_tides_supplement_data(beaches, openmeteo_enhanced)

        # Upsert to DB
        logger.info("   Uploading enhanced records to database…")
        total_inserted = upsert_forecast_data(fully_enhanced)

        log_step(
            f"Hybrid forecast update completed: {len(beaches)} beaches, "
            f"{total_inserted} records upserted"
        )
        return total_inserted

    except Exception as e:
        logger.error(f"ERROR: Hybrid forecast update failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        # Fallback to pure Open-Meteo if NOAA fails at any step
        # logger.info("   Falling back to pure Open-Meteo data (full horizon)…")
        return 0

# --------------------------------------------------------------------------------------
# OPEN-METEO FALLBACK (FULL HORIZON)
# --------------------------------------------------------------------------------------

# def update_forecast_data_fallback(beaches):
#     """Fallback to pure Open-Meteo data if NOAA path fails entirely."""
#     logger.info("   Using Open-Meteo fallback mode (full 7-day forecast)…")

#     try:
#         fallback_records = get_openmeteo_fallback_data(beaches)
#         total_inserted = upsert_forecast_data(fallback_records)

#         log_step(
#             f"Open-Meteo fallback completed: {len(beaches)} beaches, "
#             f"{total_inserted} records"
#         )
#         return total_inserted

#     except Exception as e:
#         logger.error(f"ERROR: Open-Meteo fallback also failed: {e}")
#         return 0

# --------------------------------------------------------------------------------------
# DAILY CONDITIONS (ASTRAL - NOAA ALGORITHMS)
# --------------------------------------------------------------------------------------

def update_daily_conditions(counties):
    """Update daily county conditions using Astral library (NOAA astronomical algorithms)."""
    log_step("Updating daily conditions", 5)

    daily_records = update_daily_conditions_astral(counties)
    daily_count = upsert_daily_conditions(daily_records)

    log_step(f"Daily conditions updated: {len(counties)} counties, {daily_count} records")
    return daily_count

# --------------------------------------------------------------------------------------
# SYSTEM CHECKS & BANNERS
# --------------------------------------------------------------------------------------

def run_system_checks():
    """Run pre-flight system checks."""
    logger.info("Running system checks…")

    checks_passed = 0
    total_checks = 4

    if check_database_connection():
        logger.info("✓ Database connection successful")
        checks_passed += 1
    else:
        logger.error("✗ Database connection failed")

    if test_openmeteo_connection():
        logger.info("✓ Open-Meteo API connection successful")
        checks_passed += 1
    else:
        logger.error("✗ Open-Meteo API connection failed")

    if test_noaa_tides_connection():
        logger.info("✓ NOAA CO-OPS API connection successful")
        checks_passed += 1
    else:
        logger.error("✗ NOAA CO-OPS API connection failed")

    if test_astral_calculation():
        logger.info("✓ Astral calculations successful")
        checks_passed += 1
    else:
        logger.error("✗ Astral calculations failed")

    logger.info(f"System checks: {checks_passed}/{total_checks} passed")
    # Need at least DB + 2 data sources
    return checks_passed >= 3

def print_startup_banner():
    """Print startup banner with configuration info."""
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("SURF DATABASE UPDATE - HYBRID NOAA + OPEN-METEO + ASTRAL")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info(f"Tide adjustment: +{TIDE_ADJUSTMENT_FT} feet")
    logger.info("")
    logger.info("DATA SOURCES:")
    logger.info("  • Primary: NOAA GFSwave (wave/swell/wind speed/dir)")
    logger.info("  • Supplement: Open-Meteo (temperature, weather, wind gust, pressure)")
    logger.info("  • Tides: NOAA CO-OPS (tide levels, water temperature)")
    logger.info("  • Astronomical: Astral library (sun/moon using NOAA algorithms)")
    logger.info("")
    logger.info("Units: Imperial (mph, feet, °F, inHg)")
    logger.info("Swell ranking: Dynamic by surf impact score")
    logger.info(f"NOAA rate limiting: {NOAA_REQUEST_DELAY}s per request, {NOAA_BATCH_DELAY}s between groups")
    logger.info(f"Open-Meteo rate limiting: {OPENMETEO_REQUEST_DELAY}s per request, {OPENMETEO_BATCH_DELAY}s between batches")
    logger.info("=" * 80)

def print_completion_summary(start_time, beaches_count, counties_count, forecast_records, daily_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("HYBRID DATABASE UPDATE COMPLETED SUCCESSFULLY!" if success else "DATABASE UPDATE COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   • Total execution time: {total_time:.1f} seconds")
    logger.info(f"   • Beaches processed: {beaches_count:,}")
    logger.info(f"   • Counties processed: {counties_count:,}")
    logger.info(f"   • Forecast records: {forecast_records:,}")
    logger.info(f"   • Daily condition records: {daily_records:,}")
    logger.info(f"   • Total records: {forecast_records + daily_records:,}")

    if beaches_count > 0:
        records_per_beach = forecast_records / beaches_count
        logger.info(f"   • Records per beach: {records_per_beach:.1f}")

    if total_time > 0:
        records_per_second = (forecast_records + daily_records) / total_time
        logger.info(f"   • Processing rate: {records_per_second:.1f} records/second")

    logger.info("=" * 80)
    logger.info("DATA SOURCES USED:")
    logger.info("   • Primary: NOAA GFSwave (wave/swell/wind speed/dir)")
    logger.info("   • Supplement: Open-Meteo (temperature, weather, wind gust, pressure)")
    logger.info("   • Tides: NOAA CO-OPS (tide levels, water temperature)")
    logger.info("   • Astronomical: Astral library (sun/moon using NOAA algorithms)")
    logger.info("   • All data converted to imperial units")
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
        if not cleanup_old_data():
            logger.warning("Cleanup had issues, continuing with upsert mode…")

        # Step 2: Fetch beaches and counties
        log_step("Fetching location data", 2)
        beaches = fetch_all_beaches()
        if not beaches:
            logger.error("CRITICAL: No beaches found. Cannot proceed.")
            return False

        counties = fetch_all_counties()
        if not counties:
            logger.error("CRITICAL: No counties found. Cannot proceed.")
            return False

        logger.info(f"Loaded {len(beaches)} beaches and {len(counties)} counties")

        # Step 3: Hybrid forecast update (NOAA primary + OM supplement)
        log_step("Processing forecast data", 3)
        forecast_count = update_forecast_data_hybrid(beaches)
        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was processed successfully")
            return False

        # Step 4: Daily conditions
        log_step("Processing daily conditions", 4)
        daily_count = update_daily_conditions(counties)

        # Step 5: Final DB statistics
        log_step("Generating final statistics", 5)
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
            logger.info("✓ Script completed successfully")
        else:
            logger.info("✗ Script completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
