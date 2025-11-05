#!/usr/bin/env python3
"""
Main execution script for Grid-Based NOAA Surf Database Update
Uses grid points instead of individual beaches to reduce database size by 93%.

Stores forecast data per grid point (94 points) instead of per beach (1336 beaches).
Beaches reference their nearest grid point via grid_id foreign key.
"""

import sys
import time
from datetime import datetime, timezone

# Import our custom modules
from config import logger, DAYS_FORECAST
from utils import log_step
from database import (
    upsert_grid_forecast_data, upsert_daily_conditions,
    check_database_connection, get_database_stats
)
from noaa_grid_handler import (
    get_noaa_dataset_url, load_noaa_dataset, validate_noaa_dataset,
    get_noaa_grid_data, fetch_grid_points_from_db
)
from noaa_handler import load_cdip_data
from gfs_atmospheric_handler_v2 import get_gfs_atmospheric_supplement_data
from openmeteo_handler import get_openmeteo_supplement_data
from noaa_tides_handler import get_noaa_tides_supplement_data
from astral_handler import update_daily_conditions_astral, test_astral_calculation


def update_grid_forecast_data():
    """
    Update forecast data using grid-based approach with full data stack:
      1) NOAA GFSwave (extract data directly from 94 grid points)
      2) CDIP enhancement (better wave accuracy)
      3) GFS Atmospheric supplement (pressure)
      4) Open Meteo supplement (weather, temperature, water temperature) - 16 days
      5) NOAA CO-OPS supplement (tides)
      6) Store in grid_forecast_data table
      7) Beaches lookup their forecast via grid_id foreign key
    """
    log_step("Updating grid-based forecast data with full NOAA stack", 1)
    step_start = time.time()

    try:
        # --- FETCH GRID POINTS ---
        logger.info("   Fetching grid points from database...")
        grid_points = fetch_grid_points_from_db()

        if not grid_points:
            logger.error("   No grid points found in database!")
            logger.error("   Run populate_grid_points.py first to set up grid points.")
            return 0

        logger.info(f"   Found {len(grid_points)} grid points")

        # --- LOAD CDIP DATA ---
        logger.info("   Loading CDIP data for enhancement...")
        cdip_data = load_cdip_data()

        # --- LOAD NOAA DATASET ---
        logger.info("   Loading NOAA GFSwave dataset...")
        gfs_start = time.time()
        noaa_url = get_noaa_dataset_url()
        ds = load_noaa_dataset(noaa_url)

        if not validate_noaa_dataset(ds):
            raise Exception("NOAA dataset validation failed")

        # --- EXTRACT GRID DATA WITH CDIP ENHANCEMENT ---
        logger.info("   Extracting data from grid points with CDIP enhancement...")
        grid_records = get_noaa_grid_data(ds, grid_points, cdip_data)
        ds.close()
        gfs_time = time.time() - gfs_start

        logger.info(f"   NOAA GFSwave complete: {len(grid_records):,} records")
        logger.info(f"   >>> GFSwave processing took: {gfs_time:.2f} seconds ({gfs_time/60:.2f} minutes)")

        if grid_records:
            sample_ts = grid_records[0].get("timestamp")
            logger.info(f"   Sample timestamp: {sample_ts}")

        # --- GFS ATMOSPHERIC SUPPLEMENT ---
        logger.info("   Enhancing with GFS Atmospheric data (pressure)...")
        gfs_atmo_start = time.time()
        # Convert grid_points to beach-like format for compatibility
        def _normalize_longitude(lon):
            try:
                lon_val = float(lon)
            except (TypeError, ValueError):
                return lon
            return lon_val - 360.0 if lon_val > 180.0 else lon_val

        grid_as_beaches = []
        for gp in grid_points:
            grid_as_beaches.append({
                "id": gp["id"],
                "Name": f"Grid Point {gp['id']}",
                "LATITUDE": gp["latitude"],
                "LONGITUDE": _normalize_longitude(gp["longitude"]),
            })
        # Temporarily rename grid_id to beach_id for supplement functions
        for rec in grid_records:
            rec["beach_id"] = rec["grid_id"]
        gfs_enhanced = get_gfs_atmospheric_supplement_data(grid_as_beaches, grid_records)
        # Rename back to grid_id
        for rec in gfs_enhanced:
            rec["grid_id"] = rec.pop("beach_id")
        gfs_atmo_time = time.time() - gfs_atmo_start
        logger.info(f"   >>> GFS Atmospheric enhancement took: {gfs_atmo_time:.2f} seconds")

        # --- OPEN METEO SUPPLEMENT (WEATHER & WATER TEMP) ---
        logger.info("   Enhancing with Open Meteo data (weather/water_temp)...")
        openmeteo_start = time.time()
        # Temporarily rename grid_id to beach_id for Open Meteo handler
        for rec in gfs_enhanced:
            rec["beach_id"] = rec["grid_id"]
        openmeteo_enhanced = get_openmeteo_supplement_data(grid_as_beaches, gfs_enhanced)
        # Rename back to grid_id
        for rec in openmeteo_enhanced:
            rec["grid_id"] = rec.pop("beach_id")
        openmeteo_time = time.time() - openmeteo_start
        logger.info(f"   >>> Open Meteo enhancement took: {openmeteo_time:.2f} seconds")

        # --- NOAA CO-OPS SUPPLEMENT ---
        logger.info("   Enhancing with NOAA CO-OPS data (tides)...")
        tides_start = time.time()
        # Temporarily rename grid_id to beach_id for tide handler
        for rec in openmeteo_enhanced:
            rec["beach_id"] = rec.pop("grid_id")
        fully_enhanced = get_noaa_tides_supplement_data(grid_as_beaches, openmeteo_enhanced)
        # Rename back to grid_id
        for rec in fully_enhanced:
            rec["grid_id"] = rec.pop("beach_id")
        tides_time = time.time() - tides_start
        logger.info(f"   >>> CO-OPS enhancement took: {tides_time:.2f} seconds")

        # --- UPSERT TO DATABASE ---
        logger.info("   Uploading fully enhanced grid forecast records to database...")
        db_start = time.time()
        total_inserted = upsert_grid_forecast_data(fully_enhanced)
        db_time = time.time() - db_start
        logger.info(f"   >>> Database upsert took: {db_time:.2f} seconds ({db_time/60:.2f} minutes)")

        step_time = time.time() - step_start
        logger.info(f"   >>> TOTAL grid forecast update time: {step_time:.2f} seconds ({step_time/60:.2f} minutes)")

        log_step(
            f"Grid forecast update completed: {len(grid_points)} grid points, "
            f"{total_inserted:,} records upserted (with CDIP, GFS Atmospheric, Open Meteo, CO-OPS)"
        )
        return total_inserted

    except Exception as e:
        logger.error(f"ERROR: Grid forecast update failed: {e}")
        logger.error(f"      Full error details: {str(e)}")
        return 0


def fetch_all_counties():
    """Fetch all counties from database."""
    from database import supabase

    logger.info("   Fetching counties from database...")
    try:
        response = supabase.table("counties").select("*").execute()
        counties = response.data
        logger.info(f"   Found {len(counties)} counties")
        return counties
    except Exception as e:
        logger.error(f"   Error fetching counties: {e}")
        return []


def run_system_checks():
    """Run pre-flight system checks."""
    logger.info("Running system checks...")

    checks_passed = 0
    total_checks = 2

    if check_database_connection():
        logger.info("[OK] Database connection successful")
        checks_passed += 1
    else:
        logger.error("[FAIL] Database connection failed")

    if test_astral_calculation():
        logger.info("[OK] Astral astronomical calculations working")
        checks_passed += 1
    else:
        logger.error("[FAIL] Astral astronomical calculations failed")

    logger.info(f"System checks: {checks_passed}/{total_checks} passed")
    return checks_passed >= 1  # Need at least DB


def print_startup_banner():
    """Print startup banner with configuration info."""
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = datetime.now(timezone.utc).astimezone(pst)

    logger.info("=" * 80)
    logger.info("SURF DATABASE UPDATE - GRID-BASED APPROACH (93% SMALLER)")
    logger.info("=" * 80)
    logger.info(f"Start time (PST/PDT): {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Forecast days: {DAYS_FORECAST}")
    logger.info("")
    logger.info("GRID-BASED ARCHITECTURE:")
    logger.info("  - 94 nearshore grid points (vs 1,336 beaches)")
    logger.info(f"  - ~{94 * DAYS_FORECAST * 8:,} forecast records per run (16 days × 8 intervals/day)")
    logger.info("  - 93% reduction in database size vs beach-based approach")
    logger.info("  - Beaches reference grid points via grid_id foreign key")
    logger.info("")
    logger.info("DATA SOURCES (All Public Domain - Free for Commercial Use):")
    logger.info("  - NOAA GFSwave - Wave/swell forecasts (direct grid extraction)")
    logger.info("  - GFS Atmospheric - Pressure data")
    logger.info("  - Open Meteo - Weather codes, temperature, water temperature (16 days)")
    logger.info("  - NOAA CO-OPS - Tides")
    logger.info("  - Astral library - Sun/moon rise/set, moon phase (using NOAA algorithms)")
    logger.info("")
    logger.info("DATA FLOW:")
    logger.info("  1. Extract wave data from 94 grid points (NOAA GFSwave + CDIP)")
    logger.info("  2. Supplement with pressure (GFS Atmospheric)")
    logger.info("  3. Supplement with weather & water temp (Open Meteo)")
    logger.info("  4. Supplement with tides (NOAA CO-OPS)")
    logger.info("  5. Store in grid_forecast_data table")
    logger.info("  6. Beaches lookup nearest grid point forecast via grid_id")
    logger.info("=" * 80)


def print_completion_summary(start_time, grid_count, counties_count, forecast_records, daily_records, success):
    """Print completion summary with statistics."""
    total_time = time.time() - start_time

    logger.info("=" * 80)
    logger.info("GRID-BASED DATABASE UPDATE COMPLETED!" if success else "DATABASE UPDATE COMPLETED WITH ERRORS!")
    logger.info("=" * 80)

    logger.info("EXECUTION SUMMARY:")
    logger.info(f"   - Total execution time: {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
    logger.info(f"   - Grid points processed: {grid_count:,}")
    logger.info(f"   - Counties processed: {counties_count:,}")
    logger.info(f"   - Grid forecast records: {forecast_records:,}")
    logger.info(f"   - Daily condition records: {daily_records:,}")
    logger.info(f"   - Total records: {forecast_records + daily_records:,}")

    if grid_count > 0:
        records_per_grid = forecast_records / grid_count
        logger.info(f"   - Records per grid point: {records_per_grid:.1f}")

    if total_time > 0:
        records_per_second = (forecast_records + daily_records) / total_time
        logger.info(f"   - Processing rate: {records_per_second:.1f} records/second")

    logger.info("")
    logger.info("EFFICIENCY COMPARISON:")
    logger.info("   - Beach-based approach: ~172,344 records (1,336 beaches × 129 times)")
    logger.info(f"   - Grid-based approach: {forecast_records:,} records (94 grids × 129 times)")
    if forecast_records > 0:
        reduction = ((172344 - forecast_records) / 172344) * 100
        logger.info(f"   - Database size reduction: {reduction:.1f}%")

    logger.info("=" * 80)
    logger.info("DATA SOURCES USED (100% Public Domain):")
    logger.info("   - NOAA GFSwave - Direct grid extraction for waves (no interpolation)")
    logger.info("   - GFS Atmospheric - Pressure data")
    logger.info("   - Open Meteo - Weather, temperature, water temperature (16-day forecast)")
    logger.info("   - NOAA CO-OPS - Tides")
    logger.info("   - Astral library - Sun/moon astronomical data (NOAA algorithms)")
    logger.info("   - All data in imperial units")
    logger.info("=" * 80)


def main():
    """Main execution function."""
    start_time = time.time()
    print_startup_banner()

    try:
        # Step 0: System checks
        if not run_system_checks():
            logger.error("CRITICAL: System checks failed. Aborting.")
            return False

        # Step 1: Grid forecast update (NOAA GFSwave from grid points)
        log_step("Processing grid forecast data", 1)
        forecast_count = update_grid_forecast_data()
        if forecast_count == 0:
            logger.error("CRITICAL: No forecast data was processed successfully")
            return False

        # Step 2: Daily conditions (Astral - local calculations)
        log_step("Processing daily conditions (Astral)", 2)
        astral_start = time.time()
        counties = fetch_all_counties()
        daily_records = update_daily_conditions_astral(counties)
        daily_count = upsert_daily_conditions(daily_records)
        astral_time = time.time() - astral_start
        logger.info(f"   >>> Astral processing took: {astral_time:.2f} seconds")

        # Step 3: Final DB statistics
        log_step("Generating final statistics", 3)
        stats_start = time.time()
        _ = get_database_stats()
        stats_time = time.time() - stats_start
        logger.info(f"   >>> Statistics generation took: {stats_time:.2f} seconds")

        # Get actual grid point count
        grid_points = fetch_grid_points_from_db()
        grid_count = len(grid_points) if grid_points else 94

        print_completion_summary(
            start_time, grid_count, len(counties),
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
                94,  # Default grid count
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
            logger.info("[OK] Grid-based script completed successfully")
        else:
            logger.info("[FAIL] Grid-based script completed with errors")

        logger.info(f"Exiting with code: {exit_code}")
        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        logger.info("Exiting with code: 2")
        sys.exit(2)
