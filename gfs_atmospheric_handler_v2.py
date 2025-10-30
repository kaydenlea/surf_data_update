#!/usr/bin/env python3
"""
NOAA GFS Atmospheric Model Handler - V2 (Rewritten to match swell handler structure)
Provides weather data (temperature, pressure, weather codes) from GFS atmospheric model
Exact same architecture as noaa_handler.py for consistency
"""

import time
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import pytz
from typing import List, Dict, Optional

from config import logger, NOAA_ATMOSPHERIC_REQUEST_DELAY, NOAA_ATMOSPHERIC_BATCH_DELAY
from utils import (
    enforce_noaa_rate_limit, safe_float, safe_int, celsius_to_fahrenheit,
    mps_to_mph, pa_to_inhg
)


# GFS Atmospheric Base URLs (0.25 degree resolution)
GFS_ATMOSPHERIC_BASE_URLS = [
    "https://nomads.ncep.noaa.gov/dods/gfs_0p25",
    "http://nomads.ncep.noaa.gov/dods/gfs_0p25",
]


# WMO Weather codes mapping
def derive_weather_code(cloud_cover_pct: float, precip_rate: float, temperature_f: float) -> int:
    """Derive WMO weather code from cloud cover, precipitation rate, and temperature."""
    if cloud_cover_pct is None:
        cloud_cover_pct = 50
    if precip_rate is None:
        precip_rate = 0
    if temperature_f is None:
        temperature_f = 60

    DRIZZLE_THRESHOLD = 0.5
    LIGHT_THRESHOLD = 2.5
    MODERATE_THRESHOLD = 10.0
    HEAVY_THRESHOLD = 50.0

    if precip_rate > DRIZZLE_THRESHOLD:
        is_snow = temperature_f < 35
        if is_snow:
            if precip_rate >= HEAVY_THRESHOLD:
                return 75
            elif precip_rate >= MODERATE_THRESHOLD:
                return 73
            else:
                return 71
        else:
            if precip_rate >= HEAVY_THRESHOLD and cloud_cover_pct >= 70:
                return 95
            if precip_rate >= HEAVY_THRESHOLD:
                return 65
            elif precip_rate >= MODERATE_THRESHOLD:
                return 63
            elif precip_rate >= LIGHT_THRESHOLD:
                return 61
            else:
                return 51

    if cloud_cover_pct < 10:
        return 0
    elif cloud_cover_pct < 25:
        return 1
    elif cloud_cover_pct < 50:
        return 2
    elif cloud_cover_pct < 90:
        return 3
    else:
        return 45


def get_gfs_atmospheric_dataset_url() -> Optional[str]:
    """Get the current GFS atmospheric dataset URL."""
    logger.info("   Searching for available GFS Atmospheric dataset...")

    now_utc = datetime.now(timezone.utc)
    possible_hours = [18, 12, 6, 0]
    current_hour = now_utc.hour
    effective_hour = (current_hour - 4) % 24
    recent_runs = [h for h in possible_hours if h <= effective_hour]

    if not recent_runs:
        run_date = (now_utc - timedelta(days=1)).date()
        run_hour = 18
    else:
        if current_hour < 4:
            run_date = (now_utc - timedelta(days=1)).date()
        else:
            run_date = now_utc.date()
        run_hour = recent_runs[0]

    logger.info(f"   Starting search from {run_date} {run_hour:02d}z")

    for days_back in range(3):
        test_date = run_date - timedelta(days=days_back)
        date_str = test_date.strftime("%Y%m%d")

        for base_url in GFS_ATMOSPHERIC_BASE_URLS:
            hours_to_try = [run_hour] + [h for h in [18, 12, 6, 0] if h != run_hour]
            for hour in hours_to_try:
                url = f"{base_url}/gfs{date_str}/gfs_0p25_{hour:02d}z"

                try:
                    enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)
                    ds = xr.open_dataset(url, engine="netcdf4")

                    required_vars = ["tmp2m", "pressfc"]
                    missing = [v for v in required_vars if v not in ds.variables]

                    if missing:
                        ds.close()
                        continue

                    ds.close()
                    logger.info(f"   [OK] Found GFS Atmospheric: {url}")
                    return url

                except Exception:
                    continue

    logger.error("   Could not find valid GFS Atmospheric dataset")
    return None


def load_gfs_atmospheric_dataset(url: str) -> Optional[xr.Dataset]:
    """Load GFS Atmospheric dataset with error handling and rate limiting."""
    logger.info("   Loading GFS Atmospheric dataset...")
    enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)

    try:
        ds = xr.open_dataset(url, engine="netcdf4")
        logger.info(f"   [OK] GFS Atmospheric dataset loaded successfully")
        return ds
    except Exception as e:
        logger.error(f"   Failed to load GFS Atmospheric dataset: {e}")
        return None


def extract_grid_point_data(ds, grid_lat, grid_lon, sel_idx, filtered_time_vals):
    """
    Extract atmospheric variables for a single grid point.
    EXACTLY matches the swell handler structure.
    """
    try:
        # Extract each variable separately using .sel().values (SAME AS SWELL HANDLER)
        # Use method='nearest' to handle slight coordinate mismatches
        tmp = ds["tmp2m"].sel(lat=grid_lat, lon=grid_lon, method='nearest').values
        pres = ds["pressfc"].sel(lat=grid_lat, lon=grid_lon, method='nearest').values

        # Optional variables
        cloud = None
        precip = None
        if 'tcdcclm' in ds.data_vars:
            try:
                cloud = ds["tcdcclm"].sel(lat=grid_lat, lon=grid_lon, method='nearest').values
            except Exception:
                pass

        if 'pratesfc' in ds.data_vars:
            try:
                precip = ds["pratesfc"].sel(lat=grid_lat, lon=grid_lon, method='nearest').values
            except Exception:
                pass

        # Slice with the time index mask
        grid_data = {
            'time_vals': filtered_time_vals,
            'temperature_k': tmp[sel_idx],
            'pressure_pa': pres[sel_idx],
            'cloud_cover_pct': cloud[sel_idx] if cloud is not None else None,
            'precip_rate_kgm2s': precip[sel_idx] if precip is not None else None,
        }
        return grid_data

    except Exception as e:
        logger.error(f"   Failed to extract grid point data: {e}")
        return None


def get_gfs_atmospheric_supplement_data(beaches: List[Dict], existing_records: List[Dict]) -> List[Dict]:
    """
    Supplement existing forecast records with GFS Atmospheric data.
    REWRITTEN to match noaa_handler.py structure EXACTLY.
    """
    start_time = time.time()
    logger.info("   GFS Atmospheric: fetching weather data...")

    # Get GFS dataset
    gfs_url = get_gfs_atmospheric_dataset_url()
    if not gfs_url:
        logger.error("   GFS Atmospheric: No dataset URL found")
        return existing_records

    ds = load_gfs_atmospheric_dataset(gfs_url)
    if not ds:
        logger.error("   GFS Atmospheric: Failed to load dataset")
        return existing_records

    # Get time range and filter to forecast window (SAME AS SWELL HANDLER)
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    logger.info(f"   GFS dataset time range: {time_vals_full[0]} to {time_vals_full[-1]} (UTC)")

    # Filter to 8-day window
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific_tz)
    window_start = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    window_end = window_start + pd.Timedelta(days=8)

    window_start_utc = window_start.astimezone(pytz.UTC)
    window_end_utc = window_end.astimezone(pytz.UTC)

    sel_idx = (time_vals_full >= window_start_utc) & (time_vals_full <= window_end_utc)
    filtered_time_vals = time_vals_full[sel_idx]

    logger.info(f"   Filtered to {len(filtered_time_vals)} time steps in 8-day forecast window")

    # Step 1: Group beaches by location (optimized for GFS 0.25 degree grid)
    logger.info("   Grouping beaches by location...")
    location_groups = {}

    for beach in beaches:
        # Round to 0.25 degree - matches GFS grid resolution exactly
        # More efficient than 0.1 degree grouping (fewer API calls, same data quality)
        rounded_lat = round(beach["LATITUDE"] / 0.25) * 0.25
        rounded_lon = round(beach["LONGITUDE"] / 0.25) * 0.25
        location_key = f"{rounded_lat:.2f},{rounded_lon:.2f}"

        if location_key not in location_groups:
            location_groups[location_key] = []
        location_groups[location_key].append(beach)

    logger.info(f"   Grouped {len(beaches)} beaches into {len(location_groups)} location groups")

    # Step 2: Process each location group (SAME AS SWELL HANDLER)
    grid_data_cache = {}

    group_count = 0
    for location_key, group_beaches in location_groups.items():
        group_count += 1
        beach_count = len(group_beaches)

        logger.info(f"   Loading location group {group_count}/{len(location_groups)}: {location_key} ({beach_count} beaches)")

        representative_beach = group_beaches[0]
        # Round to actual GFS grid (0.25 degrees) for data extraction
        # Even though we group by 0.1 degrees, we still extract from 0.25 degree grid
        grid_lat = round(representative_beach["LATITUDE"] / 0.25) * 0.25

        # Convert longitude to 0-360 range (GFS uses 0-360, not -180 to 180)
        lon_raw = representative_beach["LONGITUDE"]
        lon_360 = lon_raw if lon_raw >= 0 else lon_raw + 360
        grid_lon = round(lon_360 / 0.25) * 0.25

        # Rate limiting BEFORE extraction
        enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)

        try:
            # Extract grid point data (SAME STRUCTURE AS SWELL)
            grid_data = extract_grid_point_data(ds, grid_lat, grid_lon, sel_idx, filtered_time_vals)

            if grid_data:
                grid_data_cache[location_key] = grid_data
                logger.info(f"   Location {location_key} loaded successfully")
            else:
                grid_data_cache[location_key] = None

        except Exception as e:
            logger.error(f"   Failed to load location {location_key}: {e}")
            grid_data_cache[location_key] = None

        # Delay between location groups
        if group_count < len(location_groups):
            logger.info(f"   Waiting {NOAA_ATMOSPHERIC_BATCH_DELAY}s before next location...")
            time.sleep(NOAA_ATMOSPHERIC_BATCH_DELAY)

    ds.close()

    # Step 3: Process all beaches using cached grid data (SAME AS SWELL HANDLER)
    logger.info("   Processing beaches using cached grid data...")

    # Build index for quick updates
    updated_records = list(existing_records)
    record_index = {}
    for idx, record in enumerate(updated_records):
        bid = record.get("beach_id")
        ts = record.get("timestamp")
        if bid and ts:
            record_index[f"{bid}_{ts}"] = idx

    filled_count = 0

    for location_key, group_beaches in location_groups.items():
        if location_key not in grid_data_cache or grid_data_cache[location_key] is None:
            continue

        grid_data = grid_data_cache[location_key]
        time_vals = grid_data['time_vals']

        for beach in group_beaches:
            bid = beach["id"]

            # Process each timestep
            for i, time_val in enumerate(time_vals):
                ts_utc = time_val.tz_convert("UTC") if time_val.tz is not None else time_val.tz_localize("UTC")
                ts_iso = ts_utc.isoformat()

                key = f"{bid}_{ts_iso}"
                if key not in record_index:
                    continue

                rec = updated_records[record_index[key]]

                # Fill temperature
                if grid_data['temperature_k'] is not None:
                    temp_k = grid_data['temperature_k'][i]
                    if not np.isnan(temp_k):
                        temp_c = temp_k - 273.15
                        rec["temperature"] = safe_float(celsius_to_fahrenheit(temp_c))
                        filled_count += 1

                # Fill pressure
                if grid_data['pressure_pa'] is not None:
                    pres_pa = grid_data['pressure_pa'][i]
                    if not np.isnan(pres_pa):
                        rec["pressure_inhg"] = safe_float(pa_to_inhg(pres_pa))
                        filled_count += 1

                # Fill weather code
                cloud_pct = None
                precip_mmhr = None
                temp_f = rec.get("temperature")

                if grid_data['cloud_cover_pct'] is not None:
                    cloud_val = grid_data['cloud_cover_pct'][i]
                    if not np.isnan(cloud_val):
                        cloud_pct = float(cloud_val)

                if grid_data['precip_rate_kgm2s'] is not None:
                    precip_val = grid_data['precip_rate_kgm2s'][i]
                    if not np.isnan(precip_val):
                        precip_mmhr = float(precip_val) * 3600

                weather_code = derive_weather_code(cloud_pct, precip_mmhr, temp_f)
                rec["weather"] = safe_int(weather_code)
                filled_count += 1

    elapsed_time = time.time() - start_time
    logger.info(f"   GFS Atmospheric: filled {filled_count} field values")
    logger.info(f"   GFS Atmospheric: completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")

    return updated_records


def test_gfs_atmospheric_connection() -> bool:
    """Test GFS Atmospheric dataset connectivity."""
    try:
        logger.info("Testing GFS Atmospheric dataset connection...")
        url = get_gfs_atmospheric_dataset_url()

        if not url:
            logger.error("GFS Atmospheric: Could not find dataset URL")
            return False

        ds = load_gfs_atmospheric_dataset(url)
        if not ds:
            logger.error("GFS Atmospheric: Could not load dataset")
            return False

        ds.close()
        logger.info("GFS Atmospheric dataset connection successful")
        return True

    except Exception as e:
        logger.error(f"GFS Atmospheric connection test failed: {e}")
        return False
