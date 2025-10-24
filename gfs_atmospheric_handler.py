#!/usr/bin/env python3
"""
NOAA GFS Atmospheric Model Handler
Provides weather data (temperature, wind, pressure, weather codes) from GFS atmospheric model
Uses same NOMADS infrastructure as GFSwave for consistency and performance
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

# WMO Weather codes mapping from cloud cover and precipitation
def derive_weather_code(cloud_cover_pct: float, precip_rate: float, temperature_f: float) -> int:
    """
    Derive WMO weather code from cloud cover, precipitation rate, and temperature.

    WMO Codes used:
    0 = Clear sky
    1 = Mainly clear
    2 = Partly cloudy
    3 = Overcast
    45 = Fog
    51 = Drizzle (light precipitation)
    61 = Light rain
    63 = Moderate rain
    65 = Heavy rain
    71 = Light snow
    73 = Moderate snow
    75 = Heavy snow
    95 = Thunderstorm

    Args:
        cloud_cover_pct: Cloud cover percentage (0-100)
        precip_rate: Precipitation rate in mm/hr (converted from kg/m²/s)
        temperature_f: Temperature in Fahrenheit
    """
    # Default to partly cloudy if no data
    if cloud_cover_pct is None:
        cloud_cover_pct = 50

    if precip_rate is None:
        precip_rate = 0

    if temperature_f is None:
        temperature_f = 60  # Default to above freezing

    # Precipitation thresholds (mm/hr)
    # kg/m²/s * 3600 = mm/hr (since 1 kg/m² = 1 mm of water)
    DRIZZLE_THRESHOLD = 0.5    # mm/hr
    LIGHT_THRESHOLD = 2.5      # mm/hr
    MODERATE_THRESHOLD = 10.0  # mm/hr
    HEAVY_THRESHOLD = 50.0     # mm/hr

    # Check for precipitation first
    if precip_rate > DRIZZLE_THRESHOLD:
        # Determine if rain or snow based on temperature
        is_snow = temperature_f < 35  # Below 35°F likely snow

        if is_snow:
            # Snow codes
            if precip_rate >= HEAVY_THRESHOLD:
                return 75  # Heavy snow
            elif precip_rate >= MODERATE_THRESHOLD:
                return 73  # Moderate snow
            else:
                return 71  # Light snow
        else:
            # Rain codes
            # Check for thunderstorm (heavy rain + high cloud cover)
            if precip_rate >= HEAVY_THRESHOLD and cloud_cover_pct >= 70:
                return 95  # Thunderstorm

            if precip_rate >= HEAVY_THRESHOLD:
                return 65  # Heavy rain
            elif precip_rate >= MODERATE_THRESHOLD:
                return 63  # Moderate rain
            elif precip_rate >= LIGHT_THRESHOLD:
                return 61  # Light rain
            else:
                return 51  # Drizzle

    # No precipitation - use cloud cover
    if cloud_cover_pct < 10:
        return 0  # Clear
    elif cloud_cover_pct < 25:
        return 1  # Mainly clear
    elif cloud_cover_pct < 50:
        return 2  # Partly cloudy
    elif cloud_cover_pct < 90:
        return 3  # Overcast
    else:
        return 45  # Fog/very cloudy


def get_gfs_atmospheric_dataset_url() -> Optional[str]:
    """
    Get the current GFS atmospheric dataset URL.
    GFS runs at 00, 06, 12, 18 UTC daily.
    """
    logger.info("   Searching for available GFS Atmospheric dataset...")

    now_utc = datetime.now(timezone.utc)

    # GFS runs: 00z, 06z, 12z, 18z
    # Try most recent run first, then fall back
    # Note: GFS runs take ~4 hours to complete, so latest run may not be available yet
    possible_hours = [18, 12, 6, 0]
    current_hour = now_utc.hour

    # Find most recent run that should be available (subtract 4 hours for processing time)
    effective_hour = (current_hour - 4) % 24
    recent_runs = [h for h in possible_hours if h <= effective_hour]

    if not recent_runs:
        # Use previous day's last run
        run_date = (now_utc - timedelta(days=1)).date()
        run_hour = 18
    else:
        # If we're past midnight but before first run completes, use yesterday
        if current_hour < 4:
            run_date = (now_utc - timedelta(days=1)).date()
        else:
            run_date = now_utc.date()
        run_hour = recent_runs[0]

    logger.info(f"   Starting search from {run_date} {run_hour:02d}z (UTC now: {now_utc.strftime('%Y-%m-%d %H:%M')})")

    # Try current run date, then progressively older dates
    for days_back in range(3):
        test_date = run_date - timedelta(days=days_back)
        date_str = test_date.strftime("%Y%m%d")

        for base_url in GFS_ATMOSPHERIC_BASE_URLS:
            # Try the calculated run hour first, then all other hours
            hours_to_try = [run_hour] + [h for h in [18, 12, 6, 0] if h != run_hour]
            for hour in hours_to_try:
                url = f"{base_url}/gfs{date_str}/gfs_0p25_{hour:02d}z"

                try:
                    enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)
                    logger.debug(f"   Testing GFS Atmospheric URL: {url}")

                    ds = xr.open_dataset(url, engine="netcdf4")

                    # Verify required variables exist
                    required_vars = ["tmp2m", "ugrd10m", "vgrd10m", "pressfc"]
                    missing = [v for v in required_vars if v not in ds.variables]

                    if missing:
                        logger.debug(f"   Missing variables: {missing}")
                        ds.close()
                        continue

                    ds.close()
                    logger.info(f"   [OK] Found GFS Atmospheric: {url}")
                    return url

                except Exception as e:
                    logger.debug(f"   Failed: {e}")
                    continue

    logger.error("   Could not find valid GFS Atmospheric dataset")
    return None


def load_gfs_atmospheric_dataset(url: str) -> Optional[xr.Dataset]:
    """
    Load GFS Atmospheric dataset with error handling and rate limiting.
    """
    logger.info("   Loading GFS Atmospheric dataset with rate limiting...")

    enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)

    try:
        ds = xr.open_dataset(url, engine="netcdf4")
        logger.info(f"   [OK] GFS Atmospheric dataset loaded successfully")
        return ds
    except Exception as e:
        logger.error(f"   Failed to load GFS Atmospheric dataset: {e}")
        return None


def validate_gfs_atmospheric_dataset(ds: xr.Dataset) -> bool:
    """
    Validate that the GFS Atmospheric dataset has required variables.
    """
    required_vars = {
        "tmp2m": "2-meter temperature",
        "ugrd10m": "10-meter U wind component",
        "vgrd10m": "10-meter V wind component",
        "pressfc": "Surface pressure",
    }

    optional_vars = {
        "gustsfc": "Surface wind gust",
        "tcdcclm": "Total cloud cover",
        "pratesfc": "Precipitation rate",
    }

    missing_required = []
    for var, desc in required_vars.items():
        if var not in ds.variables:
            missing_required.append(f"{var} ({desc})")

    if missing_required:
        logger.error(f"   Missing required variables: {', '.join(missing_required)}")
        return False

    missing_optional = []
    for var, desc in optional_vars.items():
        if var not in ds.variables:
            missing_optional.append(f"{var} ({desc})")

    if missing_optional:
        logger.warning(f"   Missing optional variables: {', '.join(missing_optional)}")

    logger.info("   [OK] GFS Atmospheric dataset validation passed")
    return True


def extract_gfs_atmospheric_point(
    ds: xr.Dataset,
    lat: float,
    lon: float,
    time_indices: List[int]
) -> Dict[str, List]:
    """
    Extract atmospheric data for a single point from GFS dataset.

    Returns dict with:
        - temperature: List of temperatures in Fahrenheit
        - wind_speed: List of wind speeds in mph
        - wind_direction: List of wind directions in degrees
        - wind_gust: List of wind gusts in mph (if available)
        - pressure: List of pressures in inHg
        - cloud_cover: List of cloud cover percentages
        - precip_rate: List of precipitation rates in mm/hr
    """
    # Find nearest grid point
    lat_idx = np.abs(ds.lat.values - lat).argmin()
    lon_idx = np.abs(ds.lon.values - lon).argmin()

    result = {
        "temperature": [],
        "wind_speed": [],
        "wind_direction": [],
        "wind_gust": [],
        "pressure": [],
        "cloud_cover": [],
        "precip_rate": [],
    }

    # OPTIMIZED: Extract ALL variables for ALL time indices in ONE call
    try:
        # Extract all data at once (single network request for all variables)
        temp_k_arr = ds["tmp2m"][time_indices, lat_idx, lon_idx].values if "tmp2m" in ds.variables else None
        u_wind_arr = ds["ugrd10m"][time_indices, lat_idx, lon_idx].values if "ugrd10m" in ds.variables else None
        v_wind_arr = ds["vgrd10m"][time_indices, lat_idx, lon_idx].values if "vgrd10m" in ds.variables else None
        gust_arr = ds["gustsfc"][time_indices, lat_idx, lon_idx].values if "gustsfc" in ds.variables else None
        pressure_arr = ds["pressfc"][time_indices, lat_idx, lon_idx].values if "pressfc" in ds.variables else None
        cloud_arr = ds["tcdcclm"][time_indices, lat_idx, lon_idx].values if "tcdcclm" in ds.variables else None
        precip_arr = ds["pratesfc"][time_indices, lat_idx, lon_idx].values if "pratesfc" in ds.variables else None

        # Now process the arrays in memory (no more network calls)
        for i in range(len(time_indices)):
            # Temperature (convert Kelvin to Fahrenheit)
            if temp_k_arr is not None:
                temp_c = temp_k_arr[i] - 273.15
                temp_f = celsius_to_fahrenheit(temp_c)
                result["temperature"].append(temp_f)
            else:
                result["temperature"].append(None)

            # Wind components
            if u_wind_arr is not None and v_wind_arr is not None:
                u_wind = float(u_wind_arr[i])
                v_wind = float(v_wind_arr[i])

                # Calculate wind speed and direction
                wind_speed_ms = np.sqrt(u_wind**2 + v_wind**2)
                wind_speed = mps_to_mph(wind_speed_ms)
                result["wind_speed"].append(wind_speed)

                # Wind direction (meteorological convention: direction FROM which wind blows)
                wind_dir = (np.degrees(np.arctan2(-u_wind, -v_wind)) + 360) % 360
                result["wind_direction"].append(wind_dir)
            else:
                result["wind_speed"].append(None)
                result["wind_direction"].append(None)

            # Wind gust (if available)
            if gust_arr is not None:
                gust_ms = float(gust_arr[i])
                gust_mph = mps_to_mph(gust_ms)
                result["wind_gust"].append(gust_mph)
            else:
                # Estimate gust as 1.4x wind speed if not available
                if result["wind_speed"][-1] is not None:
                    result["wind_gust"].append(result["wind_speed"][-1] * 1.4)
                else:
                    result["wind_gust"].append(None)

            # Surface pressure (convert Pa to inHg)
            if pressure_arr is not None:
                pressure_pa = float(pressure_arr[i])
                pressure_inhg = pa_to_inhg(pressure_pa)
                result["pressure"].append(pressure_inhg)
            else:
                result["pressure"].append(None)

            # Cloud cover
            if cloud_arr is not None:
                cloud_pct = float(cloud_arr[i])
                result["cloud_cover"].append(cloud_pct)
            else:
                result["cloud_cover"].append(None)

            # Precipitation rate (convert kg/m²/s to mm/hr)
            if precip_arr is not None:
                precip_kgm2s = float(precip_arr[i])
                precip_mmhr = precip_kgm2s * 3600  # Convert to mm/hr
                result["precip_rate"].append(precip_mmhr)
            else:
                result["precip_rate"].append(None)

    except Exception as e:
        logger.error(f"   Error extracting atmospheric data: {e}")
        # Return empty results on error
        for _ in time_indices:
            result["temperature"].append(None)
            result["wind_speed"].append(None)
            result["wind_direction"].append(None)
            result["wind_gust"].append(None)
            result["pressure"].append(None)
            result["cloud_cover"].append(None)
            result["precip_rate"].append(None)

    return result


def get_gfs_atmospheric_supplement_data(beaches: List[Dict], existing_records: List[Dict]) -> List[Dict]:
    """
    Supplement existing forecast records with GFS Atmospheric data.
    Fills: temperature, weather, wind_speed_mph, wind_direction_deg, wind_gust_mph, pressure_inhg

    Args:
        beaches: List of beach dicts with id, LATITUDE, LONGITUDE
        existing_records: List of forecast records with timestamps

    Returns:
        Updated records with GFS atmospheric data filled in
    """
    start_time = time.time()
    logger.info("   GFS Atmospheric: fetching weather data...")

    # Get GFS Atmospheric dataset
    gfs_url = get_gfs_atmospheric_dataset_url()
    if not gfs_url:
        logger.error("   GFS Atmospheric: No dataset URL found")
        return existing_records

    ds = load_gfs_atmospheric_dataset(gfs_url)
    if not ds:
        logger.error("   GFS Atmospheric: Failed to load dataset")
        return existing_records

    if not validate_gfs_atmospheric_dataset(ds):
        ds.close()
        return existing_records

    # Get time range from dataset
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    # Log first few GFS times to verify they're in UTC
    # logger.info(f"   GFS dataset time range: {time_vals_full[0]} to {time_vals_full[-1]} (UTC)")
    # logger.info(f"   First 5 GFS times: {time_vals_full[:5].tolist()}")

    # Store GFS data boundaries for filtering
    gfs_start_time = time_vals_full[0]
    gfs_end_time = time_vals_full[-1]
    # logger.info(f"   GFS data available from {gfs_start_time} to {gfs_end_time}")

    # Build mapping of timestamps by beach
    pacific = pytz.timezone("America/Los_Angeles")
    needed_by_beach = {}

    # Filter records to only those within GFS data range
    skipped_before_range = 0
    skipped_after_range = 0

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_iso = rec.get("timestamp")
        if not bid or not ts_iso:
            continue

        # Parse timestamp and check if it's within GFS range
        try:
            ts_dt = datetime.fromisoformat(ts_iso)
            if ts_dt.tzinfo is None:
                ts_dt = pacific.localize(ts_dt)
            ts_utc = pd.Timestamp(ts_dt).tz_convert("UTC")

            # Skip timestamps outside GFS data range
            if ts_utc < gfs_start_time:
                skipped_before_range += 1
                continue
            if ts_utc > gfs_end_time:
                skipped_after_range += 1
                continue

        except Exception as e:
            logger.debug(f"   Error parsing timestamp {ts_iso}: {e}")
            continue

        if bid not in needed_by_beach:
            needed_by_beach[bid] = set()
        needed_by_beach[bid].add(ts_iso)

    # if skipped_before_range > 0 or skipped_after_range > 0:
    #     logger.info(
    #         f"   Filtered timestamps outside GFS range:\n"
    #         f"      Before GFS start ({gfs_start_time}): {skipped_before_range}\n"
    #         f"      After GFS end ({gfs_end_time}): {skipped_after_range}"
    #     )

    if not needed_by_beach:
        logger.info("   GFS Atmospheric: no records to process")
        ds.close()
        return existing_records

    # Build index for quick updates
    updated_records = list(existing_records)
    key_to_index = {f"{r['beach_id']}_{r['timestamp']}": idx for idx, r in enumerate(updated_records)}

    # Group beaches by location (cache nearby beaches)
    from collections import defaultdict
    location_groups = defaultdict(list)

    for beach in beaches:
        if beach["id"] not in needed_by_beach:
            continue

        # Round to 0.25 degrees (GFS grid resolution)
        # This creates ~83 location groups for 1336 beaches
        # Each location group will have ~16 beaches on average
        lat_key = round(beach["LATITUDE"] * 4) / 4
        lon_key = round(beach["LONGITUDE"] * 4) / 4
        cache_key = f"{lat_key},{lon_key}"
        location_groups[cache_key].append(beach)

    logger.info(f"   GFS Atmospheric: processing {len(location_groups)} unique locations for {len(needed_by_beach)} beaches...")

    filled_count = 0
    processed_locations = 0
    total_locations = len(location_groups)

    for cache_key, group_beaches in location_groups.items():
        processed_locations += 1

        # Rate limit BEFORE processing EVERY location (including first) to avoid hitting NOAA limits
        # NOAA is extremely strict - we must delay even before the first request
        logger.info(f"   GFS Atmospheric: waiting {NOAA_ATMOSPHERIC_BATCH_DELAY}s before location {processed_locations}/{total_locations}...")
        time.sleep(NOAA_ATMOSPHERIC_BATCH_DELAY)

        if processed_locations % 10 == 0:
            logger.info(f"   GFS Atmospheric: {processed_locations}/{total_locations} locations processed")

        # Use first beach in group for coordinates
        representative_beach = group_beaches[0]
        lat = representative_beach["LATITUDE"]
        lon = representative_beach["LONGITUDE"]

        # Get all timestamps needed for this location group
        all_timestamps_needed = set()
        for beach in group_beaches:
            all_timestamps_needed.update(needed_by_beach[beach["id"]])

        # Map timestamps to GFS time indices
        # Note: Timestamps from database are already aligned to 3-hour Pacific intervals
        # but may be stored in UTC format
        timestamp_to_index = {}
        sample_logged = False  # Only log first few samples to avoid log spam
        for ts_iso in all_timestamps_needed:
            try:
                ts_dt = datetime.fromisoformat(ts_iso)
                original_tz = ts_dt.tzinfo

                # If timestamp has no timezone, assume Pacific
                if ts_dt.tzinfo is None:
                    ts_dt = pacific.localize(ts_dt)

                # Convert to UTC for matching GFS data
                # Timestamps are already aligned by noaa_handler.py, just need to match them
                ts_utc = pd.Timestamp(ts_dt).tz_convert("UTC") if ts_dt.tzinfo else pd.Timestamp(ts_dt).tz_localize("UTC")

                # Find closest GFS time index
                time_diffs = np.abs(time_vals_full - ts_utc)
                closest_idx = time_diffs.argmin()

                # Debug logging for timestamp mapping (sample only to avoid spam)
                # if not sample_logged:
                #     gfs_time_at_idx = time_vals_full[closest_idx]
                #     time_diff_hours = time_diffs[closest_idx] / pd.Timedelta(hours=1)
                #     pacific_time = ts_utc.tz_convert("America/Los_Angeles")
                #     logger.info(
                #         f"   SAMPLE timestamp mapping:\n"
                #         f"      Database timestamp: {ts_iso}\n"
                #         f"      Parsed as: {ts_dt} (tz: {original_tz})\n"
                #         f"      As UTC: {ts_utc}\n"
                #         f"      As Pacific: {pacific_time}\n"
                #         f"      Matched GFS time (UTC): {gfs_time_at_idx}\n"
                #         f"      Time difference: {time_diff_hours:.2f} hours"
                #     )
                #     sample_logged = True

                # Warn if time difference is large (but only if within GFS range)
                # gfs_time_at_idx = time_vals_full[closest_idx]
                # time_diff_hours = time_diffs[closest_idx] / pd.Timedelta(hours=1)

                # Only warn if timestamp is within GFS range but still far from nearest point
                # (timestamps outside range are expected to have large offsets)
                # if time_diff_hours > 1.5 and ts_utc >= gfs_start_time and ts_utc <= gfs_end_time:
                #     logger.warning(
                #         f"   Unexpected time offset (within GFS range):\n"
                #         f"      Database timestamp: {ts_iso}\n"
                #         f"      As UTC: {ts_utc}\n"
                #         f"      Matched GFS time: {gfs_time_at_idx}\n"
                #         f"      Difference: {time_diff_hours:.2f} hours"
                #     )

                # Only use if within 3 hours
                if time_diffs[closest_idx] <= pd.Timedelta(hours=3):
                    timestamp_to_index[ts_iso] = closest_idx
            except Exception as e:
                logger.debug(f"   Error processing timestamp {ts_iso}: {e}")
                continue

        if not timestamp_to_index:
            continue

        # Extract data for this location
        time_indices = list(set(timestamp_to_index.values()))

        try:
            enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_REQUEST_DELAY)
            atmospheric_data = extract_gfs_atmospheric_point(ds, lat, lon, time_indices)
        except Exception as e:
            logger.debug(f"   Error extracting atmospheric data for {cache_key}: {e}")
            continue

        # Map time indices back to data
        index_to_data = {}
        for i, time_idx in enumerate(time_indices):
            index_to_data[time_idx] = {
                "temperature": atmospheric_data["temperature"][i],
                "wind_speed": atmospheric_data["wind_speed"][i],
                "wind_direction": atmospheric_data["wind_direction"][i],
                "wind_gust": atmospheric_data["wind_gust"][i],
                "pressure": atmospheric_data["pressure"][i],
                "cloud_cover": atmospheric_data["cloud_cover"][i],
                "precip_rate": atmospheric_data["precip_rate"][i],
            }

        # Update records for all beaches in this group
        skipped_no_timestamp_match = 0
        skipped_no_data = 0
        skipped_no_record = 0

        for beach in group_beaches:
            bid = beach["id"]

            for ts_iso in needed_by_beach[bid]:
                if ts_iso not in timestamp_to_index:
                    skipped_no_timestamp_match += 1
                    continue

                time_idx = timestamp_to_index[ts_iso]
                if time_idx not in index_to_data:
                    skipped_no_data += 1
                    continue

                data = index_to_data[time_idx]
                key = f"{bid}_{ts_iso}"
                rec_idx = key_to_index.get(key)

                if rec_idx is None:
                    skipped_no_record += 1
                    continue

                rec = updated_records[rec_idx]

                # Fill temperature
                if data["temperature"] is not None:
                    rec["temperature"] = safe_float(data["temperature"])
                    filled_count += 1

                # Fill weather code from cloud cover, precipitation, and temperature
                weather_code = derive_weather_code(
                    data["cloud_cover"],
                    data["precip_rate"],
                    data["temperature"]
                )
                rec["weather"] = safe_int(weather_code)
                filled_count += 1

                # Fill wind speed
                if data["wind_speed"] is not None:
                    rec["wind_speed_mph"] = safe_float(data["wind_speed"])
                    filled_count += 1

                # Fill wind direction
                if data["wind_direction"] is not None:
                    rec["wind_direction_deg"] = safe_float(data["wind_direction"])
                    filled_count += 1

                # Fill wind gust
                if data["wind_gust"] is not None:
                    rec["wind_gust_mph"] = safe_float(data["wind_gust"])
                    filled_count += 1

                # Fill pressure
                if data["pressure"] is not None:
                    rec["pressure_inhg"] = safe_float(data["pressure"])
                    filled_count += 1

        # Log skip reasons for this location group
        # if skipped_no_timestamp_match > 0 or skipped_no_data > 0 or skipped_no_record > 0:
        #     logger.debug(
        #         f"   Location {cache_key} skipped records:\n"
        #         f"      No timestamp match: {skipped_no_timestamp_match}\n"
        #         f"      No data at index: {skipped_no_data}\n"
        #         f"      Record not found: {skipped_no_record}"
        #     )

        # Note: Rate limiting now happens at the START of each loop iteration
        # This ensures we never exceed NOAA's rate limits

    ds.close()

    elapsed_time = time.time() - start_time
    logger.info(f"   GFS Atmospheric: filled {filled_count} field values")
    logger.info(f"   GFS Atmospheric: processed {processed_locations} unique locations")
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

        if not validate_gfs_atmospheric_dataset(ds):
            ds.close()
            return False

        ds.close()
        logger.info("GFS Atmospheric dataset connection successful")
        return True

    except Exception as e:
        logger.error(f"GFS Atmospheric connection test failed: {e}")
        return False
