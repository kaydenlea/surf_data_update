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


def normalize_to_utc_iso(value) -> Optional[str]:
    """
    Convert a datetime or ISO-formatted string to a normalized UTC ISO string.
    Returns None if the value cannot be parsed.
    """
    if value is None:
        return None

    if isinstance(value, str):
        ts_str = value.strip()
        if not ts_str:
            return None
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts_str)
        except ValueError:
            return None
    elif isinstance(value, datetime):
        dt = value
    else:
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)

    return dt.isoformat()

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
    time_indices: List[int],
    lat_idx: int = None,
    lon_idx: int = None
) -> Dict[str, List]:
    """
    Extract atmospheric data for a single point from GFS dataset.

    Args:
        lat, lon: Rounded to 0.25° grid coordinates (for logging/reference only)
        lat_idx, lon_idx: Pre-calculated indices (if provided, skips coordinate lookup)

    Returns dict with:
        - temperature: List of temperatures in Fahrenheit
        - wind_speed: List of wind speeds in mph
        - wind_direction: List of wind directions in degrees
        - wind_gust: List of wind gusts in mph (if available)
        - pressure: List of pressures in inHg
        - cloud_cover: List of cloud cover percentages
        - precip_rate: List of precipitation rates in mm/hr
    """
    result = {
        "temperature": [],
        "wind_speed": [],
        "wind_direction": [],
        "wind_gust": [],
        "pressure": [],
        "cloud_cover": [],
        "precip_rate": [],
    }

    # OPTIMIZED: Use direct OPeNDAP constraint expressions instead of xarray selection
    # This avoids triggering coordinate lookups that cause rate limit issues
    try:
        # Build a direct OPeNDAP-style URL constraint to get exactly what we need
        # Format: variable[time_start:time_end][lat_idx][lon_idx]
        # This is the MOST efficient way - single HTTP request with constraint expression

        # Get the dataset's underlying URL
        import re

        # Calculate lat/lon indices from the rounded coordinates
        # GFS 0.25° grid: lat from 90 to -90, lon from 0 to 359.75
        # lat index: (90 - lat) / 0.25
        # lon index: lon / 0.25 (lon is already 0-360 range)

        # Adjust longitude to 0-360 range if needed
        lon_360 = lon if lon >= 0 else lon + 360

        lat_calc_idx = int(round((90 - lat) / 0.25))
        lon_calc_idx = int(round(lon_360 / 0.25))

        # Extract variables one by one with explicit indexing (avoid .sel() entirely!)
        vars_to_extract = []
        if "tmp2m" in ds.variables:
            vars_to_extract.append("tmp2m")
        if "pressfc" in ds.variables:
            vars_to_extract.append("pressfc")
        if "tcdcclm" in ds.variables:
            vars_to_extract.append("tcdcclm")
        if "pratesfc" in ds.variables:
            vars_to_extract.append("pratesfc")

        # MATCH THE SWELL HANDLER APPROACH EXACTLY
        # Call .isel().values directly for each variable - NO .load() or .compute()!
        # This is how the swell handler does it and it works fine
        logger.info(f"   DEBUG: Extracting data for lat_idx={lat_calc_idx}, lon_idx={lon_calc_idx}, {len(time_indices)} timesteps")
        logger.info(f"   DEBUG: Using direct .isel().values calls (matching swell handler)")

        import time as time_module
        extract_start = time_module.time()

        # Extract each variable separately using direct .isel().values (just like swell handler)
        temp_k_arr = ds["tmp2m"].isel(time=time_indices, lat=lat_calc_idx, lon=lon_calc_idx).values if "tmp2m" in ds.variables else None
        pressure_arr = ds["pressfc"].isel(time=time_indices, lat=lat_calc_idx, lon=lon_calc_idx).values if "pressfc" in ds.variables else None
        cloud_arr = ds["tcdcclm"].isel(time=time_indices, lat=lat_calc_idx, lon=lon_calc_idx).values if "tcdcclm" in ds.variables else None
        precip_arr = ds["pratesfc"].isel(time=time_indices, lat=lat_calc_idx, lon=lon_calc_idx).values if "pratesfc" in ds.variables else None

        extract_time = time_module.time() - extract_start
        logger.info(f"   DEBUG: Variable extraction completed in {extract_time:.2f} seconds")

        # Now process the arrays in memory (no more network calls)
        for i in range(len(time_indices)):
            # Temperature (convert Kelvin to Fahrenheit)
            if temp_k_arr is not None:
                temp_c = temp_k_arr[i] - 273.15
                temp_f = celsius_to_fahrenheit(temp_c)
                result["temperature"].append(temp_f)
            else:
                result["temperature"].append(None)

            # Wind data (not used - comes from GFSwave)
            result["wind_speed"].append(None)
            result["wind_direction"].append(None)
            result["wind_gust"].append(None)

            # Surface pressure (convert Pa to inHg)
            if pressure_arr is not None:
                pressure_pa = float(pressure_arr[i])
                pressure_inhg = pa_to_inhg(pressure_pa)
                result["pressure"].append(pressure_inhg)
            else:
                result["pressure"].append(None)

            # Cloud cover (for weather code)
            if cloud_arr is not None:
                cloud_pct = float(cloud_arr[i])
                result["cloud_cover"].append(cloud_pct)
            else:
                result["cloud_cover"].append(None)

            # Precipitation rate (for weather code, convert kg/m²/s to mm/hr)
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

    # Extract coordinate metadata with rate limiting
    # Use BATCH delay (not REQUEST delay) because coordinate extraction can trigger multiple requests
    logger.info("   Loading GFS time coordinate metadata...")
    enforce_noaa_rate_limit(NOAA_ATMOSPHERIC_BATCH_DELAY)

    # Extract time coordinate (may trigger multiple HTTP range requests on server side)
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    # Log first few GFS times to verify they're in UTC
    logger.info(f"   GFS dataset time range: {time_vals_full[0]} to {time_vals_full[-1]} (UTC)")
    logger.info(f"   First 5 GFS times: {time_vals_full[:5].tolist()}")

    # Store GFS data boundaries for filtering
    gfs_start_time = time_vals_full[0]
    gfs_end_time = time_vals_full[-1]
    # logger.info(f"   GFS data available from {gfs_start_time} to {gfs_end_time}")

    # Build mapping of timestamps by beach
    needed_by_beach = {}

    # Filter records to only those within GFS data range
    skipped_before_range = 0
    skipped_after_range = 0

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_original = rec.get("timestamp")
        ts_iso = normalize_to_utc_iso(ts_original)
        if not bid or not ts_iso:
            continue

        try:
            ts_utc = pd.Timestamp(ts_iso)
            ts_utc = ts_utc.tz_convert("UTC") if ts_utc.tz is not None else ts_utc.tz_localize("UTC")

            # Skip timestamps outside GFS data range
            if ts_utc < gfs_start_time:
                skipped_before_range += 1
                continue
            if ts_utc > gfs_end_time:
                skipped_after_range += 1
                continue

        except Exception as e:
            logger.debug(f"   Error parsing timestamp {ts_original}: {e}")
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
    key_to_index: Dict[str, int] = {}
    for idx, record in enumerate(updated_records):
        bid = record.get("beach_id")
        ts_iso = normalize_to_utc_iso(record.get("timestamp"))
        if not bid or not ts_iso:
            continue
        key_to_index[f"{bid}_{ts_iso}"] = idx

    # Group beaches by location using GFS grid resolution (0.25 degrees)
    # This matches the swell handler approach - no need to load lat/lon arrays!
    # GFS is 0.25° resolution, so round to nearest 0.25° grid point
    from collections import defaultdict
    location_groups = defaultdict(list)

    for beach in beaches:
        if beach["id"] not in needed_by_beach:
            continue

        # Round to nearest 0.25 degree (GFS grid resolution) - same as swell handler approach
        # This avoids loading the full lat/lon arrays which triggers rate limits
        gfs_lat = round(beach["LATITUDE"] * 4) / 4  # Round to 0.25°
        gfs_lon = round(beach["LONGITUDE"] * 4) / 4  # Round to 0.25°
        cache_key = f"{gfs_lat},{gfs_lon}"

        location_groups[cache_key].append({
            **beach,
            "gfs_lat": gfs_lat,
            "gfs_lon": gfs_lon
        })

    logger.info(f"   GFS Atmospheric: processing {len(location_groups)} unique locations for {len(needed_by_beach)} beaches...")

    filled_count = 0
    processed_locations = 0
    total_locations = len(location_groups)

    for cache_key, group_beaches in location_groups.items():
        processed_locations += 1

        logger.info(f"   ===== PROCESSING LOCATION {processed_locations}/{total_locations}: {cache_key} =====")

        if processed_locations % 10 == 0:
            logger.info(f"   GFS Atmospheric: {processed_locations}/{total_locations} locations processed")

        # Use first beach in group for coordinates (all beaches in group share same GFS grid point)
        representative_beach = group_beaches[0]
        lat = representative_beach["gfs_lat"]  # Use exact GFS grid coordinate
        lon = representative_beach["gfs_lon"]  # Use exact GFS grid coordinate
        logger.info(f"   DEBUG: Location coords: lat={lat}, lon={lon}, beaches in group: {len(group_beaches)}")

        # Get all timestamps needed for this location group
        all_timestamps_needed = set()
        for beach in group_beaches:
            all_timestamps_needed.update(needed_by_beach[beach["id"]])

        # Map timestamps to GFS time indices
        # IMPORTANT: Database timestamps are stored in UTC but represent Pacific time intervals
        # (e.g., Pacific noon = UTC 19:00). GFS data is at UTC 3-hour intervals.
        # We need to match based on Pacific time, not UTC time.
        # SHIFT: Apply +6 hour shift to align peak temperature with afternoon (2 PM instead of 8 AM)
        timestamp_to_index = {}
        pacific_tz = pytz.timezone("America/Los_Angeles")
        TIME_SHIFT_HOURS = 6  # Shift GFS times forward by 6 hours for better alignment

        for ts_iso in all_timestamps_needed:
            try:
                ts_utc = pd.Timestamp(ts_iso)
                ts_utc = ts_utc.tz_convert("UTC") if ts_utc.tz is not None else ts_utc.tz_localize("UTC")

                # Convert database UTC timestamp to Pacific time to get the actual local time it represents
                ts_pacific = ts_utc.tz_convert(pacific_tz)

                # Now find the GFS time that corresponds to this same Pacific time
                # Convert all GFS times to Pacific and SHIFT forward by 6 hours
                gfs_times_pacific = time_vals_full.tz_convert(pacific_tz) + pd.Timedelta(hours=TIME_SHIFT_HOURS)
                time_diffs = np.abs(gfs_times_pacific - ts_pacific)
                closest_idx = time_diffs.argmin()

                # Accept matches within 2 hours
                # NOTE: There will typically be a ~1 hour offset because:
                # - Database intervals: 0, 3, 6, 9, 12, 15, 18, 21 Pacific
                # - GFS 18z run has:   11, 14, 17, 20, 23, 2, 5, 8, 11, 14... Pacific
                # This is expected and acceptable - we use the closest available GFS forecast
                if time_diffs[closest_idx] <= pd.Timedelta(hours=2):
                    timestamp_to_index[ts_iso] = closest_idx
            except Exception as e:
                logger.debug(f"   Error processing timestamp {ts_iso}: {e}")
                continue

        if not timestamp_to_index:
            continue

        # Extract data for this location
        time_indices = list(set(timestamp_to_index.values()))
        logger.info(f"   DEBUG: Found {len(time_indices)} unique time indices to extract")

        try:
            logger.info(f"   DEBUG: Starting extract_gfs_atmospheric_point()")
            atmospheric_data = extract_gfs_atmospheric_point(ds, lat, lon, time_indices)
            logger.info(f"   DEBUG: extract_gfs_atmospheric_point() completed successfully")

            # IMPORTANT: Wait AFTER each location to avoid rate limits
            # The .load() call takes ~40 seconds, but we still need to space out requests
            if processed_locations < total_locations:
                logger.info(f"   DEBUG: Waiting {NOAA_ATMOSPHERIC_BATCH_DELAY}s after completing location {processed_locations}")
                time.sleep(NOAA_ATMOSPHERIC_BATCH_DELAY)

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

                # DISABLED: Wind data now comes from NOAA GFSwave (better for coastal conditions)
                # # Fill wind speed
                # if data["wind_speed"] is not None:
                #     rec["wind_speed_mph"] = safe_float(data["wind_speed"])
                #     filled_count += 1

                # # Fill wind direction
                # if data["wind_direction"] is not None:
                #     rec["wind_direction_deg"] = safe_float(data["wind_direction"])
                #     filled_count += 1

                # # Fill wind gust
                # if data["wind_gust"] is not None:
                #     rec["wind_gust_mph"] = safe_float(data["wind_gust"])
                #     filled_count += 1

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
