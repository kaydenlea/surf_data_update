#!/usr/bin/env python3
"""
Open-Meteo API integration for Hybrid Surf Database Update Script
Supplement mode: fills ONLY missing fields on top of NOAA rows, aligned by (beach_id, timestamp).

Fills these fields if they are None:
  - temperature (F)
  - weather (code)
  - wind_speed_mph
  - wind_gust_mph
  - water_temp_f
  - pressure_inhg
  - tide_level_ft  (applies +TIDE_ADJUSTMENT_FT)
"""

import time
import pandas as pd
from collections import defaultdict

import openmeteo_requests
import requests_cache
from retry_requests import retry

from config import (
    logger, DAYS_FORECAST, BATCH_SIZE, OPENMETEO_WEATHER_URL, OPENMETEO_MARINE_URL,
    OPENMETEO_BATCH_DELAY, OPENMETEO_RETRY_DELAY, TIDE_ADJUSTMENT_FT
)
from utils import (
    api_request_with_retry, safe_openmeteo_delay, safe_float, safe_int,
    celsius_to_fahrenheit, kph_to_mph, meters_to_feet, hpa_to_inhg,
    chunk_iter
)

# Initialize Open-Meteo client with caching and retry
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# ---- Target fields we will fill if NULL in the existing record ----
TARGET_FIELDS = (
    "temperature",
    "weather",
    "wind_speed_mph",
    "wind_gust_mph",
    "water_temp_f",
    "pressure_inhg",
    "tide_level_ft",
)

def _collect_needed_hours(existing_records):
    """
    Build:
      - needed_by_beach: {beach_id: set(local_iso_timestamps)}
      - missing_fields_by_key: {f"{beach_id}_{ts}": set(field_names_that_are_None)}
      - overall min/max local times per beach to shrink API window
    Assumes record['timestamp'] is tz-aware ISO string in America/Los_Angeles (matches NOAA writer).
    """
    needed_by_beach = defaultdict(set)
    missing_fields_by_key = {}
    window_by_beach = {}  # beach_id -> (min_ts_local, max_ts_local)

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_iso = rec.get("timestamp")  # local ISO (America/Los_Angeles)
        if not bid or not ts_iso:
            continue

        # Only consider if ANY of the target fields are missing
        missing = {f for f in TARGET_FIELDS if rec.get(f) in (None,)}
        if not missing:
            continue  # nothing to fill for this hour

        needed_by_beach[bid].add(ts_iso)
        missing_fields_by_key[f"{bid}_{ts_iso}"] = missing

        # Track window per beach (local time)
        ts = pd.Timestamp(ts_iso)
        if bid not in window_by_beach:
            window_by_beach[bid] = (ts, ts)
        else:
            lo, hi = window_by_beach[bid]
            window_by_beach[bid] = (min(lo, ts), max(hi, ts))

    return needed_by_beach, missing_fields_by_key, window_by_beach

def _derive_local_date_range(lo_ts, hi_ts):
    """
    Given local tz-aware pandas Timestamps, return (start_date_str, end_date_str) for Open-Meteo.
    Open-Meteo expects date strings (YYYY-MM-DD) inclusive range for hourly queries.
    """
    # add small padding to be safe with hourly boundaries
    start_date = lo_ts.floor("D").strftime("%Y-%m-%d")
    end_date = (hi_ts.ceil("D")).strftime("%Y-%m-%d")
    return start_date, end_date

def get_openmeteo_supplement_data(beaches, existing_records):
    """
    Supplement missing fields using Open-Meteo, aligned to NOAA rows.
    Only hours present in `existing_records` (that are missing target fields) are requested/filled.
    FIXED: Aligns timestamps to clean 3-hour intervals to match NOAA handler.
    """
    logger.info("   Open-Meteo supplement: aligning to NOAA timestamps and filling only missing fields…")

    # 1) Determine exactly which (beach_id, timestamp_local_iso) we need, and for which fields.
    needed_by_beach, missing_fields_by_key, window_by_beach = _collect_needed_hours(existing_records)

    total_needed_pairs = sum(len(v) for v in needed_by_beach.values())
    if total_needed_pairs == 0:
        logger.info("   Open-Meteo supplement: nothing to fill (no missing fields).")
        return existing_records

    # Map beach_id -> (Name, LATITUDE, LONGITUDE) from provided beaches list
    beach_meta = {b["id"]: (b.get("Name"), b["LATITUDE"], b["LONGITUDE"]) for b in beaches}

    # 2) Build a worklist of beaches that actually have missing fields
    target_beaches = [b for b in beaches if b["id"] in needed_by_beach]
    logger.info(f"   Will supplement {len(target_beaches)} beaches, {total_needed_pairs} hour-rows need fill.")

    # 3) Process in batches, deriving a local date window per batch that covers only what's needed.
    updated_records = list(existing_records)  # copy to return
    key_to_index = {f"{r['beach_id']}_{r['timestamp']}": idx for idx, r in enumerate(updated_records)}

    batch_count = 0
    total_batches = len(list(chunk_iter(target_beaches, BATCH_SIZE)))

    for batch in chunk_iter(target_beaches, BATCH_SIZE):
        batch_count += 1
        ids = [b["id"] for b in batch]
        lats = [b["LATITUDE"] for b in batch]
        lons = [b["LONGITUDE"] for b in batch]

        # Compute a combined local window for this batch (min of mins, max of maxes)
        los, his = [], []
        for bid in ids:
            lo, hi = window_by_beach[bid]
            los.append(lo)
            his.append(hi)
        batch_lo = min(los)
        batch_hi = max(his)

        # Convert local window to YYYY-MM-DD for Open-Meteo. OM will return local-hour timestamps.
        start_date, end_date = _derive_local_date_range(batch_lo, batch_hi)
        logger.info(f"   Batch {batch_count}/{total_batches}: {len(batch)} beaches, window {start_date} → {end_date}")

        # Respect batch pacing
        if batch_count > 1:
            logger.info(f"   Waiting {OPENMETEO_BATCH_DELAY}s between Open-Meteo batches…")
            time.sleep(OPENMETEO_BATCH_DELAY)

        # 4) WEATHER call: gust, temp, pressure, weather_code (local timezone)
        weather_params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ["windgusts_10m", "temperature_2m", "pressure_msl", "weather_code", "windspeed_10m"],
            "timezone": "America/Los_Angeles",
            "start_date": start_date,
            "end_date": end_date
        }
        wrs = api_request_with_retry(openmeteo.weather_api, OPENMETEO_WEATHER_URL, params=weather_params)

        logger.info("      Waiting briefly before Marine call…")
        safe_openmeteo_delay()

        # 5) MARINE call: sea surface temp & tide (local timezone)
        marine_params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ["sea_surface_temperature", "sea_level_height_msl"],
            "timezone": "America/Los_Angeles",
            "start_date": start_date,
            "end_date": end_date
        }
        mrs = api_request_with_retry(openmeteo.weather_api, OPENMETEO_MARINE_URL, params=marine_params)

        if len(wrs) != len(mrs) or len(wrs) != len(batch):
            logger.warning(f"      Response count mismatch in supplement batch {batch_count}; skipping batch.")
            continue

        # 6) Build supplements keyed by (beach_id, local_ts_iso) but ONLY for hours we actually need
        for i, b in enumerate(batch):
            bid = b["id"]
            if bid not in needed_by_beach:
                continue

            try:
                wh = wrs[i].Hourly()
                mh = mrs[i].Hourly()

                # Construct local timestamps (OM returns time ranges in epoch seconds UTC with local tz requested)
                timestamps = pd.to_datetime(
                    range(wh.Time(), wh.TimeEnd(), wh.Interval()),
                    unit="s", utc=True
                ).tz_convert("America/Los_Angeles")

                wind_gust_kph = wh.Variables(0).ValuesAsNumpy()
                temp_c        = wh.Variables(1).ValuesAsNumpy()
                pressure_hpa  = wh.Variables(2).ValuesAsNumpy()
                weather_code  = wh.Variables(3).ValuesAsNumpy()
                wind_speed_kph= wh.Variables(4).ValuesAsNumpy()

                water_temp_c  = mh.Variables(0).ValuesAsNumpy()
                tide_level_m  = mh.Variables(1).ValuesAsNumpy()

                # FIXED: For each hour, align timestamps to clean 3-hour intervals
                for j, ts_local in enumerate(timestamps):
                    ts_local_aware = pd.Timestamp(ts_local)
                    
                    # Apply same alignment logic as NOAA handler
                    local_hour = ts_local_aware.hour
                    pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
                    
                    # Find the closest 3-hour interval
                    closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))
                    
                    # Create clean aligned timestamp
                    clean_local_time = ts_local_aware.replace(
                        hour=closest_interval, 
                        minute=0, 
                        second=0, 
                        microsecond=0
                    )
                    
                    ts_iso = clean_local_time.isoformat()
                    
                    if ts_iso not in needed_by_beach[bid]:
                        continue  # we don't need this hour

                    key = f"{bid}_{ts_iso}"
                    if key not in missing_fields_by_key:
                        continue

                    missing = missing_fields_by_key[key]
                    idx = key_to_index.get(key)
                    if idx is None:
                        continue  # shouldn't happen, but guard anyway
                    rec = updated_records[idx]

                    # Prepare candidate values
                    # Tide: convert meters→feet and add adjustment
                    raw_tide_ft = safe_float(meters_to_feet(tide_level_m[j]))
                    adjusted_tide_ft = (raw_tide_ft + TIDE_ADJUSTMENT_FT) if raw_tide_ft is not None else None

                    candidates = {
                        "temperature":   safe_float(celsius_to_fahrenheit(temp_c[j])),
                        "weather":       safe_int(weather_code[j]),
                        "wind_speed_mph": safe_float(kph_to_mph(wind_speed_kph[j])),
                        "wind_gust_mph": safe_float(kph_to_mph(wind_gust_kph[j])),
                        "water_temp_f":  safe_float(celsius_to_fahrenheit(water_temp_c[j])),
                        "pressure_inhg": safe_float(hpa_to_inhg(pressure_hpa[j])),
                        "tide_level_ft": adjusted_tide_ft,
                    }

                    # Guardrail: ensure gusts are never lower than speed
                    # Determine effective speed/gust considering existing record values
                    existing_speed = rec.get("wind_speed_mph")
                    existing_gust = rec.get("wind_gust_mph")
                    cand_speed = candidates.get("wind_speed_mph")
                    cand_gust = candidates.get("wind_gust_mph")
                    # What speed/gust will end up on the record after this update?
                    final_speed = cand_speed if ("wind_speed_mph" in missing and cand_speed is not None) else existing_speed
                    final_gust  = cand_gust  if ("wind_gust_mph"  in missing and cand_gust  is not None) else existing_gust
                    if final_speed is not None and final_gust is not None and final_gust < final_speed:
                        # Prefer bumping gust up to at least speed
                        if "wind_gust_mph" in missing and cand_gust is not None:
                            candidates["wind_gust_mph"] = final_speed

                    # Only set the fields that are actually missing in this record
                    for f in missing:
                        val = candidates.get(f, None)
                        if val is not None:
                            rec[f] = val

            except Exception as e:
                logger.error(f"      Supplement processing failed for beach_id={bid}: {e}")

    # 7) Done — only missing fields were filled, keys untouched, alignment preserved
    logger.info("   Open-Meteo supplement: fill complete.")
    return updated_records
# ---------------- Optional utilities retained from your original file ----------------

def test_openmeteo_connection():
    """Test Open-Meteo API connectivity."""
    try:
        logger.info("Testing Open-Meteo API connection...")
        test_params = {
            "latitude": [37.7749],  # San Francisco
            "longitude": [-122.4194],
            "hourly": ["temperature_2m"],
            "timezone": "America/Los_Angeles",
            "forecast_days": 1
        }
        response = api_request_with_retry(openmeteo.weather_api, OPENMETEO_WEATHER_URL, params=test_params)
        if response and len(response) > 0:
            logger.info("Open-Meteo API connection successful")
            return True
        else:
            logger.error("Open-Meteo API connection failed - no response")
            return False
    except Exception as e:
        logger.error(f"Open-Meteo API connection test failed: {e}")
        return False

def get_openmeteo_rate_limit_status():
    """Placeholder for future enhancement (OM doesn't return rate-limit headers)."""
    logger.debug("Open-Meteo rate limit status checking not implemented")
    return None
