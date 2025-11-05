#!/usr/bin/env python3
"""
Open-Meteo API integration for Hybrid Surf Database Update Script
Supplement mode: fills ONLY missing fields on top of NOAA rows, aligned by (beach_id, timestamp).

Fills these fields if they are None:
  - weather (code)
  - water_temp_f
  - wind_speed_mph (paired with wind_gust_mph)
  - wind_gust_mph (paired with wind_speed_mph)
"""

import time
import math
import numpy as np
import bisect
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
    chunk_iter, normalize_surf_range
)

from swell_ranking import (
    calculate_wave_energy_kj, get_surf_height_range
)

def nearest_valid_value(series, index):
    """Return the closest non-null/non-NaN entry around the given index."""
    if series is None:
        return None
    try:
        values = np.asarray(series)
    except Exception:
        values = series
    try:
        n = len(values)
    except TypeError:
        return None
    if n == 0:
        return None

    def _value_at(pos):
        if pos < 0 or pos >= n:
            return None
        try:
            val = values[pos]
        except Exception:
            return None
        if np.ma.is_masked(val):
            return None
        if isinstance(val, np.ma.MaskedArray):
            val = val.data
        if isinstance(val, np.ndarray):
            if val.size != 1:
                return None
            val = val.item()
        if isinstance(val, np.generic):
            val = val.item()
        if val is None:
            return None
        try:
            if np.isnan(val):
                return None
        except Exception:
            pass
        return val

    current = _value_at(index)
    if current is not None:
        return current
    for offset in range(1, n):
        left = index - offset
        if left >= 0:
            val = _value_at(left)
            if val is not None:
                return val
        right = index + offset
        if right < n:
            val = _value_at(right)
            if val is not None:
                return val
    return None

# Initialize Open-Meteo client with caching and retry
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# ---- Target fields we will fill if NULL in the existing record ----
TARGET_FIELDS = (
    "weather",
    "water_temp_f",
    "wind_speed_mph",
    "wind_gust_mph",
)


def _haversine_distance(lat1, lon1, lat2, lon2):
    """Compute great-circle distance between two points (km)."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    try:
        lat1_rad = math.radians(float(lat1))
        lon1_rad = math.radians(float(lon1))
        lat2_rad = math.radians(float(lat2))
        lon2_rad = math.radians(float(lon2))
    except (TypeError, ValueError):
        return None
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(max(0.0, min(1.0, a))))
    return 6371.0 * c


FIELDS_FOR_NEIGHBOR_FILL = (
    "weather",
)



def _fill_weather_from_nearby_time(records, beach_meta, max_hours=6, time_weight_km=20.0):
    """Fill missing weather codes by borrowing from nearby beaches at nearby hours."""
    if not records:
        return records

    donors = []
    donor_times = []
    for rec in records:
        weather = rec.get('weather')
        bid = rec.get('beach_id')
        ts = rec.get('timestamp')
        meta = beach_meta.get(bid)
        if weather is None or not meta or meta[1] is None or meta[2] is None or not ts:
            continue
        try:
            ts_obj = pd.Timestamp(ts)
        except Exception:
            continue
        donors.append((ts_obj, meta[1], meta[2], weather))
        donor_times.append(ts_obj.value)

    if not donors:
        return records

    donors_sorted = sorted(zip(donor_times, donors), key=lambda x: x[0])
    donor_times_sorted = [time for time, _ in donors_sorted]
    donors_data = [data for _, data in donors_sorted]

    max_delta_ns = max_hours * 3600 * 1_000_000_000

    filled = 0
    for rec in records:
        if rec.get('weather') is not None:
            continue
        bid = rec.get('beach_id')
        ts = rec.get('timestamp')
        meta = beach_meta.get(bid)
        if not ts or not meta or meta[1] is None or meta[2] is None:
            continue
        try:
            target_ts = pd.Timestamp(ts)
        except Exception:
            continue
        target_val = target_ts.value
        idx = bisect.bisect_left(donor_times_sorted, target_val)

        best_val = None
        best_cost = None

        left = idx - 1
        right = idx
        while True:
            progressed = False
            if left >= 0:
                delta = abs(donor_times_sorted[left] - target_val)
                if delta <= max_delta_ns:
                    ts_obj, lat, lon, weather = donors_data[left]
                    dist = _haversine_distance(meta[1], meta[2], lat, lon)
                    if dist is not None:
                        cost = dist + (delta / 1_000_000_000 / 3600.0) * time_weight_km
                        if best_cost is None or cost < best_cost:
                            best_cost = cost
                            best_val = weather
                    left -= 1
                    progressed = True
                else:
                    left = -1
            if right < len(donors_data):
                delta = abs(donor_times_sorted[right] - target_val)
                if delta <= max_delta_ns:
                    ts_obj, lat, lon, weather = donors_data[right]
                    dist = _haversine_distance(meta[1], meta[2], lat, lon)
                    if dist is not None:
                        cost = dist + (delta / 1_000_000_000 / 3600.0) * time_weight_km
                        if best_cost is None or cost < best_cost:
                            best_cost = cost
                            best_val = weather
                    right += 1
                    progressed = True
                else:
                    right = len(donors_data)
            if not progressed:
                break

        if best_val is not None:
            rec['weather'] = best_val
            filled += 1

    if filled:
        logger.info(f"   Open-Meteo supplement: filled {filled} weather codes using cross-hour neighbors")
    return records

def _fill_missing_fields_from_neighbors(records, beach_meta, fields=FIELDS_FOR_NEIGHBOR_FILL):
    """Fill missing fields using nearest neighbor records at the same timestamp."""
    if not records:
        return records

    grouped = defaultdict(list)
    for idx, rec in enumerate(records):
        ts = rec.get("timestamp")
        bid = rec.get("beach_id")
        if not ts or bid not in beach_meta:
            continue
        grouped[ts].append(idx)

    filled_counts = {field: 0 for field in fields}

    for ts, indices in grouped.items():
        per_record_meta = []
        for idx in indices:
            rec = records[idx]
            bid = rec.get("beach_id")
            meta = beach_meta.get(bid)
            if not meta or meta[1] is None or meta[2] is None:
                per_record_meta.append(None)
            else:
                per_record_meta.append((meta[1], meta[2]))

        for field in fields:
            available = []
            missing = []
            for local_idx, record_idx in enumerate(indices):
                meta = per_record_meta[local_idx]
                if meta is None:
                    continue
                lat, lon = meta
                val = records[record_idx].get(field)
                if val is not None:
                    available.append((lat, lon, val))
                else:
                    missing.append((record_idx, lat, lon))

            if not available or not missing:
                continue

            for record_idx, lat, lon in missing:
                best_val = None
                best_dist = None
                for av_lat, av_lon, val in available:
                    distance = _haversine_distance(lat, lon, av_lat, av_lon)
                    if distance is None:
                        continue
                    if best_dist is None or distance < best_dist:
                        best_dist = distance
                        best_val = val
                if best_val is not None:
                    records[record_idx][field] = best_val
                    filled_counts[field] += 1

    total_filled = sum(filled_counts.values())
    if total_filled:
        summary = ", ".join(f"{field}: {count}" for field, count in filled_counts.items() if count)
        logger.info(f"   Open-Meteo supplement: filled {total_filled} fields via nearest neighbors ({summary})")
    return records

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
    Caps the range to stay within Open-Meteo's available data range (typically ~7 days historical to 10 days future).
    """
    now_local = pd.Timestamp.now(tz=lo_ts.tz)

    # Open-Meteo free tier limits (being conservative):
    # - Historical: up to ~7 days back (to be safe)
    # - Future: up to 10 days ahead (Open-Meteo free tier typically provides up to 10 days)
    min_allowed_date = now_local.normalize() - pd.Timedelta(days=7)
    max_allowed_date = now_local.normalize() + pd.Timedelta(days=10)

    logger.info(f"   Open-Meteo: now_local={now_local}, requested range: {lo_ts} to {hi_ts}")
    logger.info(f"   Open-Meteo: allowed range: {min_allowed_date} to {max_allowed_date}")

    # Clamp start date to not go too far back
    if lo_ts < min_allowed_date:
        logger.warning(f"   Open-Meteo: requested start {lo_ts} is too far back, capping to {min_allowed_date}")
        lo_ts = min_allowed_date

    # Clamp end date to not go too far forward
    if hi_ts > max_allowed_date:
        logger.warning(f"   Open-Meteo: requested end {hi_ts} exceeds 10-day forecast limit, capping to {max_allowed_date}")
        hi_ts = max_allowed_date

    # Use floor to avoid going past limits (don't add extra padding)
    start_date_ts = lo_ts.floor("D")
    end_date_ts = hi_ts.floor("D")

    # Ensure end_date is not before start_date
    if end_date_ts < start_date_ts:
        logger.warning(f"   Open-Meteo: adjusted date range resulted in end before start, using start date for both")
        end_date_ts = start_date_ts

    start_date = start_date_ts.strftime("%Y-%m-%d")
    end_date = end_date_ts.strftime("%Y-%m-%d")

    logger.info(f"   Open-Meteo: final date range {start_date} to {end_date}")
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

        # 4) WEATHER call: weather_code, wind speed, and wind gust (local timezone)
        weather_params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ["weather_code", "windspeed_10m", "windgusts_10m"],
            "timezone": "America/Los_Angeles",
            "start_date": start_date,
            "end_date": end_date
        }
        wrs = api_request_with_retry(openmeteo.weather_api, OPENMETEO_WEATHER_URL, params=weather_params)

        logger.info("      Waiting briefly before Marine call…")
        safe_openmeteo_delay()

        # 5) MARINE call: sea surface temp only (local timezone)
        # Using forecast_days instead of start_date/end_date for better data coverage
        marine_params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ["sea_surface_temperature"],
            "timezone": "America/Los_Angeles",
            "forecast_days": 16
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

                # Construct local timestamps for BOTH weather and marine (they may differ)
                weather_timestamps = pd.to_datetime(
                    range(wh.Time(), wh.TimeEnd(), wh.Interval()),
                    unit="s", utc=True
                ).tz_convert("America/Los_Angeles")

                marine_timestamps = pd.to_datetime(
                    range(mh.Time(), mh.TimeEnd(), mh.Interval()),
                    unit="s", utc=True
                ).tz_convert("America/Los_Angeles")

                weather_code      = wh.Variables(0).ValuesAsNumpy()
                wind_speed_kph    = wh.Variables(1).ValuesAsNumpy()
                wind_gust_kph     = wh.Variables(2).ValuesAsNumpy()

                water_temp_c      = mh.Variables(0).ValuesAsNumpy()

                # Process WEATHER data with weather timestamps
                for j, ts_local in enumerate(weather_timestamps):
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

                    # Convert to UTC for matching database timestamps
                    clean_utc_time = clean_local_time.tz_convert('UTC')
                    ts_iso = clean_utc_time.isoformat()

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

                    # Prepare candidate values from WEATHER API
                    weather_val = nearest_valid_value(weather_code, j)
                    wind_speed_val = nearest_valid_value(wind_speed_kph, j)
                    wind_gust_val = nearest_valid_value(wind_gust_kph, j)

                    # Convert wind speeds to mph
                    wind_speed_mph_val = safe_float(kph_to_mph(wind_speed_val)) if wind_speed_val is not None else None
                    wind_gust_mph_val = safe_float(kph_to_mph(wind_gust_val)) if wind_gust_val is not None else None

                    # Ensure gusts are never lower than speed
                    if wind_speed_mph_val is not None and wind_gust_mph_val is not None:
                        if wind_gust_mph_val < wind_speed_mph_val:
                            wind_gust_mph_val = wind_speed_mph_val

                    candidates = {
                        "weather": safe_int(weather_val) if weather_val is not None else None,
                        "wind_speed_mph": wind_speed_mph_val,
                        "wind_gust_mph": wind_gust_mph_val,
                    }

                    # PAIRED FILLING LOGIC: wind_speed_mph and wind_gust_mph must be filled together
                    # If EITHER field is missing, fill BOTH from Open Meteo to ensure they're from same source
                    wind_speed_missing = "wind_speed_mph" in missing
                    wind_gust_missing = "wind_gust_mph" in missing

                    if wind_speed_missing or wind_gust_missing:
                        if wind_speed_mph_val is not None and wind_gust_mph_val is not None:
                            # Fill BOTH fields to ensure paired data from Open Meteo
                            rec["wind_speed_mph"] = wind_speed_mph_val
                            rec["wind_gust_mph"] = wind_gust_mph_val

                    # Fill non-wind fields independently
                    for f in missing:
                        if f not in ("wind_speed_mph", "wind_gust_mph"):
                            val = candidates.get(f, None)
                            if val is not None:
                                rec[f] = val

                # Process MARINE data with marine timestamps (separate loop for water_temp, tides, etc.)
                for j, ts_local in enumerate(marine_timestamps):
                    ts_local_aware = pd.Timestamp(ts_local)

                    # Apply same alignment logic
                    local_hour = ts_local_aware.hour
                    pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
                    closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))
                    clean_local_time = ts_local_aware.replace(
                        hour=closest_interval,
                        minute=0,
                        second=0,
                        microsecond=0
                    )

                    # Convert to UTC for matching database timestamps
                    clean_utc_time = clean_local_time.tz_convert('UTC')
                    ts_iso = clean_utc_time.isoformat()

                    if ts_iso not in needed_by_beach[bid]:
                        continue

                    key = f"{bid}_{ts_iso}"
                    if key not in missing_fields_by_key:
                        continue

                    missing = missing_fields_by_key[key]
                    idx = key_to_index.get(key)
                    if idx is None:
                        continue
                    rec = updated_records[idx]

                    # Prepare candidate values from MARINE API
                    water_temp_val = nearest_valid_value(water_temp_c, j)

                    marine_candidates = {
                        "water_temp_f": safe_float(celsius_to_fahrenheit(water_temp_val)) if water_temp_val is not None else None,
                    }

                    # Only set the fields that are actually missing in this record
                    for f in missing:
                        val = marine_candidates.get(f, None)
                        if val is not None:
                            rec[f] = val

            except Exception as e:
                logger.error(f"      Supplement processing failed for beach_id={bid}: {e}")

    # 7) Done — only missing fields were filled, keys untouched, alignment preserved
    updated_records = _fill_missing_fields_from_neighbors(updated_records, beach_meta)
    updated_records = _fill_weather_from_nearby_time(updated_records, beach_meta)
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
