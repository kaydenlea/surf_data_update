#!/usr/bin/env python3
"""
Debug script to understand timestamp matching between Marine API and database records
"""

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from config import logger
import pytz
from datetime import datetime, timedelta

# Setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Test one grid point
test_lat = 41.833367
test_lon = -124.49994899999999

logger.info(f"Testing Grid 1: Lat={test_lat}, Lon={test_lon}")
logger.info("=" * 80)

# Make marine API request
params = {
    "latitude": test_lat,
    "longitude": test_lon,
    "hourly": ["sea_surface_temperature"],
    "timezone": "America/Los_Angeles",
    "forecast_days": 16
}

try:
    responses = openmeteo.weather_api("https://marine-api.open-meteo.com/v1/marine", params=params)
    response = responses[0]
    hourly = response.Hourly()

    # Get marine timestamps
    marine_timestamps = pd.to_datetime(
        range(hourly.Time(), hourly.TimeEnd(), hourly.Interval()),
        unit="s", utc=True
    ).tz_convert("America/Los_Angeles")

    water_temps = hourly.Variables(0).ValuesAsNumpy()

    logger.info(f"\nMarine API returned {len(marine_timestamps)} timestamps")
    logger.info(f"First timestamp: {marine_timestamps[0]}")
    logger.info(f"Last timestamp: {marine_timestamps[-1]}")
    logger.info(f"Interval: Every {(marine_timestamps[1] - marine_timestamps[0]).total_seconds()/3600:.1f} hours")

    # Now simulate what the code does: align to 3-hour intervals
    pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
    aligned_timestamps = {}

    for j, ts_local in enumerate(marine_timestamps):
        ts_local_aware = pd.Timestamp(ts_local)
        local_hour = ts_local_aware.hour
        closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))

        clean_local_time = ts_local_aware.replace(
            hour=closest_interval,
            minute=0,
            second=0,
            microsecond=0
        )

        ts_iso = clean_local_time.isoformat()

        # Check if water temp is valid
        is_valid = water_temps[j] is not None and water_temps[j] == water_temps[j]  # not NaN

        # Track which marine timestamps map to which aligned timestamp
        if ts_iso not in aligned_timestamps:
            aligned_timestamps[ts_iso] = []

        aligned_timestamps[ts_iso].append({
            "original_hour": ts_local_aware.hour,
            "marine_index": j,
            "has_valid_water_temp": is_valid,
            "water_temp_c": water_temps[j] if is_valid else None
        })

    logger.info(f"\n{len(aligned_timestamps)} unique aligned 3-hour timestamps")
    logger.info(f"Each aligned timestamp has {len(marine_timestamps)/len(aligned_timestamps):.1f} marine timestamps on average")

    # Show first 10 aligned timestamps
    logger.info("\nFirst 10 aligned timestamps and their marine sources:")
    for i, (aligned_ts, sources) in enumerate(sorted(aligned_timestamps.items())[:10]):
        logger.info(f"\n{i+1}. Aligned timestamp: {aligned_ts}")
        logger.info(f"   Maps from {len(sources)} marine timestamp(s):")
        for src in sources:
            valid_str = "VALID" if src["has_valid_water_temp"] else "NaN"
            temp_str = f"{src['water_temp_c']:.1f}°C" if src["has_valid_water_temp"] else "N/A"
            logger.info(f"     - Hour {src['original_hour']:02d}:00 (marine_index={src['marine_index']}) - {valid_str} - {temp_str}")

    # Show how many aligned timestamps have valid water temp
    aligned_with_valid = sum(1 for sources in aligned_timestamps.values()
                            if any(s["has_valid_water_temp"] for s in sources))
    logger.info(f"\n{aligned_with_valid}/{len(aligned_timestamps)} aligned timestamps have at least one valid water temp")

except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
