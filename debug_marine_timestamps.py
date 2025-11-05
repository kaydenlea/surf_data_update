#!/usr/bin/env python3
"""
Debug script to see what marine timestamps we're actually getting from Open Meteo
"""

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from config import logger

# Setup
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Test a grid point that SHOULD have data (Grid 1 - Northern California)
test_coords = [
    {"id": 1, "lat": 41.833367, "lon": -124.49994899999999, "name": "Grid 1 (Northern CA)"},
    {"id": 75, "lat": 34.666699, "lon": -120.833282, "name": "Grid 75 (Southern CA)"},
]

for coord in test_coords:
    logger.info(f"\n{'='*80}")
    logger.info(f"Testing: {coord['name']}")
    logger.info(f"Coordinates: {coord['lat']}, {coord['lon']}")
    logger.info(f"{'='*80}")

    # Make marine API request
    params = {
        "latitude": coord['lat'],
        "longitude": coord['lon'],
        "hourly": ["sea_surface_temperature"],
        "timezone": "America/Los_Angeles",
        "forecast_days": 16
    }

    try:
        responses = openmeteo.weather_api("https://marine-api.open-meteo.com/v1/marine", params=params)
        response = responses[0]
        hourly = response.Hourly()

        # Get timestamps
        timestamps = pd.to_datetime(
            range(hourly.Time(), hourly.TimeEnd(), hourly.Interval()),
            unit="s", utc=True
        ).tz_convert("America/Los_Angeles")

        # Get water temp data
        water_temps = hourly.Variables(0).ValuesAsNumpy()

        logger.info(f"Total timestamps returned: {len(timestamps)}")
        logger.info(f"Date range: {timestamps[0].date()} to {timestamps[-1].date()}")

        # Count valid (non-NaN) values
        valid_count = sum(1 for temp in water_temps if temp is not None and temp == temp)  # check for NaN
        logger.info(f"Valid water temp values: {valid_count}/{len(water_temps)} ({valid_count/len(water_temps)*100:.1f}%)")

        # Show first and last valid values
        first_valid_idx = next((i for i, temp in enumerate(water_temps) if temp is not None and temp == temp), None)
        last_valid_idx = next((i for i in range(len(water_temps)-1, -1, -1) if water_temps[i] is not None and water_temps[i] == water_temps[i]), None)

        if first_valid_idx is not None:
            logger.info(f"First valid: {timestamps[first_valid_idx]} = {water_temps[first_valid_idx]:.1f}°C ({water_temps[first_valid_idx]*9/5+32:.1f}°F)")
        if last_valid_idx is not None:
            logger.info(f"Last valid: {timestamps[last_valid_idx]} = {water_temps[last_valid_idx]:.1f}°C ({water_temps[last_valid_idx]*9/5+32:.1f}°F)")

        # Check which 3-hour intervals have data
        pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
        interval_counts = {h: 0 for h in pacific_intervals}

        for i, ts in enumerate(timestamps):
            if water_temps[i] is not None and water_temps[i] == water_temps[i]:
                hour = ts.hour
                closest = min(pacific_intervals, key=lambda x: abs(x - hour))
                interval_counts[closest] += 1

        logger.info(f"\nValid water temp by 3-hour interval:")
        for hour, count in sorted(interval_counts.items()):
            logger.info(f"  {hour:02d}:00 - {count} records")

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
