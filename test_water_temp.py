#!/usr/bin/env python3
"""
Test script to check what water temperature data Open Meteo Marine API returns
for our grid points.
"""

import openmeteo_requests
import requests_cache
from retry_requests import retry
from config import logger
from noaa_grid_handler import fetch_grid_points_from_db
from datetime import datetime, timedelta
import pytz


def test_water_temp_availability():
    """Test which grid points have water temperature data available."""

    # Setup the Open-Meteo API client
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Fetch grid points
    logger.info("Fetching grid points...")
    grid_points = fetch_grid_points_from_db()
    logger.info(f"Found {len(grid_points)} grid points")

    # Test date range (next 7 days)
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now = datetime.now(pacific_tz)
    start_date = now.date().strftime("%Y-%m-%d")
    end_date = (now.date() + timedelta(days=7)).strftime("%Y-%m-%d")

    logger.info(f"Testing water temperature for date range: {start_date} to {end_date}")
    logger.info("=" * 80)

    # Test each grid point
    has_data = []
    no_data = []

    for i, gp in enumerate(grid_points[:10], 1):  # Test first 10 points
        lat = gp['latitude']
        lon = gp['longitude']

        # Normalize longitude
        if lon > 180:
            lon = lon - 360

        logger.info(f"\n{i}. Grid Point {gp['id']}: Lat={lat}, Lon={lon} ({gp.get('region', 'unknown')})")

        try:
            # Marine API request
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": ["sea_surface_temperature"],
                "timezone": "America/Los_Angeles",
                "start_date": start_date,
                "end_date": end_date
            }

            responses = openmeteo.weather_api("https://marine-api.open-meteo.com/v1/marine", params=params)

            if responses:
                response = responses[0]
                hourly = response.Hourly()

                # Get water temp data
                water_temps = hourly.Variables(0).ValuesAsNumpy()

                # Check for valid data (not all NaN)
                valid_count = sum(1 for temp in water_temps if temp is not None and not (temp != temp))  # check for NaN
                total_count = len(water_temps)

                if valid_count > 0:
                    logger.info(f"   ✓ HAS DATA: {valid_count}/{total_count} values ({valid_count/total_count*100:.1f}%)")
                    has_data.append(gp['id'])

                    # Show sample values
                    sample_temps = [temp for temp in water_temps[:24] if temp is not None and temp == temp][:5]
                    if sample_temps:
                        temps_f = [temp * 9/5 + 32 for temp in sample_temps]
                        logger.info(f"   Sample temps (F): {[f'{t:.1f}' for t in temps_f]}")
                else:
                    logger.info(f"   ✗ NO DATA: All values are NaN")
                    no_data.append(gp['id'])
            else:
                logger.info(f"   ✗ NO RESPONSE")
                no_data.append(gp['id'])

        except Exception as e:
            logger.error(f"   ✗ ERROR: {e}")
            no_data.append(gp['id'])

    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Grid points WITH water temperature data: {len(has_data)}")
    logger.info(f"Grid points WITHOUT water temperature data: {len(no_data)}")
    logger.info(f"Coverage: {len(has_data)/(len(has_data)+len(no_data))*100:.1f}%")

    if has_data:
        logger.info(f"\nGrid points with data: {has_data}")
    if no_data:
        logger.info(f"Grid points without data: {no_data}")


if __name__ == "__main__":
    try:
        test_water_temp_availability()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        exit(1)
