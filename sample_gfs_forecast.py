#!/usr/bin/env python3
"""
Sample script to extract temperature and wind speed forecast for tomorrow
from GFS Atmospheric model using gfs_atmospheric_handler.py
"""

import sys
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Import the GFS atmospheric handler
from gfs_atmospheric_handler import (
    get_gfs_atmospheric_dataset_url,
    load_gfs_atmospheric_dataset,
    validate_gfs_atmospheric_dataset,
    extract_gfs_atmospheric_point
)
from config import logger

def get_tomorrow_forecast_sample():
    """
    Extract temperature and wind speed for tomorrow for a sample location.
    Using Santa Monica Beach as example: 34.0195° N, 118.4912° W
    """

    # Sample location (Santa Monica Beach, CA)
    sample_lat = 34.0195
    sample_lon = -118.4912
    location_name = "Santa Monica Beach, CA"

    logger.info("=" * 80)
    logger.info(f"GFS ATMOSPHERIC FORECAST SAMPLE - {location_name}")
    logger.info("=" * 80)
    logger.info(f"Location: {sample_lat}°N, {sample_lon}°W")
    logger.info("")

    # Get current time in Pacific timezone
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific_tz)

    # Calculate tomorrow's date range (midnight to midnight Pacific)
    tomorrow_start = (now_pacific + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_end = tomorrow_start + timedelta(days=1)

    logger.info(f"Current time (Pacific): {now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Tomorrow's date: {tomorrow_start.strftime('%Y-%m-%d')}")
    logger.info("")

    # Get GFS dataset
    logger.info("Fetching GFS Atmospheric dataset URL...")
    gfs_url = get_gfs_atmospheric_dataset_url()
    if not gfs_url:
        logger.error("ERROR: Could not find GFS dataset URL")
        return None

    logger.info(f"Loading dataset from: {gfs_url}")
    ds = load_gfs_atmospheric_dataset(gfs_url)
    if not ds:
        logger.error("ERROR: Failed to load GFS dataset")
        return None

    if not validate_gfs_atmospheric_dataset(ds):
        logger.error("ERROR: Dataset validation failed")
        ds.close()
        return None

    # Get time range from dataset
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    logger.info(f"Dataset time range: {time_vals_full[0]} to {time_vals_full[-1]} (UTC)")
    logger.info("")

    # Convert tomorrow's Pacific time range to UTC for filtering
    tomorrow_start_utc = tomorrow_start.astimezone(pytz.UTC)
    tomorrow_end_utc = tomorrow_end.astimezone(pytz.UTC)

    # Find GFS time indices that fall within tomorrow (Pacific time)
    gfs_times_pacific = time_vals_full.tz_convert(pacific_tz)
    mask = (gfs_times_pacific >= tomorrow_start) & (gfs_times_pacific < tomorrow_end)
    tomorrow_indices = [i for i, m in enumerate(mask) if m]

    if not tomorrow_indices:
        logger.error("ERROR: No data available for tomorrow")
        ds.close()
        return None

    logger.info(f"Found {len(tomorrow_indices)} forecast timesteps for tomorrow")
    logger.info("")

    # Extract atmospheric data for the sample location
    logger.info(f"Extracting forecast data for {location_name}...")
    atmospheric_data = extract_gfs_atmospheric_point(ds, sample_lat, sample_lon, tomorrow_indices)

    ds.close()

    # Format and display results
    logger.info("=" * 80)
    logger.info("FORECAST RESULTS FOR TOMORROW")
    logger.info("=" * 80)
    logger.info("")

    results = []
    for i, time_idx in enumerate(tomorrow_indices):
        time_utc = time_vals_full[time_idx]
        time_pacific = gfs_times_pacific[time_idx]

        temp_f = atmospheric_data["temperature"][i]
        wind_mph = atmospheric_data["wind_speed"][i]
        wind_dir = atmospheric_data["wind_direction"][i]
        gust_mph = atmospheric_data["wind_gust"][i]
        pressure_inhg = atmospheric_data["pressure"][i]
        cloud_pct = atmospheric_data["cloud_cover"][i]
        precip_mmhr = atmospheric_data["precip_rate"][i]

        result = {
            "time_pacific": time_pacific.strftime("%Y-%m-%d %I:%M %p %Z"),
            "time_utc": time_utc.strftime("%Y-%m-%d %H:%M UTC"),
            "temperature_f": f"{temp_f:.1f}°F" if temp_f is not None else "N/A",
            "wind_speed_mph": f"{wind_mph:.1f} mph" if wind_mph is not None else "N/A",
            "wind_direction": f"{wind_dir:.0f}°" if wind_dir is not None else "N/A",
            "wind_gust_mph": f"{gust_mph:.1f} mph" if gust_mph is not None else "N/A",
            "pressure_inhg": f"{pressure_inhg:.2f} inHg" if pressure_inhg is not None else "N/A",
            "cloud_cover": f"{cloud_pct:.0f}%" if cloud_pct is not None else "N/A",
            "precip_rate": f"{precip_mmhr:.2f} mm/hr" if precip_mmhr is not None else "N/A",
        }
        results.append(result)

        logger.info(f"Time: {result['time_pacific']}")
        logger.info(f"  Temperature:    {result['temperature_f']}")
        logger.info(f"  Wind Speed:     {result['wind_speed_mph']}")
        logger.info(f"  Wind Direction: {result['wind_direction']}")
        logger.info(f"  Wind Gust:      {result['wind_gust_mph']}")
        logger.info(f"  Pressure:       {result['pressure_inhg']}")
        logger.info(f"  Cloud Cover:    {result['cloud_cover']}")
        logger.info(f"  Precip Rate:    {result['precip_rate']}")
        logger.info("")

    # Summary statistics
    temps = [atmospheric_data["temperature"][i] for i in range(len(tomorrow_indices))
             if atmospheric_data["temperature"][i] is not None]
    winds = [atmospheric_data["wind_speed"][i] for i in range(len(tomorrow_indices))
             if atmospheric_data["wind_speed"][i] is not None]

    if temps:
        logger.info("=" * 80)
        logger.info("SUMMARY FOR TOMORROW")
        logger.info("=" * 80)
        logger.info(f"Temperature Range: {min(temps):.1f}°F to {max(temps):.1f}°F")
        logger.info(f"Average Temp:      {sum(temps)/len(temps):.1f}°F")
        if winds:
            logger.info(f"Wind Speed Range:  {min(winds):.1f} mph to {max(winds):.1f} mph")
            logger.info(f"Average Wind:      {sum(winds)/len(winds):.1f} mph")
        logger.info("=" * 80)

    return results


if __name__ == "__main__":
    try:
        results = get_tomorrow_forecast_sample()
        if results:
            logger.info("\n✓ Sample forecast extraction completed successfully")
            sys.exit(0)
        else:
            logger.error("\n✗ Sample forecast extraction failed")
            sys.exit(1)
    except Exception as e:
        logger.error(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
