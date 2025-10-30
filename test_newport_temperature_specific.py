#!/usr/bin/env python3
"""
Temperature data test for the most specific Newport Beach location possible
Using exact coordinates for Newport Beach Pier
"""

import sys
from datetime import datetime, timedelta
import pytz
import pandas as pd

from gfs_atmospheric_handler import (
    get_gfs_atmospheric_dataset_url,
    load_gfs_atmospheric_dataset,
    validate_gfs_atmospheric_dataset,
    extract_gfs_atmospheric_point
)
from config import logger

def test_newport_beach_specific_temperature():
    """
    Test temperature data for Newport Beach Pier (most specific location)
    Newport Beach Pier coordinates: 33.6080° N, 117.9297° W
    """

    # Most specific Newport Beach location - Newport Beach Pier
    pier_lat = 33.6080
    pier_lon = -117.9297
    location_name = "Newport Beach Pier, CA"

    logger.info("=" * 80)
    logger.info(f"SPECIFIC TEMPERATURE TEST - {location_name}")
    logger.info("=" * 80)
    logger.info(f"Exact Location: {pier_lat}°N, {pier_lon}°W")
    logger.info("This is the most precise Newport Beach surf spot")
    logger.info("")

    # Get current time
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific_tz)

    logger.info(f"Current time (Pacific): {now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("")

    # Get GFS dataset
    logger.info("Step 1: Fetching GFS Atmospheric dataset...")
    gfs_url = get_gfs_atmospheric_dataset_url()
    if not gfs_url:
        logger.error("ERROR: Could not find GFS dataset URL")
        return None

    logger.info(f"  Dataset URL: {gfs_url}")
    logger.info("")

    logger.info("Step 2: Loading dataset...")
    ds = load_gfs_atmospheric_dataset(gfs_url)
    if not ds:
        logger.error("ERROR: Failed to load GFS dataset")
        return None

    if not validate_gfs_atmospheric_dataset(ds):
        logger.error("ERROR: Dataset validation failed")
        ds.close()
        return None

    # Get time information
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    logger.info(f"  Dataset time range: {time_vals_full[0]} to {time_vals_full[-1]} (UTC)")
    logger.info(f"  Total timesteps: {len(time_vals_full)}")
    logger.info("")

    # Check GFS grid resolution at this location
    logger.info("Step 3: Finding nearest GFS grid point...")
    import numpy as np

    lats = ds.lat.values
    lons = ds.lon.values

    lat_idx = np.abs(lats - pier_lat).argmin()
    lon_idx = np.abs(lons - pier_lon).argmin()

    grid_lat = float(lats[lat_idx])
    grid_lon = float(lons[lon_idx])

    # Calculate distance from pier to grid point
    lat_diff = abs(grid_lat - pier_lat)
    lon_diff = abs(grid_lon - pier_lon)

    # Approximate distance in miles (1 degree ≈ 69 miles for latitude, adjusted for longitude)
    lat_dist_mi = lat_diff * 69
    lon_dist_mi = lon_diff * 69 * np.cos(np.radians(pier_lat))
    total_dist_mi = np.sqrt(lat_dist_mi**2 + lon_dist_mi**2)

    logger.info(f"  Newport Beach Pier:    {pier_lat:.4f}°N, {pier_lon:.4f}°W")
    logger.info(f"  Nearest GFS grid point: {grid_lat:.4f}°N, {grid_lon:.4f}°W")
    logger.info(f"  Distance from pier:     {total_dist_mi:.2f} miles")
    logger.info(f"  Grid offset:            {lat_diff:.4f}° lat, {lon_diff:.4f}° lon")
    logger.info("")

    # Get next 24 hours of data
    logger.info("Step 4: Extracting next 24 hours of temperature data (times shifted +6 hours for display)...")

    # Find timesteps for next 24 hours
    now_utc = now_pacific.astimezone(pytz.UTC)
    end_time_utc = now_utc + timedelta(hours=24)

    mask = (time_vals_full >= now_utc) & (time_vals_full <= end_time_utc)
    next_24h_indices = [i for i, m in enumerate(mask) if m]

    if not next_24h_indices:
        logger.error("ERROR: No data available for next 24 hours")
        ds.close()
        return None

    logger.info(f"  Found {len(next_24h_indices)} timesteps for next 24 hours")
    logger.info("")

    # Extract atmospheric data for Newport Beach Pier
    atmospheric_data = extract_gfs_atmospheric_point(ds, pier_lat, pier_lon, next_24h_indices)

    ds.close()

    # Display results
    logger.info("=" * 80)
    logger.info("TEMPERATURE FORECAST - Next 24 Hours")
    logger.info("=" * 80)
    logger.info("")

    gfs_times_pacific = time_vals_full.tz_convert(pacific_tz)

    results = []
    for i, time_idx in enumerate(next_24h_indices):
        time_utc = time_vals_full[time_idx]
        time_pacific = gfs_times_pacific[time_idx]

        # Shift display time by +6 hours
        time_pacific_shifted = time_pacific + timedelta(hours=6)

        temp_f = atmospheric_data["temperature"][i]
        wind_mph = atmospheric_data["wind_speed"][i]
        wind_dir = atmospheric_data["wind_direction"][i]
        gust_mph = atmospheric_data["wind_gust"][i]
        pressure_inhg = atmospheric_data["pressure"][i]
        cloud_pct = atmospheric_data["cloud_cover"][i]
        precip_mmhr = atmospheric_data["precip_rate"][i]

        # Calculate hours from now
        hours_from_now = (time_utc - now_utc).total_seconds() / 3600

        result = {
            "hours_from_now": hours_from_now,
            "time_pacific": time_pacific_shifted,  # Store shifted time
            "temp_f": temp_f,
            "wind_mph": wind_mph,
            "wind_dir": wind_dir,
            "gust_mph": gust_mph,
            "pressure": pressure_inhg,
            "clouds": cloud_pct,
            "precip": precip_mmhr,
        }
        results.append(result)

        # Format output with shifted time
        time_str = time_pacific_shifted.strftime("%a %I:%M %p")
        hours_str = f"+{hours_from_now:.0f}h"

        logger.info(f"{hours_str:>5s} | {time_str:>12s} | Temp: {temp_f:5.1f}°F | "
                   f"Wind: {wind_mph:4.1f} mph @ {wind_dir:3.0f}° | "
                   f"Clouds: {cloud_pct:3.0f}%")

    logger.info("")
    logger.info("=" * 80)
    logger.info("TEMPERATURE STATISTICS - Next 24 Hours")
    logger.info("=" * 80)

    temps = [r["temp_f"] for r in results if r["temp_f"] is not None]
    winds = [r["wind_mph"] for r in results if r["wind_mph"] is not None]
    clouds = [r["clouds"] for r in results if r["clouds"] is not None]

    if temps:
        logger.info(f"Temperature:")
        logger.info(f"  Current:  {temps[0]:.1f}°F (at {results[0]['time_pacific'].strftime('%I:%M %p')})")
        logger.info(f"  High:     {max(temps):.1f}°F")
        logger.info(f"  Low:      {min(temps):.1f}°F")
        logger.info(f"  Average:  {sum(temps)/len(temps):.1f}°F")
        logger.info(f"  Range:    {max(temps) - min(temps):.1f}°F variation")

    if winds:
        logger.info(f"")
        logger.info(f"Wind:")
        logger.info(f"  Average:  {sum(winds)/len(winds):.1f} mph")
        logger.info(f"  Range:    {min(winds):.1f} - {max(winds):.1f} mph")

    if clouds:
        logger.info(f"")
        logger.info(f"Cloud Cover:")
        logger.info(f"  Average:  {sum(clouds)/len(clouds):.0f}%")

    logger.info("")
    logger.info("=" * 80)
    logger.info("DATA SOURCE DETAILS")
    logger.info("=" * 80)
    logger.info(f"Model:          NOAA GFS Atmospheric (0.25° resolution)")
    logger.info(f"Grid spacing:   ~17 miles (25 km)")
    logger.info(f"Location:       Newport Beach Pier")
    logger.info(f"Coordinates:    {pier_lat}°N, {pier_lon}°W")
    logger.info(f"Grid point:     {grid_lat}°N, {grid_lon}°W")
    logger.info(f"Distance:       {total_dist_mi:.2f} miles from exact pier location")
    logger.info(f"Forecast run:   {gfs_url.split('/')[-1]}")
    logger.info("=" * 80)

    return results


if __name__ == "__main__":
    try:
        results = test_newport_beach_specific_temperature()
        if results:
            logger.info("")
            logger.info("Temperature data extraction completed successfully")
            sys.exit(0)
        else:
            logger.error("")
            logger.error("Temperature data extraction failed")
            sys.exit(1)
    except Exception as e:
        logger.error(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
