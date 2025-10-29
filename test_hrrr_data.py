#!/usr/bin/env python3
"""
Test script to explore HRRR dataset structure and available variables
"""

import xarray as xr
from datetime import datetime, timezone
import pytz
from config import logger

def test_hrrr_dataset():
    """Explore HRRR dataset structure."""

    logger.info("=" * 80)
    logger.info("HRRR DATASET EXPLORATION")
    logger.info("=" * 80)

    # HRRR dataset naming convention
    # Format: http://nomads.ncep.noaa.gov/dods/hrrr/hrrrYYYYMMDD/hrrr_sfc_HHz
    # where HH is the model run time (00-23 UTC)

    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y%m%d")

    # Try recent model runs
    hours_to_try = [
        (now_utc.hour - 2) % 24,  # 2 hours ago
        (now_utc.hour - 3) % 24,  # 3 hours ago
        (now_utc.hour - 4) % 24,  # 4 hours ago
    ]

    logger.info(f"Current UTC time: {now_utc.strftime('%Y-%m-%d %H:%M')}")
    logger.info(f"Testing HRRR datasets for date: {today_str}")
    logger.info("")

    for hour in hours_to_try:
        # HRRR has multiple products, start with surface (sfc)
        # NOTE: HRRR uses port 9090 for OpenDAP access
        url = f"http://nomads.ncep.noaa.gov:9090/dods/hrrr/hrrr{today_str}/hrrr_sfc_{hour:02d}z"

        logger.info(f"Testing URL: {url}")

        try:
            ds = xr.open_dataset(url, engine="netcdf4")

            logger.info("=" * 80)
            logger.info(f"SUCCESS! Found HRRR dataset at {hour:02d}z")
            logger.info("=" * 80)

            # List dimensions
            logger.info("DIMENSIONS:")
            for dim_name, dim_size in ds.dims.items():
                logger.info(f"  {dim_name}: {dim_size}")
            logger.info("")

            # List coordinates
            logger.info("COORDINATES:")
            for coord_name in ds.coords:
                coord_data = ds.coords[coord_name]
                logger.info(f"  {coord_name}: shape={coord_data.shape}, dtype={coord_data.dtype}")
            logger.info("")

            # List variables (limit to first 20 for readability)
            logger.info("VARIABLES (first 30):")
            var_names = list(ds.data_vars.keys())[:30]
            for var_name in var_names:
                var_data = ds[var_name]
                logger.info(f"  {var_name}: shape={var_data.shape}, dtype={var_data.dtype}")
                # Try to get long_name or description
                if hasattr(var_data, 'long_name'):
                    logger.info(f"    Description: {var_data.long_name}")
                elif hasattr(var_data, 'description'):
                    logger.info(f"    Description: {var_data.description}")
            logger.info("")

            # Check for key atmospheric variables
            key_vars = ['tmp2m', 'tmpsfc', 'pressfc', 'ugrd10m', 'vgrd10m',
                       'gustsfc', 'tcdcclm', 'pratesfc', 'dpt2m']

            logger.info("KEY ATMOSPHERIC VARIABLES:")
            for var_name in key_vars:
                if var_name in ds.variables:
                    logger.info(f"  ✓ {var_name} - AVAILABLE")
                else:
                    logger.info(f"  ✗ {var_name} - NOT FOUND")
            logger.info("")

            # Check time information
            if 'time' in ds.coords:
                time_vals = ds['time'].values
                logger.info(f"TIME INFORMATION:")
                logger.info(f"  Number of timesteps: {len(time_vals)}")

                import pandas as pd
                time_pd = pd.to_datetime(time_vals)
                if time_pd.tz is None:
                    time_pd = time_pd.tz_localize("UTC")

                pacific_tz = pytz.timezone("America/Los_Angeles")
                time_pacific = time_pd.tz_convert(pacific_tz)

                logger.info(f"  First timestep (UTC):     {time_pd[0]}")
                logger.info(f"  First timestep (Pacific): {time_pacific[0]}")
                logger.info(f"  Last timestep (UTC):      {time_pd[-1]}")
                logger.info(f"  Last timestep (Pacific):  {time_pacific[-1]}")

                # Calculate forecast range
                forecast_hours = len(time_vals)
                logger.info(f"  Forecast range: {forecast_hours} hours")

            ds.close()

            logger.info("=" * 80)
            return True

        except Exception as e:
            logger.debug(f"  Failed: {str(e)[:100]}")
            continue

    logger.error("Could not access any HRRR datasets")
    return False

if __name__ == "__main__":
    import sys
    success = test_hrrr_dataset()
    sys.exit(0 if success else 1)
