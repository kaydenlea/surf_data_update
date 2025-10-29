#!/usr/bin/env python3
"""
Debug script to verify temperature timing and timezone handling
"""

import pandas as pd
import pytz
from datetime import datetime, timedelta
from gfs_atmospheric_handler import (
    get_gfs_atmospheric_dataset_url,
    load_gfs_atmospheric_dataset,
)
from config import logger

def debug_temperature_timing():
    """Check if temperature timing makes sense."""

    # Get GFS dataset
    logger.info("Loading GFS dataset...")
    gfs_url = get_gfs_atmospheric_dataset_url()
    if not gfs_url:
        logger.error("Could not find GFS dataset")
        return

    ds = load_gfs_atmospheric_dataset(gfs_url)
    if not ds:
        logger.error("Could not load GFS dataset")
        return

    # Get time values
    time_vals_full = pd.to_datetime(ds.time.values)
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    # Convert to Pacific
    pacific_tz = pytz.timezone("America/Los_Angeles")
    time_vals_pacific = time_vals_full.tz_convert(pacific_tz)

    logger.info("=" * 80)
    logger.info("GFS TIME ANALYSIS - First 24 Hours")
    logger.info("=" * 80)

    # Show first 24 hours (8 timesteps at 3-hour intervals)
    for i in range(min(12, len(time_vals_full))):
        utc_time = time_vals_full[i]
        pacific_time = time_vals_pacific[i]

        logger.info(f"Index {i}:")
        logger.info(f"  UTC:     {utc_time.strftime('%Y-%m-%d %H:%M %Z')}")
        logger.info(f"  Pacific: {pacific_time.strftime('%Y-%m-%d %H:%M %Z (%I:%M %p)')}")
        logger.info("")

    # Now check what times are being selected for "tomorrow"
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now_pacific = datetime.now(pacific_tz)
    tomorrow_start = (now_pacific + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_end = tomorrow_start + timedelta(days=1)

    logger.info("=" * 80)
    logger.info("TOMORROW'S TIME WINDOW")
    logger.info("=" * 80)
    logger.info(f"Now (Pacific):      {now_pacific.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Tomorrow Start:     {tomorrow_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Tomorrow End:       {tomorrow_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info("")

    # Find indices that fall in tomorrow
    mask = (time_vals_pacific >= tomorrow_start) & (time_vals_pacific < tomorrow_end)
    tomorrow_indices = [i for i, m in enumerate(mask) if m]

    logger.info(f"Found {len(tomorrow_indices)} timesteps for tomorrow:")
    logger.info("")

    for idx in tomorrow_indices:
        utc_time = time_vals_full[idx]
        pacific_time = time_vals_pacific[idx]

        logger.info(f"Index {idx}:")
        logger.info(f"  UTC:     {utc_time.strftime('%Y-%m-%d %H:%M %Z')}")
        logger.info(f"  Pacific: {pacific_time.strftime('%Y-%m-%d %H:%M %Z (%I:%M %p)')}")
        logger.info("")

    ds.close()

if __name__ == "__main__":
    debug_temperature_timing()
