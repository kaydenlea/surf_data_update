#!/usr/bin/env python3
"""
NOAA GFSwave handler for grid-based forecast data.
This module extracts wave data directly from grid points instead of interpolating to beach locations.
"""

import numpy as np
from datetime import datetime, timezone, timedelta
import pytz
from config import logger
from noaa_handler import (
    get_noaa_dataset_url, load_noaa_dataset, validate_noaa_dataset,
    load_cdip_data, find_nearest_cdip_site, interpolate_cdip_to_gfs_times
)
from swell_ranking import (
    rank_swell_trains, calculate_wave_energy_kj, get_surf_height_range
)
from utils import normalize_surf_range
import pandas as pd


def get_noaa_grid_data(ds, grid_points, cdip_data=None):
    """
    Extract NOAA GFSwave data directly from grid points with optional CDIP enhancement.

    Args:
        ds: Loaded xarray dataset from NOAA GFSwave
        grid_points: List of dicts with id, latitude, longitude, latitude_index, longitude_index
        cdip_data: Optional CDIP data for enhancement (loaded via load_cdip_data())

    Returns:
        List of forecast records (one per grid_id per timestamp)
    """
    logger.info("=" * 80)
    logger.info("EXTRACTING NOAA GFSWAVE DATA FROM GRID POINTS")
    if cdip_data:
        logger.info("WITH CDIP ENHANCEMENT")
    logger.info("=" * 80)

    records = []
    pacific = pytz.timezone('America/Los_Angeles')

    # Get time values
    times = ds['time'].values

    # Determine coordinate variable names
    if 'latitude' in ds.dims:
        lat_var = 'latitude'
        lon_var = 'longitude'
    else:
        lat_var = 'lat'
        lon_var = 'lon'

    logger.info(f"Processing {len(grid_points)} grid points × {len(times)} time steps")
    logger.info(f"Expected records: {len(grid_points) * len(times):,}")

    # Process each grid point
    for idx, grid_point in enumerate(grid_points, start=1):
        grid_id = grid_point['id']
        lat_idx = grid_point['latitude_index']
        lon_idx = grid_point['longitude_index']
        lat = grid_point['latitude']
        lon = grid_point['longitude']

        logger.info(f"   Processing grid point {idx}/{len(grid_points)} (grid_id={grid_id})")

        # Extract data for this grid point across all times
        try:
            # Get swell data (height, period, direction for 3 trains)
            swell_1_height = ds['swell_1'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_2_height = ds['swell_2'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_3_height = ds['swell_3'].isel({lat_var: lat_idx, lon_var: lon_idx}).values

            swell_1_period = ds['swper_1'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_2_period = ds['swper_2'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_3_period = ds['swper_3'].isel({lat_var: lat_idx, lon_var: lon_idx}).values

            swell_1_dir = ds['swdir_1'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_2_dir = ds['swdir_2'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            swell_3_dir = ds['swdir_3'].isel({lat_var: lat_idx, lon_var: lon_idx}).values

            # Get combined wave height (for surf calculations)
            combined_wave_height = ds['htsgwsfc'].isel({lat_var: lat_idx, lon_var: lon_idx}).values

            # Get wind data
            wind_u = ds['ugrdsfc'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            wind_v = ds['vgrdsfc'].isel({lat_var: lat_idx, lon_var: lon_idx}).values

            # Wind gust (if available)
            if 'gustsfc' in ds.data_vars:
                wind_gust = ds['gustsfc'].isel({lat_var: lat_idx, lon_var: lon_idx}).values
            else:
                wind_gust = np.full(len(times), np.nan)

        except Exception as e:
            logger.warning(f"Error extracting data for grid point {grid_id}: {e}")
            continue

        # CDIP Enhancement (replace swell_1 with CDIP data where available)
        cdip_enhanced = False
        if cdip_data is not None:
            try:
                # Find nearest CDIP site for this grid point
                cdip_idx = find_nearest_cdip_site(cdip_data, lat, lon)

                if cdip_idx is not None:
                    # Convert times to Pacific for interpolation
                    pacific_times = []
                    for t_val in times:
                        ts = np.datetime64(t_val, 'ns')
                        dt_utc = datetime.utcfromtimestamp(ts.astype('datetime64[s]').astype(int)).replace(tzinfo=timezone.utc)
                        dt_pac = dt_utc.astimezone(pacific)
                        pacific_times.append(pd.Timestamp(dt_pac))

                    # Extract and interpolate CDIP data to match GFS times
                    cdip_hs_m = cdip_data['hs_m'][:, cdip_idx]
                    cdip_tp_s = cdip_data['tp_s'][:, cdip_idx]
                    cdip_dp_deg = cdip_data['dp_deg'][:, cdip_idx]

                    cdip_hs_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_hs_m, pacific_times)
                    cdip_tp_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_tp_s, pacific_times)
                    cdip_dp_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_dp_deg, pacific_times)

                    # Replace swell_1 with CDIP data where valid
                    valid_cdip = ~np.isnan(cdip_hs_interp)
                    if np.any(valid_cdip):
                        swell_1_height = np.where(valid_cdip, cdip_hs_interp, swell_1_height)
                        swell_1_period = np.where(valid_cdip, cdip_tp_interp, swell_1_period)
                        swell_1_dir = np.where(valid_cdip, cdip_dp_interp, swell_1_dir)
                        combined_wave_height = np.where(valid_cdip, cdip_hs_interp, combined_wave_height)
                        cdip_enhanced = True
                        logger.debug(f"   Grid {grid_id}: Enhanced with CDIP data for {np.sum(valid_cdip)} timesteps")
            except Exception as e:
                logger.debug(f"   Grid {grid_id}: CDIP enhancement failed: {e}")

        # Process each timestamp
        for t_idx, time_val in enumerate(times):
            try:
                # Convert timestamp to Pacific time
                ts = np.datetime64(time_val, 'ns')
                dt_utc = datetime.utcfromtimestamp(ts.astype('datetime64[s]').astype(int)).replace(tzinfo=timezone.utc)
                dt_pacific = dt_utc.astimezone(pacific)

                # Round to nearest 3-hour interval (00:00, 03:00, 06:00, etc.)
                hour = dt_pacific.hour
                remainder = hour % 3
                if remainder <= 1:
                    target_hour = hour - remainder
                else:
                    target_hour = hour + (3 - remainder)

                if target_hour >= 24:
                    target_hour = 0
                    dt_pacific += timedelta(days=1)

                # Create clean timestamp
                clean_timestamp = pacific.localize(datetime(
                    dt_pacific.year, dt_pacific.month, dt_pacific.day,
                    target_hour, 0, 0
                ))

                timestamp_iso = clean_timestamp.isoformat()

                # Extract swell data for this time
                swells = []

                # Swell 1
                h1 = float(swell_1_height[t_idx])
                p1 = float(swell_1_period[t_idx])
                d1 = float(swell_1_dir[t_idx])
                if not (np.isnan(h1) or np.isnan(p1) or np.isnan(d1)):
                    swells.append({
                        'height_m': h1,
                        'height_ft': h1 * 3.28084,
                        'period_s': p1,
                        'direction_deg': d1
                    })

                # Swell 2
                h2 = float(swell_2_height[t_idx])
                p2 = float(swell_2_period[t_idx])
                d2 = float(swell_2_dir[t_idx])
                if not (np.isnan(h2) or np.isnan(p2) or np.isnan(d2)):
                    swells.append({
                        'height_m': h2,
                        'height_ft': h2 * 3.28084,
                        'period_s': p2,
                        'direction_deg': d2
                    })

                # Swell 3
                h3 = float(swell_3_height[t_idx])
                p3 = float(swell_3_period[t_idx])
                d3 = float(swell_3_dir[t_idx])
                if not (np.isnan(h3) or np.isnan(p3) or np.isnan(d3)):
                    swells.append({
                        'height_m': h3,
                        'height_ft': h3 * 3.28084,
                        'period_s': p3,
                        'direction_deg': d3
                    })

                # Rank swells by impact
                primary, secondary, tertiary = rank_swell_trains(swells)

                # Calculate surf height from combined wave height
                combined_height_m = float(combined_wave_height[t_idx])
                if not np.isnan(combined_height_m):
                    surf_min_ft, surf_max_ft = get_surf_height_range(combined_height_m)
                    surf_min_ft, surf_max_ft = normalize_surf_range(surf_min_ft, surf_max_ft)
                else:
                    surf_min_ft = None
                    surf_max_ft = None

                # Calculate wave energy
                if primary:
                    wave_energy_kj = calculate_wave_energy_kj(primary['height_ft'], primary['period_s'])
                else:
                    wave_energy_kj = None

                # Calculate wind speed and direction
                wu = float(wind_u[t_idx])
                wv = float(wind_v[t_idx])

                if not (np.isnan(wu) or np.isnan(wv)):
                    wind_speed_ms = np.sqrt(wu**2 + wv**2)
                    wind_speed_mph = wind_speed_ms * 2.23694
                    wind_direction = (270 - np.degrees(np.arctan2(wv, wu))) % 360
                else:
                    wind_speed_mph = None
                    wind_direction = None

                # Wind gust
                wg = float(wind_gust[t_idx])
                wind_gust_mph = wg * 2.23694 if not np.isnan(wg) else None

                # Build record
                record = {
                    "grid_id": grid_id,
                    "timestamp": timestamp_iso,
                }

                # Only include non-None values
                def _set_if_value(key, value):
                    if value is not None:
                        record[key] = value

                _set_if_value("primary_swell_height_ft", primary['height_ft'] if primary else None)
                _set_if_value("primary_swell_period_s", primary['period_s'] if primary else None)
                _set_if_value("primary_swell_direction", primary['direction_deg'] if primary else None)
                _set_if_value("secondary_swell_height_ft", secondary['height_ft'] if secondary else None)
                _set_if_value("secondary_swell_period_s", secondary['period_s'] if secondary else None)
                _set_if_value("secondary_swell_direction", secondary['direction_deg'] if secondary else None)
                _set_if_value("tertiary_swell_height_ft", tertiary['height_ft'] if tertiary else None)
                _set_if_value("tertiary_swell_period_s", tertiary['period_s'] if tertiary else None)
                _set_if_value("tertiary_swell_direction", tertiary['direction_deg'] if tertiary else None)
                _set_if_value("surf_height_min_ft", surf_min_ft)
                _set_if_value("surf_height_max_ft", surf_max_ft)
                _set_if_value("wave_energy_kj", wave_energy_kj)
                _set_if_value("wind_speed_mph", wind_speed_mph)
                _set_if_value("wind_direction_deg", wind_direction)
                _set_if_value("wind_gust_mph", wind_gust_mph)

                records.append(record)

            except Exception as e:
                logger.debug(f"Error processing grid {grid_id} at time {t_idx}: {e}")
                continue

    logger.info(f"Extracted {len(records):,} records from {len(grid_points)} grid points")
    logger.info("=" * 80)

    return records


def fetch_grid_points_from_db():
    """Fetch all grid points from database."""
    from database import supabase

    logger.info("Fetching grid points from database...")

    try:
        response = supabase.table("grid_points").select(
            "id, latitude, longitude, latitude_index, longitude_index, region"
        ).execute()

        grid_points = response.data
        logger.info(f"Found {len(grid_points)} grid points")
        return grid_points

    except Exception as e:
        logger.error(f"Error fetching grid points: {e}")
        return []


def test_grid_extraction():
    """Test function to verify grid-based extraction works."""
    logger.info("Testing grid-based NOAA data extraction...")

    # Load dataset
    url = get_noaa_dataset_url()
    ds = load_noaa_dataset(url)

    if not validate_noaa_dataset(ds):
        logger.error("Dataset validation failed")
        return False

    # Fetch grid points
    grid_points = fetch_grid_points_from_db()

    if not grid_points:
        logger.error("No grid points found")
        return False

    # Extract data
    records = get_noaa_grid_data(ds, grid_points)

    ds.close()

    if records:
        logger.info(f"SUCCESS: Extracted {len(records)} records")
        logger.info(f"Sample record: {records[0]}")
        return True
    else:
        logger.error("FAILED: No records extracted")
        return False


if __name__ == "__main__":
    test_grid_extraction()
