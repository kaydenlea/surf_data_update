#!/usr/bin/env python3
"""
Analyze GFSwave dataset to count exact grid points and show coverage area.
"""

import xarray as xr
import numpy as np
from noaa_handler import get_noaa_dataset_url, load_noaa_dataset
from config import logger

def analyze_gfswave_grid():
    """Load GFSwave dataset and analyze its grid structure."""

    logger.info("=" * 80)
    logger.info("GFSWAVE GRID ANALYSIS")
    logger.info("=" * 80)

    # Load the dataset
    logger.info("\nStep 1: Loading GFSwave dataset...")
    url = get_noaa_dataset_url()
    logger.info(f"Dataset URL: {url}")

    ds = load_noaa_dataset(url)
    if ds is None:
        logger.error("Failed to load dataset")
        return

    logger.info("Dataset loaded successfully!")

    # Analyze dimensions
    logger.info("\n" + "=" * 80)
    logger.info("DATASET DIMENSIONS")
    logger.info("=" * 80)

    if 'latitude' in ds.dims:
        lat_var = 'latitude'
        lon_var = 'longitude'
    else:
        lat_var = 'lat'
        lon_var = 'lon'

    lats = ds[lat_var].values
    lons = ds[lon_var].values

    logger.info(f"Latitude points: {len(lats)}")
    logger.info(f"Longitude points: {len(lons)}")
    logger.info(f"Total grid cells: {len(lats) * len(lons):,}")

    # Coordinate ranges
    logger.info("\n" + "=" * 80)
    logger.info("COORDINATE RANGES")
    logger.info("=" * 80)
    logger.info(f"Latitude range: {lats.min():.4f}° to {lats.max():.4f}°")
    logger.info(f"Longitude range: {lons.min():.4f}° to {lons.max():.4f}°")
    logger.info(f"Latitude span: {lats.max() - lats.min():.4f}°")
    logger.info(f"Longitude span: {lons.max() - lons.min():.4f}°")

    # Calculate resolution
    if len(lats) > 1:
        lat_resolution = np.median(np.diff(lats))
        lon_resolution = np.median(np.diff(lons))
        logger.info(f"\nGrid resolution:")
        logger.info(f"  Latitude: {abs(lat_resolution):.4f}° (~{abs(lat_resolution) * 69:.1f} miles)")
        logger.info(f"  Longitude: {abs(lon_resolution):.4f}° (~{abs(lon_resolution) * 54.6:.1f} miles at 37°N)")

    # Check for valid ocean points
    logger.info("\n" + "=" * 80)
    logger.info("VALID OCEAN POINTS - ALL WEST COAST")
    logger.info("=" * 80)

    # Check swell_1 variable for valid data (non-NaN indicates ocean point)
    if 'swell_1' in ds.data_vars:
        # Get first time step
        swell_data = ds['swell_1'].isel(time=0).values

        valid_points = ~np.isnan(swell_data)
        num_valid = np.sum(valid_points)
        num_total = valid_points.size

        logger.info(f"Total grid cells: {num_total:,}")
        logger.info(f"Valid ocean points: {num_valid:,}")
        logger.info(f"Land/invalid points: {num_total - num_valid:,}")
        logger.info(f"Ocean coverage: {num_valid / num_total * 100:.1f}%")

        # Show valid coordinate ranges
        valid_lats_idx, valid_lons_idx = np.where(valid_points)
        if len(valid_lats_idx) > 0:
            valid_lats = lats[valid_lats_idx]
            valid_lons = lons[valid_lons_idx]
            logger.info(f"\nValid ocean point ranges:")
            logger.info(f"  Latitude: {valid_lats.min():.4f}° to {valid_lats.max():.4f}°")
            logger.info(f"  Longitude: {valid_lons.min():.4f}° to {valid_lons.max():.4f}°")

        # CALIFORNIA ONLY ANALYSIS
        logger.info("\n" + "=" * 80)
        logger.info("CALIFORNIA ONLY - OCEAN POINTS")
        logger.info("=" * 80)

        # California boundaries
        CA_LAT_MIN = 32.5  # San Diego
        CA_LAT_MAX = 42.0  # Oregon border
        CA_LON_MIN = 235.5  # -124.5°W (westernmost coast)
        CA_LON_MAX = 243.0  # -117.0°W (easternmost coast)

        logger.info(f"California boundaries:")
        logger.info(f"  Latitude: {CA_LAT_MIN}° to {CA_LAT_MAX}° N")
        logger.info(f"  Longitude: {CA_LON_MIN}° to {CA_LON_MAX}° E ({CA_LON_MIN - 360:.1f}° to {CA_LON_MAX - 360:.1f}° W)")

        # Create mask for California region
        lat_mask = (lats >= CA_LAT_MIN) & (lats <= CA_LAT_MAX)
        lon_mask = (lons >= CA_LON_MIN) & (lons <= CA_LON_MAX)

        # Create 2D mask
        lat_mask_2d = lat_mask[:, np.newaxis]
        lon_mask_2d = lon_mask[np.newaxis, :]
        ca_region_mask = lat_mask_2d & lon_mask_2d

        # Apply to valid points
        ca_valid_points = valid_points & ca_region_mask
        num_ca_valid = np.sum(ca_valid_points)
        num_ca_total = np.sum(ca_region_mask)

        logger.info(f"\nCalifornia grid statistics:")
        logger.info(f"  Total California grid cells: {num_ca_total:,}")
        logger.info(f"  Valid California ocean points: {num_ca_valid:,}")
        logger.info(f"  California land/invalid points: {num_ca_total - num_ca_valid:,}")
        logger.info(f"  California ocean coverage: {num_ca_valid / num_ca_total * 100:.1f}%")

        # Get CA-specific coordinate ranges
        ca_lats_idx, ca_lons_idx = np.where(ca_valid_points)
        if len(ca_lats_idx) > 0:
            ca_valid_lats = lats[ca_lats_idx]
            ca_valid_lons = lons[ca_lons_idx]
            logger.info(f"\nValid California ocean point ranges:")
            logger.info(f"  Latitude: {ca_valid_lats.min():.4f}° to {ca_valid_lats.max():.4f}° ({ca_valid_lats.max() - ca_valid_lats.min():.2f}° span)")
            logger.info(f"  Longitude: {ca_valid_lons.min():.4f}° to {ca_valid_lons.max():.4f}° ({ca_valid_lons.max() - ca_valid_lons.min():.2f}° span)")

            logger.info(f"\nPercentage of total West Coast points:")
            logger.info(f"  California: {num_ca_valid / num_valid * 100:.1f}% of all valid ocean points")

            # NEARSHORE ANALYSIS (within 25 miles of coast)
            logger.info("\n" + "=" * 80)
            logger.info("CALIFORNIA NEARSHORE - WITHIN 25 MILES OF COAST")
            logger.info("=" * 80)

            # Approximate California coastline longitude (varies by latitude)
            # Simplified: California coast is roughly at these longitudes
            # Northern CA (~42°N): -124.2°W (235.8°E)
            # Central CA (~37°N): -122.4°W (237.6°E)
            # Southern CA (~33°N): -117.5°W (242.5°E)

            # 25 miles ≈ 0.36° longitude at 37°N (varies with latitude)
            # We'll use a conservative 0.5° buffer to ensure we capture nearshore

            NEARSHORE_BUFFER_DEG = 0.5  # ~34 miles at 37°N (conservative)

            # Define approximate coastline longitude by latitude region
            def get_coast_longitude(lat):
                """Approximate California coast longitude for a given latitude."""
                if lat >= 40:  # Northern CA
                    return 235.8  # -124.2°W
                elif lat >= 35:  # Central CA
                    return 237.6  # -122.4°W
                else:  # Southern CA
                    return 242.5  # -117.5°W

            # Count nearshore points (within buffer of coast)
            nearshore_count = 0
            nearshore_points_list = []

            for lat, lon in zip(ca_valid_lats, ca_valid_lons):
                coast_lon = get_coast_longitude(lat)
                # Check if within buffer of coast (to the west of coast)
                if lon <= coast_lon and lon >= (coast_lon - NEARSHORE_BUFFER_DEG):
                    nearshore_count += 1
                    nearshore_points_list.append((lat, lon))

            logger.info(f"Nearshore buffer: {NEARSHORE_BUFFER_DEG}° longitude (~{NEARSHORE_BUFFER_DEG * 69:.1f} miles)")
            logger.info(f"Total California ocean points: {num_ca_valid:,}")
            logger.info(f"Nearshore points (within ~25 miles): {nearshore_count:,}")
            logger.info(f"Offshore points (beyond 25 miles): {num_ca_valid - nearshore_count:,}")
            logger.info(f"Nearshore percentage: {nearshore_count / num_ca_valid * 100:.1f}%")

            if nearshore_count > 0:
                nearshore_lats = [p[0] for p in nearshore_points_list]
                nearshore_lons = [p[1] for p in nearshore_points_list]
                logger.info(f"\nNearshore point ranges:")
                logger.info(f"  Latitude: {min(nearshore_lats):.4f}° to {max(nearshore_lats):.4f}°")
                logger.info(f"  Longitude: {min(nearshore_lons):.4f}° to {max(nearshore_lons):.4f}°")
                logger.info(f"  Longitude span: {max(nearshore_lons) - min(nearshore_lons):.2f}° (~{(max(nearshore_lons) - min(nearshore_lons)) * 54.6:.1f} miles)")

    # Time dimension
    logger.info("\n" + "=" * 80)
    logger.info("TIME COVERAGE")
    logger.info("=" * 80)

    if 'time' in ds.dims:
        times = ds['time'].values
        logger.info(f"Number of forecast time steps: {len(times)}")

        if len(times) > 0:
            import pandas as pd
            first_time = pd.Timestamp(times[0])
            last_time = pd.Timestamp(times[-1])

            logger.info(f"First forecast: {first_time}")
            logger.info(f"Last forecast: {last_time}")
            logger.info(f"Forecast horizon: {(last_time - first_time).total_seconds() / 3600:.1f} hours")

            if len(times) > 1:
                time_diff = (pd.Timestamp(times[1]) - pd.Timestamp(times[0])).total_seconds() / 3600
                logger.info(f"Time step interval: {time_diff:.1f} hours")

    # Available variables
    logger.info("\n" + "=" * 80)
    logger.info("AVAILABLE VARIABLES")
    logger.info("=" * 80)

    wave_vars = [v for v in ds.data_vars if 'swell' in str(v).lower() or 'wind' in str(v).lower() or 'wave' in str(v).lower()]
    logger.info(f"Wave-related variables ({len(wave_vars)}):")
    for var in sorted(wave_vars):
        logger.info(f"  - {var}")

    # Storage calculation
    logger.info("\n" + "=" * 80)
    logger.info("STORAGE ESTIMATES")
    logger.info("=" * 80)

    if 'swell_1' in ds.data_vars and 'time' in ds.dims:
        num_times = len(ds['time'])
        num_ocean_points = num_valid if 'swell_1' in ds.data_vars else len(lats) * len(lons)

        # Assuming 8 fields (3 swell trains with height/period/direction, surf height, wave energy)
        # Each field is ~4 bytes (float32)
        bytes_per_record = 8 * 4  # 32 bytes per timestamp per grid point

        # ALL WEST COAST
        total_bytes_all = num_ocean_points * num_times * bytes_per_record

        logger.info(f"ALL WEST COAST - Records per forecast run:")
        logger.info(f"  {num_ocean_points:,} ocean points × {num_times} time steps = {num_ocean_points * num_times:,} records")
        logger.info(f"\nALL WEST COAST - Storage (per forecast run):")
        logger.info(f"  Raw data: {total_bytes_all / 1024 / 1024:.1f} MB")
        logger.info(f"  With 7-day retention: {total_bytes_all * 7 / 1024 / 1024 / 1024:.2f} GB")

        # CALIFORNIA ONLY
        if 'ca_valid_points' in locals() and num_ca_valid > 0:
            total_bytes_ca = num_ca_valid * num_times * bytes_per_record

            logger.info(f"\nCALIFORNIA ONLY - Records per forecast run:")
            logger.info(f"  {num_ca_valid:,} ocean points × {num_times} time steps = {num_ca_valid * num_times:,} records")
            logger.info(f"\nCALIFORNIA ONLY - Storage (per forecast run):")
            logger.info(f"  Raw data: {total_bytes_ca / 1024 / 1024:.1f} MB")
            logger.info(f"  With 7-day retention: {total_bytes_ca * 7 / 1024 / 1024 / 1024:.2f} GB")

        # Current beach-based approach
        logger.info(f"\n" + "=" * 80)
        logger.info("COMPARISON TO BEACH-BASED APPROACH")
        logger.info("=" * 80)
        logger.info(f"Current (beaches): ~400 beaches × {num_times} times = {400 * num_times:,} records")
        logger.info(f"Grid-based (All West Coast): {num_ocean_points:,} points × {num_times} times = {num_ocean_points * num_times:,} records")
        logger.info(f"  → {(num_ocean_points * num_times) / (400 * num_times):.1f}x more records")

        if 'ca_valid_points' in locals() and num_ca_valid > 0:
            logger.info(f"\nGrid-based (California only): {num_ca_valid:,} points × {num_times} times = {num_ca_valid * num_times:,} records")
            logger.info(f"  → {(num_ca_valid * num_times) / (400 * num_times):.1f}x more records")

            if 'nearshore_count' in locals() and nearshore_count > 0:
                total_bytes_nearshore = nearshore_count * num_times * bytes_per_record
                logger.info(f"\nGrid-based (California nearshore only, ~25 miles): {nearshore_count:,} points × {num_times} times = {nearshore_count * num_times:,} records")
                logger.info(f"  → {(nearshore_count * num_times) / (400 * num_times):.1f}x more records")
                logger.info(f"  Storage: {total_bytes_nearshore / 1024 / 1024:.1f} MB per run, {total_bytes_nearshore * 7 / 1024 / 1024 / 1024:.2f} GB for 7 days")

    logger.info("\n" + "=" * 80)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 80)

    ds.close()

if __name__ == "__main__":
    try:
        analyze_gfswave_grid()
    except Exception as e:
        logger.error(f"Error analyzing grid: {e}")
        import traceback
        traceback.print_exc()
