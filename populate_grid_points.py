#!/usr/bin/env python3
"""
Populate grid_points table with California nearshore grid points from NOAA GFSwave.
Extracts the 94 grid points within ~25 miles of the California coast.
"""

import numpy as np
from config import logger
from noaa_handler import get_noaa_dataset_url, load_noaa_dataset
from database import supabase


def get_region_name(lat):
    """Determine California coastal region based on latitude."""
    if lat >= 40:
        return "Northern California"
    elif lat >= 35:
        return "Central California"
    else:
        return "Southern California"


def get_coast_longitude(lat):
    """Approximate California coast longitude for a given latitude."""
    if lat >= 40:  # Northern CA
        return 235.8  # -124.2°W
    elif lat >= 35:  # Central CA
        return 237.6  # -122.4°W
    else:  # Southern CA
        return 242.5  # -117.5°W


def calculate_distance_from_coast(lat, lon):
    """
    Calculate approximate distance from California coastline in miles.
    Uses longitude difference and latitude-adjusted conversion.
    """
    coast_lon = get_coast_longitude(lat)
    lon_diff = coast_lon - lon

    # Convert longitude degrees to miles (varies by latitude)
    # At equator: 1° ≈ 69 miles, but decreases with cos(latitude)
    miles_per_degree_lon = 69 * np.cos(np.radians(lat))

    distance_miles = abs(lon_diff * miles_per_degree_lon)
    return round(distance_miles, 2)


def extract_nearshore_grid_points():
    """
    Extract California nearshore grid points from NOAA GFSwave dataset.
    Returns list of dicts with grid point information.
    """
    logger.info("=" * 80)
    logger.info("EXTRACTING CALIFORNIA NEARSHORE GRID POINTS")
    logger.info("=" * 80)

    # Load GFSwave dataset
    logger.info("Loading NOAA GFSwave dataset...")
    url = get_noaa_dataset_url()
    ds = load_noaa_dataset(url)

    if ds is None:
        logger.error("Failed to load NOAA dataset")
        return []

    # Get coordinate arrays
    if 'latitude' in ds.dims:
        lat_var = 'latitude'
        lon_var = 'longitude'
    else:
        lat_var = 'lat'
        lon_var = 'lon'

    lats = ds[lat_var].values
    lons = ds[lon_var].values

    logger.info(f"Grid dimensions: {len(lats)} × {len(lons)} = {len(lats) * len(lons):,} total cells")

    # Get valid ocean points (non-NaN in swell_1)
    if 'swell_1' not in ds.data_vars:
        logger.error("swell_1 variable not found in dataset")
        ds.close()
        return []

    swell_data = ds['swell_1'].isel(time=0).values
    valid_points = ~np.isnan(swell_data)

    # California boundaries
    CA_LAT_MIN = 32.5  # San Diego
    CA_LAT_MAX = 42.0  # Oregon border
    CA_LON_MIN = 235.5  # -124.5°W
    CA_LON_MAX = 243.0  # -117.0°W

    logger.info(f"California boundaries: {CA_LAT_MIN}°-{CA_LAT_MAX}°N, {CA_LON_MIN}°-{CA_LON_MAX}°E")

    # Create California region mask
    lat_mask = (lats >= CA_LAT_MIN) & (lats <= CA_LAT_MAX)
    lon_mask = (lons >= CA_LON_MIN) & (lons <= CA_LON_MAX)

    lat_mask_2d = lat_mask[:, np.newaxis]
    lon_mask_2d = lon_mask[np.newaxis, :]
    ca_region_mask = lat_mask_2d & lon_mask_2d

    # Apply to valid ocean points
    ca_valid_points = valid_points & ca_region_mask

    # Get California ocean point indices
    ca_lats_idx, ca_lons_idx = np.where(ca_valid_points)
    ca_valid_lats = lats[ca_lats_idx]
    ca_valid_lons = lons[ca_lons_idx]

    logger.info(f"Total California ocean points: {len(ca_valid_lats):,}")

    # Filter for nearshore points (within ~25 miles of coast)
    NEARSHORE_BUFFER_DEG = 0.5  # ~34 miles at 37°N (conservative)

    nearshore_points = []

    for i, (lat, lon) in enumerate(zip(ca_valid_lats, ca_valid_lons)):
        coast_lon = get_coast_longitude(lat)

        # Check if within buffer of coast
        if lon <= coast_lon and lon >= (coast_lon - NEARSHORE_BUFFER_DEG):
            lat_idx = ca_lats_idx[i]
            lon_idx = ca_lons_idx[i]

            point = {
                'latitude': float(lat),
                'longitude': float(lon),
                'latitude_index': int(lat_idx),
                'longitude_index': int(lon_idx),
                'region': get_region_name(lat),
                'distance_from_coast_miles': calculate_distance_from_coast(lat, lon)
            }
            nearshore_points.append(point)

    ds.close()

    logger.info(f"Extracted {len(nearshore_points)} nearshore grid points (within ~25 miles)")

    # Sort by latitude (north to south) then longitude (west to east)
    nearshore_points.sort(key=lambda p: (-p['latitude'], p['longitude']))

    return nearshore_points


def populate_grid_points_table(grid_points):
    """
    Populate grid_points table in Supabase with extracted grid points.
    Uses upsert to avoid duplicates.
    """
    if not grid_points:
        logger.error("No grid points to insert")
        return 0

    logger.info("=" * 80)
    logger.info("POPULATING GRID_POINTS TABLE")
    logger.info("=" * 80)

    try:
        # Clear existing data first (optional - comment out if you want to preserve existing data)
        logger.info("Clearing existing grid_points data...")
        try:
            supabase.table("grid_points").delete().neq('id', 0).execute()
            logger.info("Existing data cleared")
        except Exception as e:
            logger.warning(f"Could not clear existing data (table may be empty): {e}")

        # Insert new grid points
        logger.info(f"Inserting {len(grid_points)} grid points...")

        # Supabase has a limit on batch size, so chunk the inserts
        CHUNK_SIZE = 100
        total_inserted = 0

        for i in range(0, len(grid_points), CHUNK_SIZE):
            chunk = grid_points[i:i + CHUNK_SIZE]

            # Use upsert to handle duplicates
            response = supabase.table("grid_points").upsert(
                chunk,
                on_conflict="latitude,longitude"
            ).execute()

            total_inserted += len(chunk)
            logger.info(f"   Inserted chunk {i//CHUNK_SIZE + 1}: {len(chunk)} points")

        logger.info(f"Successfully inserted {total_inserted} grid points")

        # Verify insertion
        count_response = supabase.table("grid_points").select("id", count="exact").execute()
        db_count = count_response.count if hasattr(count_response, 'count') else len(count_response.data)

        logger.info(f"Verification: Database now contains {db_count} grid points")

        return total_inserted

    except Exception as e:
        logger.error(f"Error populating grid_points table: {e}")
        import traceback
        traceback.print_exc()
        return 0


def display_summary(grid_points):
    """Display summary statistics of grid points."""
    if not grid_points:
        return

    logger.info("=" * 80)
    logger.info("GRID POINTS SUMMARY")
    logger.info("=" * 80)

    # Count by region
    regions = {}
    for point in grid_points:
        region = point['region']
        regions[region] = regions.get(region, 0) + 1

    logger.info(f"Total points: {len(grid_points)}")
    logger.info("\nBy region:")
    for region, count in sorted(regions.items()):
        logger.info(f"  {region}: {count} points")

    # Coordinate ranges
    lats = [p['latitude'] for p in grid_points]
    lons = [p['longitude'] for p in grid_points]
    distances = [p['distance_from_coast_miles'] for p in grid_points]

    logger.info(f"\nCoordinate ranges:")
    logger.info(f"  Latitude: {min(lats):.4f}° to {max(lats):.4f}°")
    logger.info(f"  Longitude: {min(lons):.4f}° to {max(lons):.4f}°")
    logger.info(f"  Distance from coast: {min(distances):.2f} to {max(distances):.2f} miles")
    logger.info(f"  Average distance: {np.mean(distances):.2f} miles")

    # Sample points
    logger.info(f"\nSample points (first 5):")
    for i, point in enumerate(grid_points[:5], 1):
        logger.info(f"  {i}. Lat: {point['latitude']:.4f}°, Lon: {point['longitude']:.4f}° "
                   f"({point['longitude'] - 360:.1f}°W), {point['region']}, "
                   f"{point['distance_from_coast_miles']:.1f} mi from coast")


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("CALIFORNIA NEARSHORE GRID POINTS POPULATION SCRIPT")
    logger.info("=" * 80)
    logger.info("")

    try:
        # Step 1: Extract grid points from GFSwave
        grid_points = extract_nearshore_grid_points()

        if not grid_points:
            logger.error("Failed to extract grid points")
            return False

        # Step 2: Display summary
        display_summary(grid_points)

        # Step 3: Populate database
        inserted_count = populate_grid_points_table(grid_points)

        if inserted_count > 0:
            logger.info("=" * 80)
            logger.info("SUCCESS: Grid points table populated successfully!")
            logger.info("=" * 80)
            return True
        else:
            logger.error("Failed to populate grid_points table")
            return False

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import sys

    try:
        success = main()
        exit_code = 0 if success else 1

        if success:
            logger.info("[OK] Script completed successfully")
        else:
            logger.info("[FAIL] Script completed with errors")

        sys.exit(exit_code)

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        sys.exit(2)
