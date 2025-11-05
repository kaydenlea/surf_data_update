#!/usr/bin/env python3
"""
Check which grid points have water temperature data in the database
"""

from config import logger
from database import supabase
from noaa_grid_handler import fetch_grid_points_from_db


def check_coverage():
    """Check water temperature coverage by grid point."""

    # Fetch grid points
    grid_points = fetch_grid_points_from_db()
    logger.info(f"Checking water temperature coverage for {len(grid_points)} grid points\n")

    # Check each grid point
    has_data = []
    no_data = []

    for gp in grid_points:
        grid_id = gp['id']

        # Count records with water_temp_f for this grid point
        response = supabase.table("grid_forecast_data")\
            .select("water_temp_f", count="exact")\
            .eq("grid_id", grid_id)\
            .not_.is_("water_temp_f", "null")\
            .execute()

        count = response.count if hasattr(response, 'count') else 0

        if count > 0:
            has_data.append((grid_id, count, gp.get('region', 'unknown')))
            logger.info(f"Grid {grid_id:3d} ({gp.get('region', 'unknown'):25s}): {count:4d} records with water_temp")
        else:
            no_data.append((grid_id, gp.get('region', 'unknown')))

    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Grid points WITH water temperature data: {len(has_data)}")
    logger.info(f"Grid points WITHOUT water temperature data: {len(no_data)}")
    logger.info(f"Coverage: {len(has_data)/len(grid_points)*100:.1f}%")

    if has_data:
        logger.info("\nGrid points WITH data:")
        for grid_id, count, region in has_data:
            logger.info(f"  - Grid {grid_id}: {count} records ({region})")

    if no_data:
        logger.info(f"\nGrid points WITHOUT data ({len(no_data)} total):")
        regions_without_data = {}
        for grid_id, region in no_data:
            if region not in regions_without_data:
                regions_without_data[region] = []
            regions_without_data[region].append(grid_id)

        for region, grid_ids in sorted(regions_without_data.items()):
            logger.info(f"  - {region}: {len(grid_ids)} grid points")


if __name__ == "__main__":
    try:
        check_coverage()
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        exit(1)
