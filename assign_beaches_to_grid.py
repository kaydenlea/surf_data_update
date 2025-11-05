#!/usr/bin/env python3
"""
Assign each beach to its nearest grid point.
Updates the grid_id column in the beaches table.
"""

import math
from config import logger
from database import supabase


def calculate_distance_miles(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points on Earth.
    Uses the Haversine formula.

    Args:
        lat1, lon1: First point coordinates (decimal degrees)
        lat2, lon2: Second point coordinates (decimal degrees)

    Returns:
        Distance in miles
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    # Earth's radius in miles
    radius_miles = 3959.0

    distance = radius_miles * c
    return distance


def normalize_longitude(lon):
    """
    Normalize longitude to 0-360 range (matching GFSwave convention).
    Converts -180 to 180 range to 0 to 360 range.

    Args:
        lon: Longitude in decimal degrees (can be -180 to 180 or 0 to 360)

    Returns:
        Longitude in 0-360 range
    """
    if lon < 0:
        return lon + 360
    return lon


def fetch_all_beaches():
    """Fetch all beaches from database with their coordinates."""
    logger.info("Fetching beaches from database...")

    try:
        # Fetch all beaches (Supabase has 1000 row default limit, so we need to paginate)
        all_beaches = []
        page_size = 1000
        offset = 0

        while True:
            response = supabase.table("beaches").select(
                "id, Name, LATITUDE, LONGITUDE, grid_id"
            ).range(offset, offset + page_size - 1).execute()

            if not response.data:
                break

            all_beaches.extend(response.data)

            # If we got less than page_size, we're done
            if len(response.data) < page_size:
                break

            offset += page_size

        # Normalize column names to lowercase for easier handling
        for beach in all_beaches:
            beach['name'] = beach.pop('Name', None)
            beach['latitude'] = beach.pop('LATITUDE', None)
            beach['longitude'] = beach.pop('LONGITUDE', None)

        logger.info(f"Found {len(all_beaches)} beaches")
        return all_beaches

    except Exception as e:
        logger.error(f"Error fetching beaches: {e}")
        return []


def fetch_all_grid_points():
    """Fetch all grid points from database with their coordinates."""
    logger.info("Fetching grid points from database...")

    try:
        response = supabase.table("grid_points").select(
            "id, latitude, longitude, region, distance_from_coast_miles"
        ).execute()

        grid_points = response.data
        logger.info(f"Found {len(grid_points)} grid points")
        return grid_points

    except Exception as e:
        logger.error(f"Error fetching grid points: {e}")
        return []


def find_nearest_grid_point(beach_lat, beach_lon, grid_points):
    """
    Find the nearest grid point to a beach.

    Args:
        beach_lat: Beach latitude (decimal degrees)
        beach_lon: Beach longitude (decimal degrees, can be -180 to 180)
        grid_points: List of grid point dicts with id, latitude, longitude

    Returns:
        Tuple of (grid_point_id, distance_miles)
    """
    # Normalize beach longitude to 0-360 range to match grid points
    beach_lon_normalized = normalize_longitude(beach_lon)

    min_distance = float('inf')
    nearest_grid_id = None

    for grid_point in grid_points:
        grid_lat = grid_point['latitude']
        grid_lon = grid_point['longitude']

        # Calculate distance
        distance = calculate_distance_miles(beach_lat, beach_lon_normalized, grid_lat, grid_lon)

        if distance < min_distance:
            min_distance = distance
            nearest_grid_id = grid_point['id']

    return nearest_grid_id, min_distance


def assign_beaches_to_grid_points():
    """
    Main function to assign each beach to its nearest grid point.
    Updates the grid_id column in the beaches table.
    """
    logger.info("=" * 80)
    logger.info("ASSIGNING BEACHES TO NEAREST GRID POINTS")
    logger.info("=" * 80)

    # Fetch data
    beaches = fetch_all_beaches()
    grid_points = fetch_all_grid_points()

    if not beaches:
        logger.error("No beaches found")
        return False

    if not grid_points:
        logger.error("No grid points found")
        return False

    # Process each beach
    logger.info(f"\nProcessing {len(beaches)} beaches...")

    updates = []
    statistics = {
        'updated': 0,
        'skipped_no_coords': 0,
        'skipped_already_assigned': 0,
        'distances': []
    }

    for beach in beaches:
        beach_id = beach['id']
        beach_name = beach['name']
        beach_lat = beach.get('latitude')
        beach_lon = beach.get('longitude')
        current_grid_id = beach.get('grid_id')

        # Skip if no coordinates
        if beach_lat is None or beach_lon is None:
            logger.warning(f"   Skipping {beach_name} (ID: {beach_id}) - no coordinates")
            statistics['skipped_no_coords'] += 1
            continue

        # Find nearest grid point
        nearest_grid_id, distance = find_nearest_grid_point(beach_lat, beach_lon, grid_points)

        if nearest_grid_id is None:
            logger.warning(f"   Could not find grid point for {beach_name} (ID: {beach_id})")
            continue

        # Log assignment
        logger.info(f"   {beach_name} (ID: {beach_id}) -> Grid Point {nearest_grid_id} ({distance:.2f} miles)")

        # Add to update list (store beach_id separately for matching)
        updates.append({
            'beach_id': beach_id,  # Store for WHERE clause
            'grid_id': nearest_grid_id
        })

        statistics['updated'] += 1
        statistics['distances'].append(distance)

    # Perform batch update
    if updates:
        logger.info(f"\nUpdating {len(updates)} beaches in database...")

        try:
            # Update each beach individually (Supabase doesn't support batch WHERE updates)
            total_updated = 0
            batch_count = 0

            for update in updates:
                beach_id = update['beach_id']
                grid_id = update['grid_id']

                # Update single beach
                supabase.table("beaches").update(
                    {'grid_id': grid_id}
                ).eq('id', beach_id).execute()

                total_updated += 1

                # Log progress every 100 updates
                if total_updated % 100 == 0:
                    batch_count += 1
                    logger.info(f"   Updated {total_updated} beaches...")

            logger.info(f"Successfully updated {total_updated} beaches")

        except Exception as e:
            logger.error(f"Error updating beaches: {e}")
            import traceback
            traceback.print_exc()
            return False

    # Print statistics
    logger.info("\n" + "=" * 80)
    logger.info("ASSIGNMENT STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Total beaches: {len(beaches)}")
    logger.info(f"Updated with grid_id: {statistics['updated']}")
    logger.info(f"Skipped (no coordinates): {statistics['skipped_no_coords']}")

    if statistics['distances']:
        distances = statistics['distances']
        logger.info(f"\nDistance from beach to nearest grid point:")
        logger.info(f"  Minimum: {min(distances):.2f} miles")
        logger.info(f"  Maximum: {max(distances):.2f} miles")
        logger.info(f"  Average: {sum(distances)/len(distances):.2f} miles")
        logger.info(f"  Median: {sorted(distances)[len(distances)//2]:.2f} miles")

    # Verify update
    logger.info("\nVerifying update...")
    try:
        response = supabase.table("beaches").select("id", count="exact").is_("grid_id", "null").execute()
        beaches_without_grid = response.count if hasattr(response, 'count') else len(response.data)

        logger.info(f"Beaches without grid_id: {beaches_without_grid}")

        if beaches_without_grid > 0:
            logger.warning(f"Warning: {beaches_without_grid} beaches still don't have a grid_id assigned")
            logger.warning("These beaches may be missing coordinates or are outside the grid coverage area")

    except Exception as e:
        logger.warning(f"Could not verify update: {e}")

    logger.info("=" * 80)
    logger.info("ASSIGNMENT COMPLETE")
    logger.info("=" * 80)

    return True


def display_sample_assignments():
    """Display sample beach-to-grid assignments for verification."""
    logger.info("\n" + "=" * 80)
    logger.info("SAMPLE BEACH-TO-GRID ASSIGNMENTS")
    logger.info("=" * 80)

    try:
        # Fetch beaches with their assigned grid points
        response = supabase.table("beaches").select(
            "id, Name, LATITUDE, LONGITUDE, grid_id"
        ).not_.is_("grid_id", "null").limit(10).execute()

        beaches = response.data

        # Normalize column names
        for beach in beaches:
            beach['name'] = beach.pop('Name', None)
            beach['latitude'] = beach.pop('LATITUDE', None)
            beach['longitude'] = beach.pop('LONGITUDE', None)

        if not beaches:
            logger.warning("No beach assignments found")
            return

        # Get grid point details for these beaches
        grid_ids = [b['grid_id'] for b in beaches if b.get('grid_id')]

        if grid_ids:
            grid_response = supabase.table("grid_points").select(
                "id, latitude, longitude, region"
            ).in_("id", grid_ids).execute()

            grid_points_map = {gp['id']: gp for gp in grid_response.data}

            logger.info(f"Showing first {len(beaches)} beach assignments:\n")

            for beach in beaches:
                beach_lat = beach.get('latitude')
                beach_lon = beach.get('longitude')
                grid_id = beach.get('grid_id')

                if grid_id and grid_id in grid_points_map:
                    grid_point = grid_points_map[grid_id]
                    grid_lat = grid_point['latitude']
                    grid_lon = grid_point['longitude']
                    region = grid_point['region']

                    # Calculate distance
                    beach_lon_normalized = normalize_longitude(beach_lon)
                    distance = calculate_distance_miles(beach_lat, beach_lon_normalized, grid_lat, grid_lon)

                    logger.info(f"Beach: {beach['name']}")
                    logger.info(f"  Beach coords: {beach_lat:.4f}°N, {beach_lon:.4f}°W")
                    logger.info(f"  Grid Point {grid_id}: {grid_lat:.4f}°N, {grid_lon-360:.4f}°W ({region})")
                    logger.info(f"  Distance: {distance:.2f} miles\n")

    except Exception as e:
        logger.error(f"Error displaying sample assignments: {e}")


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("BEACH-TO-GRID ASSIGNMENT SCRIPT")
    logger.info("=" * 80)
    logger.info("")

    try:
        # Assign beaches to grid points
        success = assign_beaches_to_grid_points()

        if not success:
            logger.error("Failed to assign beaches to grid points")
            return False

        # Display sample assignments
        display_sample_assignments()

        logger.info("\n" + "=" * 80)
        logger.info("SUCCESS: All beaches assigned to nearest grid points!")
        logger.info("=" * 80)

        return True

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
