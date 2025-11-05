#!/usr/bin/env python3
"""
Script to fill weather and temperature data in existing grid_forecast_data records using Open Meteo.
Fetches records from database, supplements with Open Meteo, and updates the database.
"""

from config import logger
from database import supabase, upsert_grid_forecast_data
from openmeteo_handler import get_openmeteo_supplement_data, test_openmeteo_connection
from noaa_grid_handler import fetch_grid_points_from_db
import pytz
from datetime import datetime, timedelta


def fetch_grid_forecast_records():
    """
    Fetch existing grid forecast records from database.
    Returns records that need weather/temperature data filled.
    """
    logger.info("Fetching existing grid forecast records from database...")

    try:
        # Fetch all grid forecast data (limit to next 16 days to match Open Meteo)
        pacific_tz = pytz.timezone("America/Los_Angeles")
        now = datetime.now(pacific_tz)
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=16)

        # Convert to ISO format for query
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()

        logger.info(f"   Fetching records from {start_date.date()} to {end_date.date()}")

        # Fetch records in batches
        all_records = []
        page_size = 1000
        start_idx = 0

        while True:
            response = (
                supabase.table("grid_forecast_data")
                .select("*")
                .gte("timestamp", start_iso)
                .lte("timestamp", end_iso)
                .range(start_idx, start_idx + page_size - 1)
                .execute()
            )

            records = response.data or []
            all_records.extend(records)

            logger.info(f"   Fetched batch: {len(records)} records (total so far: {len(all_records)})")

            if len(records) < page_size:
                break

            start_idx += page_size

        logger.info(f"   Total records fetched: {len(all_records)}")

        # Convert to format expected by Open Meteo handler
        # Rename grid_id to beach_id for compatibility
        for rec in all_records:
            rec["beach_id"] = rec["grid_id"]

        return all_records

    except Exception as e:
        logger.error(f"   Error fetching grid forecast records: {e}")
        return []


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("FILL OPEN METEO DATA IN DATABASE")
    logger.info("=" * 80)
    logger.info("")
    logger.info("This script will:")
    logger.info("  1. Fetch existing grid forecast records from database")
    logger.info("  2. Fill missing weather, temperature, and water_temp fields using Open Meteo")
    logger.info("  3. Update the database with enhanced data")
    logger.info("=" * 80)

    try:
        # Test Open Meteo connection
        logger.info("\n1. Testing Open Meteo API connection...")
        if not test_openmeteo_connection():
            logger.error("   Open Meteo connection failed!")
            return False
        logger.info("   Open Meteo connection successful")

        # Fetch grid points
        logger.info("\n2. Fetching grid points...")
        grid_points = fetch_grid_points_from_db()
        if not grid_points:
            logger.error("   No grid points found!")
            return False
        logger.info(f"   Found {len(grid_points)} grid points")

        # Convert grid points to beach-like format
        def _normalize_longitude(lon):
            try:
                lon_val = float(lon)
            except (TypeError, ValueError):
                return lon
            return lon_val - 360.0 if lon_val > 180.0 else lon_val

        grid_as_beaches = []
        for gp in grid_points:
            grid_as_beaches.append({
                "id": gp["id"],
                "Name": f"Grid Point {gp['id']}",
                "LATITUDE": gp["latitude"],
                "LONGITUDE": _normalize_longitude(gp["longitude"]),
            })

        # Fetch existing records
        logger.info("\n3. Fetching existing grid forecast records...")
        records = fetch_grid_forecast_records()
        if not records:
            logger.error("   No records found to update!")
            return False
        logger.info(f"   Found {len(records)} records")

        # Analyze what's missing before enhancement
        logger.info("\n4. Analyzing missing data...")
        missing_counts = {
            "temperature": 0,
            "weather": 0,
            "water_temp_f": 0,
            "wind_speed_mph": 0,
            "wind_gust_mph": 0,
            "pressure_inhg": 0,
        }

        for rec in records:
            for field in missing_counts.keys():
                if rec.get(field) is None:
                    missing_counts[field] += 1

        logger.info("   Missing field counts:")
        for field, count in missing_counts.items():
            pct = (count / len(records)) * 100 if records else 0
            logger.info(f"     - {field}: {count}/{len(records)} ({pct:.1f}%)")

        # Enhance with Open Meteo
        logger.info("\n5. Enhancing records with Open Meteo data...")
        enhanced_records = get_openmeteo_supplement_data(grid_as_beaches, records)

        # Rename beach_id back to grid_id
        for rec in enhanced_records:
            rec["grid_id"] = rec.pop("beach_id")

        # Analyze what was filled
        logger.info("\n6. Analyzing filled data...")
        filled_counts = {
            "temperature": 0,
            "weather": 0,
            "water_temp_f": 0,
            "wind_speed_mph": 0,
            "wind_gust_mph": 0,
            "pressure_inhg": 0,
        }

        for rec in enhanced_records:
            for field in filled_counts.keys():
                if rec.get(field) is not None:
                    filled_counts[field] += 1

        logger.info("   Filled field counts:")
        for field, count in filled_counts.items():
            pct = (count / len(enhanced_records)) * 100 if enhanced_records else 0
            logger.info(f"     - {field}: {count}/{len(enhanced_records)} ({pct:.1f}%)")

        # Update database
        logger.info("\n7. Updating database with enhanced records...")
        total_updated = upsert_grid_forecast_data(enhanced_records)
        logger.info(f"   Successfully updated {total_updated} records")

        # Show sample records
        logger.info("\n8. Sample enhanced records:")
        for i, rec in enumerate(enhanced_records[:3]):
            logger.info(f"\n   Record {i+1}:")
            logger.info(f"     Grid ID: {rec.get('grid_id')}")
            logger.info(f"     Timestamp: {rec.get('timestamp')}")
            logger.info(f"     Temperature: {rec.get('temperature')} F")
            logger.info(f"     Weather: {rec.get('weather')}")
            logger.info(f"     Water Temp: {rec.get('water_temp_f')} F")
            logger.info(f"     Wind Speed: {rec.get('wind_speed_mph')} mph")

        logger.info("\n" + "=" * 80)
        logger.info("DATABASE UPDATE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records processed: {len(enhanced_records)}")
        logger.info(f"Total records updated: {total_updated}")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    try:
        success = main()
        exit_code = 0 if success else 1
        logger.info(f"\nExiting with code: {exit_code}")
        exit(exit_code)
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        exit(2)
