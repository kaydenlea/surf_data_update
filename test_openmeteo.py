#!/usr/bin/env python3
"""
Test script to run Open Meteo handler standalone
"""

from config import logger
from openmeteo_handler import get_openmeteo_supplement_data, test_openmeteo_connection
import pandas as pd
from datetime import datetime, timedelta
import pytz

def create_test_data():
    """Create some test beaches and records for Open Meteo to supplement."""

    # Sample beaches (grid points or actual beaches)
    beaches = [
        {
            "id": 1,
            "Name": "Test Beach 1",
            "LATITUDE": 33.7701,  # San Diego area
            "LONGITUDE": -118.1937
        },
        {
            "id": 2,
            "Name": "Test Beach 2",
            "LATITUDE": 37.8044,  # San Francisco area
            "LONGITUDE": -122.4653
        }
    ]

    # Create test records for next 16 days with 3-hour intervals
    pacific_tz = pytz.timezone("America/Los_Angeles")
    now = datetime.now(pacific_tz)
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

    records = []
    for beach in beaches:
        for day in range(16):  # 16 days
            for hour in [0, 3, 6, 9, 12, 15, 18, 21]:  # 3-hour intervals
                timestamp = start_time + timedelta(days=day, hours=hour)

                # Create a record with missing fields that Open Meteo should fill
                record = {
                    "beach_id": beach["id"],
                    "timestamp": timestamp.isoformat(),
                    # Leave these fields None so Open Meteo fills them
                    "temperature": None,
                    "weather": None,
                    "water_temp_f": None,
                    "wind_speed_mph": None,
                    "wind_gust_mph": None,
                    "pressure_inhg": None,
                    # Include some existing data
                    "surf_height_min_ft": 2.0,
                    "surf_height_max_ft": 4.0,
                }
                records.append(record)

    logger.info(f"Created {len(records)} test records across {len(beaches)} beaches")
    return beaches, records


def main():
    """Test Open Meteo handler standalone."""
    logger.info("=" * 80)
    logger.info("OPEN METEO HANDLER STANDALONE TEST")
    logger.info("=" * 80)

    # Test connection first
    logger.info("\n1. Testing Open Meteo API connection...")
    if not test_openmeteo_connection():
        logger.error("Open Meteo connection test failed!")
        return False

    # Create test data
    logger.info("\n2. Creating test data...")
    beaches, records = create_test_data()

    # Run Open Meteo supplement
    logger.info("\n3. Running Open Meteo supplement...")
    enhanced_records = get_openmeteo_supplement_data(beaches, records)

    # Analyze results
    logger.info("\n4. Analyzing results...")
    filled_count = {
        "temperature": 0,
        "weather": 0,
        "water_temp_f": 0,
        "wind_speed_mph": 0,
        "wind_gust_mph": 0,
        "pressure_inhg": 0,
    }

    for rec in enhanced_records:
        for field in filled_count.keys():
            if rec.get(field) is not None:
                filled_count[field] += 1

    logger.info(f"\n   Total records: {len(enhanced_records)}")
    logger.info(f"   Fields filled:")
    for field, count in filled_count.items():
        pct = (count / len(enhanced_records)) * 100 if enhanced_records else 0
        logger.info(f"     - {field}: {count}/{len(enhanced_records)} ({pct:.1f}%)")

    # Show sample records
    logger.info("\n5. Sample enhanced records:")
    for i, rec in enumerate(enhanced_records[:3]):  # Show first 3
        logger.info(f"\n   Record {i+1}:")
        logger.info(f"     Beach ID: {rec.get('beach_id')}")
        logger.info(f"     Timestamp: {rec.get('timestamp')}")
        logger.info(f"     Temperature: {rec.get('temperature')} F")
        logger.info(f"     Weather: {rec.get('weather')}")
        logger.info(f"     Water Temp: {rec.get('water_temp_f')} F")
        logger.info(f"     Wind Speed: {rec.get('wind_speed_mph')} mph")
        logger.info(f"     Wind Gust: {rec.get('wind_gust_mph')} mph")
        logger.info(f"     Pressure: {rec.get('pressure_inhg')} inHg")

    logger.info("\n" + "=" * 80)
    logger.info("TEST COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)

    return True


if __name__ == "__main__":
    try:
        success = main()
        exit_code = 0 if success else 1
        logger.info(f"\nExiting with code: {exit_code}")
        exit(exit_code)
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}", exc_info=True)
        exit(2)
