#!/usr/bin/env python3
"""
Standalone NWS Test Script
Tests NWS supplement step independently with progress tracking
"""

import sys
from config import logger
from database import supabase
from nws_handler import get_nws_supplement_data

def main():
    """Run NWS supplement test standalone."""

    logger.info("=" * 80)
    logger.info("NWS STANDALONE TEST")
    logger.info("=" * 80)

    # Step 1: Fetch beaches with pagination
    logger.info("Step 1: Fetching beaches from database...")
    try:
        beaches = []
        page_size = 1000
        start = 0

        while True:
            end_idx = start + page_size - 1
            response = supabase.table('beaches').select('id,Name,LATITUDE,LONGITUDE').range(start, end_idx).execute()
            rows = response.data or []
            beaches.extend(rows)
            logger.info(f"   Fetched {len(rows)} beaches (batch {start//page_size + 1})")

            if len(rows) < page_size:
                break
            start += page_size

        logger.info(f"   Total: {len(beaches)} beaches")
    except Exception as e:
        logger.error(f"Failed to fetch beaches: {e}")
        return False

    # Step 2: Fetch real forecast data from database
    logger.info("Step 2: Fetching real forecast data from database...")
    try:
        forecast_records = []
        page_size = 1000
        start = 0

        while True:
            end_idx = start + page_size - 1
            response = supabase.table('forecast_data').select('*').range(start, end_idx).execute()
            rows = response.data or []
            forecast_records.extend(rows)
            logger.info(f"   Fetched {len(rows)} forecast records (batch {start//page_size + 1})")

            if len(rows) < page_size:
                break
            start += page_size

        logger.info(f"   Total: {len(forecast_records)} forecast records")
    except Exception as e:
        logger.error(f"Failed to fetch forecast data: {e}")
        return False

    # Step 3: Run NWS supplement
    logger.info("Step 3: Running NWS supplement (this will take 15-20 minutes)...")
    logger.info("")

    try:
        enhanced_records = get_nws_supplement_data(beaches, forecast_records)

        logger.info("")
        logger.info("=" * 80)
        logger.info("NWS SUPPLEMENT COMPLETE")
        logger.info("=" * 80)

        # Analyze results
        total_records = len(enhanced_records)
        fields_to_check = ["temperature", "weather", "wind_speed_mph", "wind_gust_mph"]

        completeness = {}
        for field in fields_to_check:
            filled = sum(1 for rec in enhanced_records if rec.get(field) is not None)
            completeness[field] = filled

        logger.info("Results:")
        for field, count in completeness.items():
            pct = (count / total_records * 100) if total_records > 0 else 0
            logger.info(f"  {field}: {count}/{total_records} ({pct:.1f}%)")

        return True

    except Exception as e:
        logger.error(f"NWS supplement failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
