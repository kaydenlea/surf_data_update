#!/usr/bin/env python3
"""
NOAA CO-OPS Tide Updater for Waves & Waders.

Pulls tide predictions from NOAA CO-OPS API at 6-minute intervals, grouped by county.
Each county uses the nearest NOAA tide station. Data is stored in `county_tides_15min`
table (one row per county per timestamp, not per beach). Starting at 12:00 AM of the
current Pacific day for DAYS_FORECAST days.

Advantages over Open-Meteo beach-based approach:
  - Uses official NOAA tide predictions (more accurate)
  - 6-minute granularity instead of hourly (10x more data points)
  - Free, public domain, no API key required
  - County-based storage = 99% less database storage (15 counties vs 1,336 beaches)
  - Faster queries - join beaches to county tides instead of querying per beach
"""

import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import pytz

from config import logger, DAYS_FORECAST, TIDE_ADJUSTMENT_FT
from database import (
    fetch_all_beaches, fetch_all_counties,
    upsert_county_tide_data, delete_all_county_tide_data, delete_county_tide_data_before
)
from noaa_tides_handler import (
    find_nearest_tide_station, CA_TIDE_STATIONS
)
import requests

COOPS_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"


def pacific_midnight_today(now=None):
    """Get midnight today in Pacific timezone."""
    tz = pytz.timezone('America/Los_Angeles')
    now = now or datetime.now(tz)
    if now.tzinfo is None:
        now = tz.localize(now)
    else:
        now = now.astimezone(tz)
    today = now.date()
    return tz.localize(datetime.combine(today, datetime.min.time()))


def derive_date_range(days=DAYS_FORECAST, midnight=None):
    """
    Derive date range for tide predictions.

    Returns:
        Tuple of (begin_date, end_date) in YYYYMMDD format for NOAA API
    """
    base_start = midnight or pacific_midnight_today()
    if days < 1:
        raise ValueError('days must be >= 1 for tide date range')
    end = base_start + timedelta(days=days)
    # NOAA API uses YYYYMMDD format
    return base_start.strftime('%Y%m%d'), end.strftime('%Y%m%d')


def get_tide_predictions_15min(
    station_id: str,
    begin_date: str,
    end_date: str,
    datum: str = "MLLW"
) -> List[Dict]:
    """
    Fetch 6-minute tide predictions from NOAA CO-OPS.

    Args:
        station_id: NOAA station ID
        begin_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        datum: Tidal datum (MLLW = Mean Lower Low Water)

    Returns:
        List of dicts with 't' (time) and 'v' (value in feet)
    """
    params = {
        "product": "predictions",
        "application": "SurfForecastApp",
        "station": station_id,
        "datum": datum,
        "units": "english",  # Returns feet
        "time_zone": "lst_ldt",  # Local standard/daylight time
        "format": "json",
        "begin_date": begin_date,
        "end_date": end_date,
        "interval": "6"  # 6 minutes is the finest granularity from NOAA
    }

    try:
        response = requests.get(COOPS_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logger.warning(f"   NOAA Tides: API error for station {station_id}: {data['error']}")
            return []

        predictions = data.get("predictions", [])

        # Return all 6-minute interval predictions (no filtering)
        logger.debug(f"   NOAA Tides: Station {station_id} returned {len(predictions)} predictions at 6-min intervals")
        return predictions

    except requests.exceptions.RequestException as e:
        logger.error(f"   NOAA Tides: Failed to fetch predictions for {station_id}: {e}")
        return []


def group_beaches_by_county(beaches: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group beaches by county.

    Returns:
        Dict mapping county name to list of beaches
    """
    by_county = {}
    for beach in beaches:
        # Use COUNTY field (all caps) from beaches table
        county = beach.get('COUNTY', beach.get('County', 'Unknown'))
        if county and county != 'Unknown':
            if county not in by_county:
                by_county[county] = []
            by_county[county].append(beach)
    return by_county


def get_county_center(beaches: List[Dict]) -> Tuple[float, float]:
    """Calculate the geographic center of beaches in a county."""
    if not beaches:
        return (0.0, 0.0)

    avg_lat = sum(b['LATITUDE'] for b in beaches) / len(beaches)
    avg_lon = sum(b['LONGITUDE'] for b in beaches) / len(beaches)
    return (avg_lat, avg_lon)


def update_tides_by_county(beaches: List[Dict], day_start) -> int:
    """
    Update tides using NOAA CO-OPS API, grouped by county.
    Each county uses the nearest NOAA tide station, then distributes to all beaches.

    Args:
        beaches: List of all beaches
        day_start: Pacific midnight datetime to start from

    Returns:
        Total number of tide records upserted
    """
    if not beaches:
        logger.error("TIDES: No beaches provided for tide update")
        return 0

    begin_date, end_date = derive_date_range(midnight=day_start)
    logger.info(f"TIDES: Fetching 6-minute tide predictions from {begin_date} through {end_date} (Pacific)")

    # Group beaches by county
    by_county = group_beaches_by_county(beaches)
    logger.info(f"TIDES: Processing {len(by_county)} counties covering {len(beaches)} beaches")

    total_records = 0
    pacific_tz = pytz.timezone('America/Los_Angeles')

    for county_name, county_beaches in by_county.items():
        logger.info(f"TIDES: Processing {county_name} County ({len(county_beaches)} beaches)")

        # Find nearest NOAA tide station for this county
        county_lat, county_lon = get_county_center(county_beaches)
        station_info = find_nearest_tide_station(county_lat, county_lon, max_distance_km=200)

        if not station_info:
            logger.warning(f"TIDES: No NOAA tide station found within 200km of {county_name} County, skipping")
            continue

        station_id, distance_km = station_info
        station_name = CA_TIDE_STATIONS.get(station_id, (0, 0, "Unknown"))[2]
        logger.info(f"   Using NOAA station {station_id} ({station_name}) at {distance_km:.1f}km from county center")

        # Fetch tide predictions for this station at 15-minute intervals
        predictions = get_tide_predictions_15min(station_id, begin_date, end_date)

        if not predictions:
            logger.warning(f"   No tide predictions received for {county_name} County")
            continue

        logger.info(f"   Received {len(predictions)} tide predictions at 6-minute intervals")

        # Build records for this county (one record per timestamp, not per beach)
        to_upsert = []
        for pred in predictions:
            try:
                # Parse timestamp (format: "YYYY-MM-DD HH:MM")
                timestamp_str = pred['t']
                tide_value_ft = float(pred['v'])

                # Convert to Pacific timezone-aware datetime
                timestamp_dt = pacific_tz.localize(datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M"))

                # Only keep timestamps on/after day_start
                if timestamp_dt < day_start:
                    continue

                # Apply tide adjustment
                adjusted_ft = tide_value_ft + TIDE_ADJUSTMENT_FT
                tide_level_m = adjusted_ft * 0.3048  # Convert feet to meters

                to_upsert.append({
                    "county": county_name,
                    "timestamp": timestamp_dt.isoformat(),
                    "tide_level_ft": adjusted_ft,
                    "tide_level_m": tide_level_m,
                    "station_id": station_id,
                    "station_name": station_name,
                })
            except (KeyError, ValueError) as e:
                logger.debug(f"   Failed to parse tide prediction: {e}")
                continue

        if to_upsert:
            inserted = upsert_county_tide_data(to_upsert)
            total_records += inserted
            logger.info(f"   ✓ Upserted {inserted} tide records for {county_name} County")

        # Small delay between counties to be respectful to NOAA API
        time.sleep(0.5)

    logger.info(f"TIDES: Successfully upserted {total_records} total tide records across {len(by_county)} counties")
    return total_records


def main():
    """Main execution: fetch all beaches, delete old tides, update with new predictions."""
    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("TIDES: No beaches found, aborting")
        return False

    day_start = pacific_midnight_today()

    # Optional delete control via env; default removes rows before today's midnight
    import os
    tide_delete_mode = os.environ.get("TIDE_DELETE", "outdated")

    if tide_delete_mode == "all":
        logger.info("TIDES: Deleting all existing county tide data")
        delete_all_county_tide_data()
    else:
        logger.info(f"TIDES: Deleting county tide data before {day_start.strftime('%Y-%m-%d %H:%M %Z')}")
        delete_county_tide_data_before(day_start)

    # Update tides using NOAA CO-OPS
    total = update_tides_by_county(beaches, day_start)

    if total > 0:
        logger.info(f"TIDES: Update completed successfully - {total} records")
        return True
    else:
        logger.error("TIDES: Update failed - no records inserted")
        return False


if __name__ == "__main__":
    ok = main()
    raise SystemExit(0 if ok else 1)
