#!/usr/bin/env python3
"""
NOAA CO-OPS Tides & Currents API Handler
Free, public domain data source for tide predictions and water temperature

API Documentation: https://api.tidesandcurrents.noaa.gov/api/prod/
No API key required - free for commercial use
"""

import time
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
import pytz
import math

from config import logger, TIDE_ADJUSTMENT_FT
from utils import safe_float, celsius_to_fahrenheit

COOPS_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

# California coastal tide stations (ID: (lat, lon, name))
CA_TIDE_STATIONS = {
    "9410170": (32.7150, -117.1733, "San Diego"),
    "9410660": (32.9506, -117.2569, "La Jolla"),
    "9410840": (33.1933, -117.3850, "Oceanside"),
    "9411340": (33.3700, -117.5650, "Santa Barbara"),
    "9412110": (33.7200, -118.2717, "Los Angeles"),
    "9413450": (33.7517, -118.1950, "Long Beach"),
    "9414290": (34.4681, -120.0133, "Santa Barbara Harbor"),
    "9414750": (34.9228, -120.6350, "Point Arguello"),
    "9415020": (36.6050, -121.8883, "Monterey"),
    "9415144": (36.9517, -122.0267, "Santa Cruz"),
    "9414863": (37.7722, -122.4650, "San Francisco"),
    "9415316": (37.9267, -122.5217, "Point Reyes"),
    "9416841": (38.9033, -123.7150, "Arena Cove"),
    "9418767": (41.7433, -124.2117, "Crescent City"),
}


def find_nearest_tide_station(lat: float, lon: float, max_distance_km: float = 200) -> Optional[Tuple[str, float]]:
    """
    Find the nearest NOAA tide station to a given location.
    IMPROVED: More lenient distance threshold for bays/harbors (200km default).

    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        max_distance_km: Maximum acceptable distance in km (default 200km)

    Returns:
        Tuple of (station_id, distance_km) or None if too far
    """
    min_dist = float("inf")
    nearest_station = None

    for station_id, (s_lat, s_lon, name) in CA_TIDE_STATIONS.items():
        # Simple distance calculation (good enough for this purpose)
        dist = math.sqrt((lat - s_lat)**2 + (lon - s_lon)**2) * 111  # rough km conversion

        if dist < min_dist:
            min_dist = dist
            nearest_station = station_id

    # IMPROVED: Increased from 100km to 200km to better serve inland bays/harbors
    # This ensures even remote locations get tide data from the nearest available station
    if min_dist > max_distance_km:
        logger.debug(f"   NOAA Tides: Nearest station is {min_dist:.1f}km away (max: {max_distance_km}km)")
        return None

    logger.debug(f"   NOAA Tides: Using station {nearest_station} at {min_dist:.1f}km distance")
    return (nearest_station, min_dist)


def get_tide_predictions(
    station_id: str,
    begin_date: str,
    end_date: str,
    datum: str = "MLLW"
) -> List[Dict]:
    """
    Fetch hourly tide predictions from NOAA CO-OPS.

    Args:
        station_id: NOAA station ID
        begin_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        datum: Tidal datum (MLLW, MSL, etc.)

    Returns:
        List of dicts with 't' (time) and 'v' (value in feet)
    """
    params = {
        "product": "predictions",
        "application": "SurfForecastApp",
        "station": station_id,
        "datum": datum,
        "units": "english",
        "time_zone": "lst_ldt",  # Local standard/daylight time
        "format": "json",
        "begin_date": begin_date,
        "end_date": end_date,
        "interval": "h"  # Hourly
    }

    try:
        response = requests.get(COOPS_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logger.warning(f"   NOAA Tides: API error for station {station_id}: {data['error']}")
            return []

        return data.get("predictions", [])

    except requests.exceptions.RequestException as e:
        logger.error(f"   NOAA Tides: Failed to fetch predictions for {station_id}: {e}")
        return []


def get_water_temperature(
    station_id: str,
    begin_date: str,
    end_date: str
) -> List[Dict]:
    """
    Fetch water temperature observations from NOAA CO-OPS.

    Returns:
        List of dicts with 't' (time) and 'v' (value in Celsius)
    """
    params = {
        "product": "water_temperature",
        "application": "SurfForecastApp",
        "station": station_id,
        "units": "metric",  # Returns Celsius
        "time_zone": "lst_ldt",
        "format": "json",
        "begin_date": begin_date,
        "end_date": end_date,
    }

    try:
        response = requests.get(COOPS_BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            # Water temp not available at all stations
            return []

        return data.get("data", [])

    except requests.exceptions.RequestException:
        # Water temp often not available, so don't log error
        return []


def get_noaa_tides_supplement_data(
    beaches: List[Dict],
    existing_records: List[Dict]
) -> List[Dict]:
    """
    Supplement missing tide and water temperature fields using NOAA CO-OPS.

    Args:
        beaches: List of beach dicts with id, LATITUDE, LONGITUDE
        existing_records: List of forecast records with potential null fields

    Returns:
        Updated records with tide/water temp data filled in
    """
    logger.info("   NOAA Tides supplement: fetching tide predictions...")

    # Build mapping of all timestamps by beach (fill everything, not just missing)
    needed_by_beach = {}

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_iso = rec.get("timestamp")
        if not bid or not ts_iso:
            continue

        if bid not in needed_by_beach:
            needed_by_beach[bid] = set()
        needed_by_beach[bid].add(ts_iso)

    if not needed_by_beach:
        logger.info("   NOAA Tides supplement: no records to process")
        return existing_records

    # Build index for quick updates
    updated_records = list(existing_records)
    key_to_index = {f"{r['beach_id']}_{r['timestamp']}": idx for idx, r in enumerate(updated_records)}

    # Find date range needed
    pacific = pytz.timezone("America/Los_Angeles")
    all_timestamps = set()
    for timestamps in needed_by_beach.values():
        all_timestamps.update(timestamps)

    timestamps_dt = [datetime.fromisoformat(ts) for ts in all_timestamps]
    min_time = min(timestamps_dt)
    max_time = max(timestamps_dt)

    begin_date = min_time.strftime("%Y%m%d")
    end_date = (max_time + timedelta(days=1)).strftime("%Y%m%d")

    logger.info(f"   NOAA Tides: fetching data from {begin_date} to {end_date}")

    # Process each beach
    target_beaches = [b for b in beaches if b["id"] in needed_by_beach]
    logger.info(f"   NOAA Tides supplement: processing {len(target_beaches)} beaches...")

    # Group beaches by nearest station to avoid duplicate API calls
    beaches_by_station = {}
    for beach in target_beaches:
        lat = beach["LATITUDE"]
        lon = beach["LONGITUDE"]

        station_info = find_nearest_tide_station(lat, lon)
        if not station_info:
            continue

        station_id, distance = station_info
        if station_id not in beaches_by_station:
            beaches_by_station[station_id] = []
        beaches_by_station[station_id].append(beach)

    logger.info(f"   NOAA Tides: using {len(beaches_by_station)} tide stations")

    filled_count = 0

    # Fetch data per station (much more efficient)
    for station_id, station_beaches in beaches_by_station.items():
        # Fetch tide predictions (no rate limiting - NOAA CO-OPS has generous limits)
        tide_data = get_tide_predictions(station_id, begin_date, end_date)

        # Fetch water temperature
        water_temp_data = get_water_temperature(station_id, begin_date, end_date)

        # Build lookup tables
        tide_by_time = {}
        for entry in tide_data:
            try:
                t_str = entry["t"]
                v_str = entry["v"]

                # Parse time (already in local timezone from API)
                dt = datetime.strptime(t_str, "%Y-%m-%d %H:%M")
                dt_local = pacific.localize(dt)

                # Align to 3-hour intervals (DST-safe using Timestamp constructor)
                local_hour = dt_local.hour
                pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
                closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))

                # Use Timestamp constructor to handle DST properly
                clean_time = pd.Timestamp(
                    year=dt_local.year,
                    month=dt_local.month,
                    day=dt_local.day,
                    hour=closest_interval,
                    minute=0,
                    second=0,
                    tz="America/Los_Angeles"
                )

                ts_iso = clean_time.isoformat()
                tide_by_time[ts_iso] = float(v_str) + TIDE_ADJUSTMENT_FT

            except (ValueError, KeyError) as e:
                continue

        water_temp_by_time = {}
        for entry in water_temp_data:
            try:
                t_str = entry["t"]
                v_str = entry["v"]

                dt = datetime.strptime(t_str, "%Y-%m-%d %H:%M")
                dt_local = pacific.localize(dt)

                # Align to 3-hour intervals (DST-safe using Timestamp constructor)
                local_hour = dt_local.hour
                pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
                closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))

                # Use Timestamp constructor to handle DST properly
                clean_time = pd.Timestamp(
                    year=dt_local.year,
                    month=dt_local.month,
                    day=dt_local.day,
                    hour=closest_interval,
                    minute=0,
                    second=0,
                    tz="America/Los_Angeles"
                )

                ts_iso = clean_time.isoformat()
                # Convert Celsius to Fahrenheit
                water_temp_by_time[ts_iso] = celsius_to_fahrenheit(float(v_str))

            except (ValueError, KeyError):
                continue

        # Apply to all beaches using this station
        for beach in station_beaches:
            bid = beach["id"]

            for ts_iso in needed_by_beach[bid]:
                key = f"{bid}_{ts_iso}"
                idx = key_to_index.get(key)
                if idx is None:
                    continue

                rec = updated_records[idx]

                # Fill tide level (overwrite existing data)
                if ts_iso in tide_by_time:
                    rec["tide_level_ft"] = safe_float(tide_by_time[ts_iso])
                    filled_count += 1

                # Fill water temperature (overwrite existing data)
                if ts_iso in water_temp_by_time:
                    rec["water_temp_f"] = safe_float(water_temp_by_time[ts_iso])
                    filled_count += 1

    logger.info(f"   NOAA Tides supplement: filled {filled_count} field values")
    return updated_records


def test_noaa_tides_connection() -> bool:
    """Test NOAA CO-OPS API connectivity."""
    try:
        logger.info("Testing NOAA CO-OPS API connection...")
        # Test with San Diego station
        today = datetime.now().strftime("%Y%m%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

        predictions = get_tide_predictions("9410170", today, tomorrow)
        if predictions:
            logger.info("NOAA CO-OPS API connection successful")
            return True
        else:
            logger.error("NOAA CO-OPS API connection failed - no predictions")
            return False
    except Exception as e:
        logger.error(f"NOAA CO-OPS API connection test failed: {e}")
        return False
