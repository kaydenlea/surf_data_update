#!/usr/bin/env python3
"""
USNO Astronomical Applications API Handler
Free, public domain data source for sun/moon rise/set times and moon phases
Replaces Visual Crossing for daily astronomical data

API Documentation: https://aa.usno.navy.mil/data/api
No API key required - free for commercial use
"""

import time
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz

from config import logger, BATCH_SIZE
from utils import chunk_iter

USNO_BASE_URL = "https://aa.usno.navy.mil/api"


def get_sun_moon_data(lat: float, lon: float, date_str: str) -> Optional[Dict]:
    """
    Fetch sun/moon data for a specific location and date from USNO.

    Args:
        lat: Latitude
        lon: Longitude
        date_str: Date in YYYY-MM-DD format

    Returns:
        Dict with sunrise, sunset, moonrise, moonset, moon_phase
    """
    # USNO API endpoint for rise/set times
    url = f"{USNO_BASE_URL}/rstt/oneday"
    params = {
        "date": date_str,
        "coords": f"{lat:.4f},{lon:.4f}",
        "tz": -8  # Pacific Standard Time offset (will adjust for PDT automatically)
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            logger.warning(f"   USNO: API error for {lat},{lon} on {date_str}: {data.get('error')}")
            return None

        # Extract data
        properties = data.get("properties", {})
        sun_data = properties.get("data", {}).get("sundata", [])
        moon_data = properties.get("data", {}).get("moondata", [])

        # Find sunrise and sunset
        sunrise = None
        sunset = None
        for entry in sun_data:
            phen = entry.get("phen", "").lower()
            time_val = entry.get("time", "")

            if "rise" in phen:
                sunrise = time_val
            elif "set" in phen:
                sunset = time_val

        # Find moonrise, moonset
        moonrise = None
        moonset = None
        for entry in moon_data:
            phen = entry.get("phen", "").lower()
            time_val = entry.get("time", "")

            if "rise" in phen:
                moonrise = time_val
            elif "set" in phen:
                moonset = time_val

        # Get moon phase
        moon_phase = properties.get("curphase", "")

        return {
            "sunrise": sunrise,
            "sunset": sunset,
            "moonrise": moonrise,
            "moonset": moonset,
            "moon_phase_name": moon_phase,
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"   USNO: Failed to fetch data for {lat},{lon}: {e}")
        return None


def convert_moon_phase_to_value(phase_name: str) -> Optional[float]:
    """
    Convert USNO moon phase name to a numeric value (0-1).

    0.0 = New Moon
    0.25 = First Quarter
    0.5 = Full Moon
    0.75 = Last Quarter
    """
    if not phase_name:
        return None

    phase_lower = phase_name.lower()

    if "new" in phase_lower:
        return 0.0
    elif "first quarter" in phase_lower or "first qtr" in phase_lower:
        return 0.25
    elif "full" in phase_lower:
        return 0.5
    elif "last quarter" in phase_lower or "last qtr" in phase_lower:
        return 0.75
    elif "waxing crescent" in phase_lower:
        return 0.125
    elif "waxing gibbous" in phase_lower:
        return 0.375
    elif "waning gibbous" in phase_lower:
        return 0.625
    elif "waning crescent" in phase_lower:
        return 0.875

    return None


def format_time_hhmm(time_str: Optional[str]) -> Optional[str]:
    """
    Convert USNO time format to HH:MM format.
    USNO returns times like "06:30" already in correct format.
    """
    if not time_str:
        return None

    try:
        # USNO already returns HH:MM format
        # Just validate it
        parts = time_str.split(":")
        if len(parts) == 2:
            hours = int(parts[0])
            minutes = int(parts[1])
            if 0 <= hours < 24 and 0 <= minutes < 60:
                return f"{hours:02d}:{minutes:02d}"
    except (ValueError, AttributeError):
        pass

    return None


def update_daily_conditions_usno(counties: List[Dict]) -> List[Dict]:
    """
    Fetch daily sun/moon conditions using USNO API.
    Replacement for Visual Crossing daily conditions.

    Args:
        counties: List of county dicts with county, latitude, longitude

    Returns:
        List of daily condition records ready for database upsert
    """
    logger.info("   USNO: fetching daily sun/moon data...")

    # Calculate date range
    pacific = pytz.timezone("America/Los_Angeles")
    today = datetime.now(pacific).date()
    end_date = today + timedelta(days=6)  # 7 days total

    all_rows = []
    total_counties = len(counties)

    for idx, county_info in enumerate(counties):
        county = county_info["county"]
        lat = county_info["latitude"]
        lon = county_info["longitude"]

        logger.info(f"   USNO: processing {county} ({idx + 1}/{total_counties})")

        # Process each day
        current_date = today
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")

            # Fetch data for this day (no rate limiting - USNO is generous)
            data = get_sun_moon_data(lat, lon, date_str)

            if data:
                # Convert moon phase name to numeric value
                moon_phase = convert_moon_phase_to_value(data.get("moon_phase_name"))

                # Format times
                sunrise = format_time_hhmm(data.get("sunrise"))
                sunset = format_time_hhmm(data.get("sunset"))

                all_rows.append({
                    "county": county,
                    "date": date_str,
                    "moon_phase": moon_phase,
                    "sunrise": sunrise,
                    "sunset": sunset,
                })
            else:
                # If API fails, create placeholder with nulls
                all_rows.append({
                    "county": county,
                    "date": date_str,
                    "moon_phase": None,
                    "sunrise": None,
                    "sunset": None,
                })

            current_date += timedelta(days=1)

    logger.info(f"   USNO: fetched {len(all_rows)} daily records")
    return all_rows


def test_usno_connection() -> bool:
    """Test USNO API connectivity."""
    try:
        logger.info("Testing USNO API connection...")
        # Test with San Francisco coordinates
        today = datetime.now().strftime("%Y-%m-%d")
        data = get_sun_moon_data(37.7749, -122.4194, today)

        if data and data.get("sunrise"):
            logger.info("USNO API connection successful")
            return True
        else:
            logger.error("USNO API connection failed - no sunrise data")
            return False
    except Exception as e:
        logger.error(f"USNO API connection test failed: {e}")
        return False
