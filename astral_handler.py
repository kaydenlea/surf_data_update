#!/usr/bin/env python3
"""
Astral-based Astronomical Calculations Handler
Uses the Astral library (which implements NOAA algorithms) for local sunrise, sunset, and moon phase calculations.
No external API calls - all calculations are done locally using astronomical algorithms.

Astral library: https://astral.readthedocs.io/
Based on NOAA Solar Calculator equations: https://www.esrl.noaa.gov/gmd/grad/solcalc/
"""

from typing import List, Dict, Optional
from datetime import datetime, timedelta, date
import pytz
from astral import LocationInfo
from astral.sun import sun
from astral.moon import phase

from config import logger


def get_sun_moon_data_local(lat: float, lon: float, target_date: date, location_name: str = "Location") -> Dict:
    """
    Calculate sun/moon data for a specific location and date using local astronomical algorithms.
    No external API calls required - all calculations done locally.

    Args:
        lat: Latitude
        lon: Longitude
        target_date: Date object
        location_name: Name of the location (for logging)

    Returns:
        Dict with sunrise, sunset, moon_phase
    """
    try:
        # Create location info
        location = LocationInfo(
            name=location_name,
            region="",
            timezone="America/Los_Angeles",
            latitude=lat,
            longitude=lon
        )

        # Calculate sun times for this location and date
        # Returns dict with: dawn, sunrise, noon, sunset, dusk
        sun_data = sun(location.observer, date=target_date, tzinfo=pytz.timezone("America/Los_Angeles"))

        # Calculate moon phase (returns value 0-27.99, where 0=New Moon, 14=Full Moon)
        moon_phase_value = phase(target_date)

        # Convert moon phase to 0-1 scale (0=New, 0.5=Full)
        # Astral uses 0-28 day cycle, convert to 0-1
        moon_phase_normalized = moon_phase_value / 28.0

        # Format times to HH:MM
        sunrise = sun_data['sunrise'].strftime("%H:%M") if sun_data.get('sunrise') else None
        sunset = sun_data['sunset'].strftime("%H:%M") if sun_data.get('sunset') else None

        return {
            "sunrise": sunrise,
            "sunset": sunset,
            "moon_phase": round(moon_phase_normalized, 3),
        }

    except Exception as e:
        logger.error(f"   Astral: Failed to calculate data for {lat},{lon} on {target_date}: {e}")
        return {
            "sunrise": None,
            "sunset": None,
            "moon_phase": None,
        }


def update_daily_conditions_astral(counties: List[Dict]) -> List[Dict]:
    """
    Calculate daily sun/moon conditions using local Astral library.
    Replacement for USNO API - no external API calls, all calculations done locally.

    Args:
        counties: List of county dicts with county, latitude, longitude

    Returns:
        List of daily condition records ready for database upsert
    """
    logger.info("   Astral: calculating daily sun/moon data locally...")

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

        logger.info(f"   Astral: processing {county} ({idx + 1}/{total_counties})")

        # Process each day
        current_date = today
        while current_date <= end_date:
            # Calculate data for this day (no API call - all local calculations)
            data = get_sun_moon_data_local(lat, lon, current_date, county)

            all_rows.append({
                "county": county,
                "date": current_date.strftime("%Y-%m-%d"),
                "moon_phase": data.get("moon_phase"),
                "sunrise": data.get("sunrise"),
                "sunset": data.get("sunset"),
            })

            current_date += timedelta(days=1)

    logger.info(f"   Astral: calculated {len(all_rows)} daily records")
    return all_rows


def test_astral_calculation() -> bool:
    """Test Astral library calculations."""
    try:
        logger.info("Testing Astral astronomical calculations...")
        # Test with San Francisco coordinates
        today = datetime.now().date()
        data = get_sun_moon_data_local(37.7749, -122.4194, today, "San Francisco")

        if data and data.get("sunrise"):
            logger.info(f"Astral calculation successful - sunrise: {data['sunrise']}, sunset: {data['sunset']}, moon phase: {data['moon_phase']}")
            return True
        else:
            logger.error("Astral calculation failed - no sunrise data")
            return False
    except Exception as e:
        logger.error(f"Astral calculation test failed: {e}")
        return False
