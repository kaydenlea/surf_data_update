#!/usr/bin/env python3
"""
NOAA National Weather Service (NWS) API Handler
Free, public domain data source for weather forecasts
Replaces Open-Meteo for: temperature, weather, wind speed/gust, pressure

API Documentation: https://www.weather.gov/documentation/services-web-api
No API key required - free for commercial use
"""

import time
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz

from config import logger, BATCH_SIZE
from utils import chunk_iter, safe_float, safe_int

# NWS API requires a User-Agent header
USER_AGENT = "SurfForecastApp/1.0 (surf@example.com)"  # Customize this
NWS_BASE_URL = "https://api.weather.gov"

# Weather code mapping from NWS icon names to simple codes
NWS_WEATHER_CODES = {
    "skc": 0,  # Clear
    "few": 1,  # Few clouds
    "sct": 2,  # Scattered clouds
    "bkn": 3,  # Broken clouds
    "ovc": 3,  # Overcast
    "wind_skc": 0,
    "wind_few": 1,
    "wind_sct": 2,
    "wind_bkn": 3,
    "wind_ovc": 3,
    "rain": 61,  # Rain
    "rain_showers": 80,  # Showers
    "tsra": 95,  # Thunderstorm
    "snow": 71,  # Snow
    "sleet": 67,  # Freezing rain
    "fog": 45,  # Fog
}


def get_nws_gridpoint(lat: float, lon: float, timeout: int = 30, allow_fallback: bool = True) -> Optional[Dict]:
    """
    Get NWS grid point information for a location.
    IMPROVED: For bays/harbors without coverage, tries nearby offshore points.

    Returns dict with:
        - gridId: str
        - gridX: int
        - gridY: int
        - forecastHourly: str (URL)
    """
    headers = {"User-Agent": USER_AGENT}

    # Try exact location first
    url = f"{NWS_BASE_URL}/points/{lat:.4f},{lon:.4f}"

    try:
        response = requests.get(url, headers=headers, timeout=timeout)

        if response.status_code != 404:
            response.raise_for_status()
            data = response.json()

            props = data.get("properties", {})
            return {
                "gridId": props.get("gridId"),
                "gridX": props.get("gridX"),
                "gridY": props.get("gridY"),
                "forecastHourly": props.get("forecastHourly"),
            }

    except requests.exceptions.Timeout:
        logger.debug(f"   NWS: Timeout getting gridpoint for {lat},{lon}")
        if not allow_fallback:
            return None
    except requests.exceptions.RequestException as e:
        logger.debug(f"   NWS: Request failed for {lat},{lon}: {e}")
        if not allow_fallback:
            return None

    # FALLBACK: Try nearby points for bays/harbors
    if not allow_fallback:
        logger.debug(f"   NWS: No coverage for location {lat},{lon}")
        return None

    logger.debug(f"   NWS: Trying nearby points for {lat},{lon}")

    # Try progressively further offshore (west for CA coast = more negative longitude)
    fallback_offsets = [
        (0, -0.05), (0, -0.1), (0, -0.15),  # Nearby offshore
        (0.05, -0.1), (-0.05, -0.1),         # Diagonal offshore
        (0, -0.2), (0, -0.25),               # Further offshore
        (0.1, -0.2), (-0.1, -0.2),           # Diagonal further
    ]

    for dlat, dlon in fallback_offsets:
        fallback_lat = lat + dlat
        fallback_lon = lon + dlon
        url = f"{NWS_BASE_URL}/points/{fallback_lat:.4f},{fallback_lon:.4f}"

        try:
            response = requests.get(url, headers=headers, timeout=timeout)

            if response.status_code == 404:
                continue

            response.raise_for_status()
            data = response.json()

            props = data.get("properties", {})
            logger.debug(f"   NWS: Found coverage at offset ({dlat},{dlon}) for {lat},{lon}")
            return {
                "gridId": props.get("gridId"),
                "gridX": props.get("gridX"),
                "gridY": props.get("gridY"),
                "forecastHourly": props.get("forecastHourly"),
            }

        except (requests.exceptions.Timeout, requests.exceptions.RequestException):
            continue

    logger.debug(f"   NWS: No coverage found even with fallback for {lat},{lon}")
    return None


def get_nws_hourly_forecast(forecast_url: str, timeout: int = 30) -> List[Dict]:
    """
    Fetch hourly forecast from NWS.

    Returns list of periods, each containing:
        - startTime: ISO timestamp
        - temperature: int (F)
        - windSpeed: str (e.g., "10 mph")
        - windGust: str or None
        - shortForecast: str
        - probabilityOfPrecipitation: dict
    """
    headers = {"User-Agent": USER_AGENT}

    try:
        response = requests.get(forecast_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        return data.get("properties", {}).get("periods", [])

    except requests.exceptions.Timeout:
        logger.debug(f"   NWS: Timeout fetching hourly forecast")
        return []
    except requests.exceptions.RequestException as e:
        logger.debug(f"   NWS: Failed to fetch hourly forecast: {e}")
        return []


def parse_wind_speed(wind_str: Optional[str]) -> Optional[float]:
    """
    Parse NWS wind speed string like "10 mph" or "5 to 10 mph".
    Returns average value in mph.
    """
    if not wind_str:
        return None

    try:
        # Remove "mph" and split
        wind_str = wind_str.lower().replace("mph", "").strip()

        # Handle ranges like "5 to 10"
        if " to " in wind_str:
            parts = wind_str.split(" to ")
            low = float(parts[0].strip())
            high = float(parts[1].strip())
            return (low + high) / 2.0
        else:
            return float(wind_str)

    except (ValueError, AttributeError):
        return None


def extract_weather_code(short_forecast: str) -> Optional[int]:
    """
    Convert NWS shortForecast text to a simple weather code.
    Uses WMO-like codes similar to Open-Meteo.
    """
    if not short_forecast:
        return None

    forecast_lower = short_forecast.lower()

    # Thunderstorms
    if "thunderstorm" in forecast_lower or "tstm" in forecast_lower:
        return 95

    # Rain
    if "rain" in forecast_lower:
        if "shower" in forecast_lower:
            return 80
        return 61

    # Snow
    if "snow" in forecast_lower:
        return 71

    # Sleet/Freezing
    if "sleet" in forecast_lower or "freezing" in forecast_lower:
        return 67

    # Fog
    if "fog" in forecast_lower:
        return 45

    # Clouds
    if "overcast" in forecast_lower or "cloudy" in forecast_lower:
        return 3
    if "partly" in forecast_lower or "scattered" in forecast_lower:
        return 2
    if "mostly clear" in forecast_lower or "few" in forecast_lower:
        return 1
    if "clear" in forecast_lower or "sunny" in forecast_lower:
        return 0

    # Default to partly cloudy
    return 2


def get_nws_supplement_data(beaches: List[Dict], existing_records: List[Dict]) -> List[Dict]:
    """
    Supplement missing fields using NOAA NWS API.
    Only fills: temperature, weather, wind_speed_mph, wind_gust_mph

    Args:
        beaches: List of beach dicts with id, LATITUDE, LONGITUDE
        existing_records: List of forecast records with potential null fields

    Returns:
        Updated records with NWS data filled in
    """
    logger.info("   NWS supplement: fetching weather forecasts...")

    # Build mapping of all timestamps by beach (fill everything, not just missing)
    needed_by_beach = {}
    fields_to_fill = {"temperature", "weather", "wind_speed_mph", "wind_gust_mph"}

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_iso = rec.get("timestamp")
        if not bid or not ts_iso:
            continue

        if bid not in needed_by_beach:
            needed_by_beach[bid] = set()
        needed_by_beach[bid].add(ts_iso)

    if not needed_by_beach:
        logger.info("   NWS supplement: no records to process")
        return existing_records

    # Create beach metadata lookup
    beach_meta = {b["id"]: (b.get("Name"), b["LATITUDE"], b["LONGITUDE"]) for b in beaches}

    # Build index for quick updates
    updated_records = list(existing_records)
    key_to_index = {f"{r['beach_id']}_{r['timestamp']}": idx for idx, r in enumerate(updated_records)}

    # Process beaches that need data
    target_beaches = [b for b in beaches if b["id"] in needed_by_beach]
    logger.info(f"   NWS supplement: processing {len(target_beaches)} beaches...")

    # Cache grid points to avoid redundant lookups for nearby beaches
    gridpoint_cache = {}
    forecast_cache = {}

    filled_count = 0
    success_count = 0
    timeout_count = 0
    no_coverage_count = 0
    processed_count = 0
    total_beaches = len(target_beaches)

    for batch in chunk_iter(target_beaches, BATCH_SIZE):
        for beach in batch:
            processed_count += 1

            # Log progress every 100 beaches
            if processed_count % 100 == 0:
                logger.info(f"   NWS progress: {processed_count}/{total_beaches} beaches ({success_count} success, {no_coverage_count} no coverage, {timeout_count} timeout)")
            bid = beach["id"]
            lat = beach["LATITUDE"]
            lon = beach["LONGITUDE"]

            # Round coordinates to cache nearby beaches (NWS grid is ~2.5km, so 0.05° ~5.5km)
            cache_key = f"{round(lat, 2)},{round(lon, 2)}"

            # Check cache first
            if cache_key in forecast_cache:
                periods = forecast_cache[cache_key]
                logger.debug(f"   NWS: Using cached forecast for {beach.get('Name', bid)}")
            else:
                # Step 1: Get grid point (check cache first)
                if cache_key in gridpoint_cache:
                    grid_info = gridpoint_cache[cache_key]
                else:
                    grid_info = get_nws_gridpoint(lat, lon)
                    gridpoint_cache[cache_key] = grid_info

                if not grid_info or not grid_info.get("forecastHourly"):
                    logger.debug(f"   NWS: No forecast available for beach {beach.get('Name', bid)}")
                    no_coverage_count += 1
                    continue

                # Step 2: Get hourly forecast
                periods = get_nws_hourly_forecast(grid_info["forecastHourly"])
                if not periods:
                    logger.debug(f"   NWS: No periods returned for beach {beach.get('Name', bid)}")
                    timeout_count += 1
                    continue

                # Cache the forecast for nearby beaches
                forecast_cache[cache_key] = periods
                success_count += 1

            # Step 3: Match periods to our timestamps
            pacific = pytz.timezone("America/Los_Angeles")

            for period in periods:
                try:
                    # Parse NWS timestamp
                    period_time = datetime.fromisoformat(period["startTime"].replace("Z", "+00:00"))
                    period_local = period_time.astimezone(pacific)

                    # Align to 3-hour intervals (same as NOAA handler)
                    local_hour = period_local.hour
                    pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
                    closest_interval = min(pacific_intervals, key=lambda x: abs(x - local_hour))

                    clean_local_time = period_local.replace(
                        hour=closest_interval,
                        minute=0,
                        second=0,
                        microsecond=0
                    )

                    ts_iso = clean_local_time.isoformat()

                    # Check if we need this timestamp
                    if ts_iso not in needed_by_beach[bid]:
                        continue

                    key = f"{bid}_{ts_iso}"
                    idx = key_to_index.get(key)
                    if idx is None:
                        continue

                    rec = updated_records[idx]

                    # Fill all NWS fields (overwrite existing data)
                    if period.get("temperature") is not None:
                        rec["temperature"] = safe_float(period["temperature"])
                        filled_count += 1

                    weather_code = extract_weather_code(period.get("shortForecast", ""))
                    if weather_code is not None:
                        rec["weather"] = safe_int(weather_code)
                        filled_count += 1

                    if period.get("windSpeed"):
                        wind_speed = parse_wind_speed(period["windSpeed"])
                        if wind_speed is not None:
                            rec["wind_speed_mph"] = safe_float(wind_speed)
                            filled_count += 1

                    # Try to get actual wind gust from NWS
                    if period.get("windGust"):
                        wind_gust = parse_wind_speed(period["windGust"])
                        if wind_gust is not None:
                            rec["wind_gust_mph"] = safe_float(wind_gust)
                            filled_count += 1
                    else:
                        # If no NWS gust data, estimate from wind speed (1.4x typical gust factor)
                        current_wind_speed = rec.get("wind_speed_mph")
                        if current_wind_speed is not None and current_wind_speed > 0:
                            rec["wind_gust_mph"] = safe_float(current_wind_speed * 1.4)
                            filled_count += 1

                except Exception as e:
                    logger.debug(f"   NWS: Error processing period: {e}")
                    continue

    logger.info(f"   NWS supplement: filled {filled_count} field values")
    logger.info(f"   NWS stats: {success_count} beaches with data, {no_coverage_count} no coverage, {timeout_count} timeouts")
    logger.info(f"   NWS cache: {len(gridpoint_cache)} unique gridpoints, {len(forecast_cache)} unique forecasts")
    return updated_records


def test_nws_connection() -> bool:
    """Test NWS API connectivity."""
    try:
        logger.info("Testing NWS API connection...")
        # Test with San Francisco coordinates
        grid_info = get_nws_gridpoint(37.7749, -122.4194)
        if grid_info and grid_info.get("forecastHourly"):
            logger.info("NWS API connection successful")
            return True
        else:
            logger.error("NWS API connection failed - no forecast URL")
            return False
    except Exception as e:
        logger.error(f"NWS API connection test failed: {e}")
        return False
