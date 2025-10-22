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
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import logger, BATCH_SIZE
from utils import chunk_iter, safe_float, safe_int

# NWS API requires a User-Agent header
USER_AGENT = "SurfForecastApp/1.0 (surf@example.com)"  # Customize this
NWS_BASE_URL = "https://api.weather.gov"

# Max concurrent workers for parallel requests
MAX_WORKERS = 20

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


def create_session() -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic.
    This significantly speeds up repeated requests to the same host.
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=50,
        pool_maxsize=50
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})

    return session


def get_nws_gridpoint(lat: float, lon: float, timeout: int = 30, allow_fallback: bool = True, session: Optional[requests.Session] = None) -> Optional[Dict]:
    """
    Get NWS grid point information for a location.
    IMPROVED: For bays/harbors without coverage, tries nearby offshore points.

    Returns dict with:
        - gridId: str
        - gridX: int
        - gridY: int
        - forecastHourly: str (URL)
    """
    # Use provided session or create a temporary one
    use_session = session if session else requests.Session()
    if not session:
        use_session.headers.update({"User-Agent": USER_AGENT})

    # Try exact location first
    url = f"{NWS_BASE_URL}/points/{lat:.4f},{lon:.4f}"

    try:
        response = use_session.get(url, timeout=timeout)

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
            response = use_session.get(url, timeout=timeout)

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


def get_nws_hourly_forecast(forecast_url: str, timeout: int = 30, session: Optional[requests.Session] = None) -> List[Dict]:
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
    # Use provided session or create a temporary one
    use_session = session if session else requests.Session()
    if not session:
        use_session.headers.update({"User-Agent": USER_AGENT})

    try:
        response = use_session.get(forecast_url, timeout=timeout)
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


def fetch_beach_forecast(beach: Dict, session: requests.Session, gridpoint_cache: Dict, forecast_cache: Dict) -> Optional[List[Dict]]:
    """
    Fetch forecast for a single beach using shared caches.
    Returns list of periods or None if fetch failed.
    """
    bid = beach["id"]
    lat = beach["LATITUDE"]
    lon = beach["LONGITUDE"]

    # Round coordinates to cache nearby beaches (NWS grid is ~2.5km, so 0.05° ~5.5km)
    cache_key = f"{round(lat, 2)},{round(lon, 2)}"

    # Check forecast cache first
    if cache_key in forecast_cache:
        logger.debug(f"   NWS: Using cached forecast for {beach.get('Name', bid)}")
        return forecast_cache[cache_key]

    # Step 1: Get grid point (check cache first)
    if cache_key in gridpoint_cache:
        grid_info = gridpoint_cache[cache_key]
    else:
        grid_info = get_nws_gridpoint(lat, lon, session=session)
        gridpoint_cache[cache_key] = grid_info

    if not grid_info or not grid_info.get("forecastHourly"):
        logger.debug(f"   NWS: No forecast available for beach {beach.get('Name', bid)}")
        return None

    # Step 2: Get hourly forecast
    periods = get_nws_hourly_forecast(grid_info["forecastHourly"], session=session)
    if not periods:
        logger.debug(f"   NWS: No periods returned for beach {beach.get('Name', bid)}")
        return None

    # Cache the forecast for nearby beaches
    forecast_cache[cache_key] = periods
    return periods


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
    start_time = time.time()
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
    # Note: Dicts are not thread-safe for writes, but we'll use a lock
    from threading import Lock
    gridpoint_cache = {}
    forecast_cache = {}
    cache_lock = Lock()

    filled_count = 0
    success_count = 0
    timeout_count = 0
    no_coverage_count = 0
    total_beaches = len(target_beaches)

    # Create a shared session with connection pooling
    session = create_session()

    # Store results: beach_id -> periods
    beach_forecasts = {}

    # Process beaches in parallel using ThreadPoolExecutor
    logger.info(f"   NWS: Fetching forecasts for {total_beaches} beaches using {MAX_WORKERS} workers...")

    def process_beach(beach):
        """Thread worker function to fetch forecast for a single beach."""
        try:
            # Thread-safe cache access
            with cache_lock:
                periods = fetch_beach_forecast(beach, session, gridpoint_cache, forecast_cache)

            if periods:
                return (beach["id"], periods, "success")
            else:
                return (beach["id"], None, "no_coverage")
        except Exception as e:
            logger.debug(f"   NWS: Error processing beach {beach.get('Name', beach['id'])}: {e}")
            return (beach["id"], None, "error")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_beach, beach): beach for beach in target_beaches}

        completed = 0
        for future in as_completed(futures):
            completed += 1

            # Log progress every 100 beaches
            if completed % 100 == 0 or completed == total_beaches:
                logger.info(f"   NWS progress: {completed}/{total_beaches} beaches")

            try:
                beach_id, periods, status = future.result()

                if status == "success":
                    beach_forecasts[beach_id] = periods
                    success_count += 1
                elif status == "no_coverage":
                    no_coverage_count += 1
                else:
                    timeout_count += 1

            except Exception as e:
                logger.debug(f"   NWS: Future exception: {e}")
                timeout_count += 1

    logger.info(f"   NWS: Completed fetching. Processing {len(beach_forecasts)} beach forecasts...")

    # Now process all fetched forecasts and update records
    pacific = pytz.timezone("America/Los_Angeles")

    for beach_id, periods in beach_forecasts.items():
        if beach_id not in needed_by_beach:
            continue

        # Step 3: Match periods to our timestamps
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
                if ts_iso not in needed_by_beach[beach_id]:
                    continue

                key = f"{beach_id}_{ts_iso}"
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

    elapsed_time = time.time() - start_time
    logger.info(f"   NWS supplement: filled {filled_count} field values")
    logger.info(f"   NWS stats: {success_count} beaches with data, {no_coverage_count} no coverage, {timeout_count} timeouts")
    logger.info(f"   NWS cache: {len(gridpoint_cache)} unique gridpoints, {len(forecast_cache)} unique forecasts")
    logger.info(f"   NWS supplement: completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    return updated_records


def test_nws_connection() -> bool:
    """Test NWS API connectivity."""
    try:
        logger.info("Testing NWS API connection...")
        session = create_session()
        # Test with San Francisco coordinates
        grid_info = get_nws_gridpoint(37.7749, -122.4194, session=session)
        if grid_info and grid_info.get("forecastHourly"):
            logger.info("NWS API connection successful")
            return True
        else:
            logger.error("NWS API connection failed - no forecast URL")
            return False
    except Exception as e:
        logger.error(f"NWS API connection test failed: {e}")
        return False
