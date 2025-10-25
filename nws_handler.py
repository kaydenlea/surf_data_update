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

# NWS API requires a User-Agent header per API documentation
# Format: ApplicationName/vX.Y (contact-email)
# See: https://weather-gov.github.io/api/general-faqs
USER_AGENT = "SurfForecastApp/1.0 ([email protected])"
NWS_BASE_URL = "https://api.weather.gov"

# Max concurrent workers for parallel requests
MAX_WORKERS = 50

# Request timeout in seconds
# NWS API can be slow, especially for certain gridpoints or during peak usage
# 30s provides good balance between responsiveness and allowing slow responses
NWS_TIMEOUT = 30

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


def normalize_to_utc_iso(value) -> Optional[str]:
    """
    Convert a datetime or ISO-formatted string to a normalized UTC ISO string.
    Returns None if the value cannot be parsed.
    """
    if value is None:
        return None

    if isinstance(value, str):
        ts_str = value.strip()
        if not ts_str:
            return None
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts_str)
        except ValueError:
            return None
    elif isinstance(value, datetime):
        dt = value
    else:
        return None

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)

    return dt.isoformat()


def create_session() -> requests.Session:
    """
    Create a requests session with connection pooling and retry logic.
    This significantly speeds up repeated requests to the same host.

    Retry configuration:
    - 3 total retries with exponential backoff (0.5s, 1s, 2s)
    - Retries on connection errors, timeouts, and 5xx server errors
    - Does not retry on 4xx client errors (except 429 rate limit)
    """
    session = requests.Session()

    # Configure retry strategy with read timeout handling
    retry_strategy = Retry(
        total=3,
        backoff_factor=1.0,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False  # Don't raise exception on max retries, return response
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


def get_nws_gridpoint(lat: float, lon: float, timeout: int = NWS_TIMEOUT, allow_fallback: bool = False, session: Optional[requests.Session] = None) -> Optional[Dict]:
    """
    Get NWS grid point information for a location.

    Returns dict with:
        - gridId: str
        - gridX: int
        - gridY: int
        - forecastHourly: str (URL)

    Returns None if location not covered by NWS.
    """
    # Use provided session or create a temporary one
    use_session = session if session else requests.Session()
    if not session:
        use_session.headers.update({"User-Agent": USER_AGENT})

    # Get gridpoint for this location
    url = f"{NWS_BASE_URL}/points/{lat:.4f},{lon:.4f}"

    try:
        response = use_session.get(url, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        props = data.get("properties", {})
        return {
            "gridId": props.get("gridId"),
            "gridX": props.get("gridX"),
            "gridY": props.get("gridY"),
            "forecastHourly": props.get("forecastHourly"),
            "forecastGridData": props.get("forecastGridData"),
        }

    except requests.exceptions.Timeout:
        logger.warning(f"   NWS: Timeout getting gridpoint for {lat},{lon} (exceeded {timeout}s)")
        return None
    except requests.exceptions.RequestException as e:
        logger.debug(f"   NWS: Request failed for {lat},{lon}: {e}")
        return None


def get_nws_hourly_forecast(forecast_url: str, timeout: int = NWS_TIMEOUT, session: Optional[requests.Session] = None) -> List[Dict]:
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
        logger.warning(f"   NWS: Timeout fetching hourly forecast from {forecast_url.split('/')[-3:]} (exceeded {timeout}s)")
        return []
    except requests.exceptions.RequestException as e:
        logger.debug(f"   NWS: Failed to fetch hourly forecast: {e}")
        return []


def get_nws_pressure_data(grid_data_url: str, timeout: int = NWS_TIMEOUT, session: Optional[requests.Session] = None) -> Dict:
    """
    Fetch pressure data from NWS grid data endpoint.

    NOTE: Pressure data is often unavailable (0 values) in the NWS gridData API.
    This is a known limitation of the NWS API - not all forecast offices provide
    pressure data in their gridpoint forecasts.

    Returns dict mapping ISO timestamps to pressure values in inHg.
    Returns empty dict if no pressure data is available.
    """
    # Use provided session or create a temporary one
    use_session = session if session else requests.Session()
    if not session:
        use_session.headers.update({"User-Agent": USER_AGENT})

    try:
        response = use_session.get(grid_data_url, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        props = data.get("properties", {})
        pressure_data = props.get("pressure", {})

        # Check if pressure data is available
        values = pressure_data.get("values", [])
        if not values:
            logger.debug(f"   NWS: No pressure data available in gridData endpoint")
            return {}

        # Extract pressure values - NWS gridData uses Pascals (Pa) as per WMO standards
        # Unit is indicated in 'uom' field (e.g., "wmoUnit:Pa")
        uom = pressure_data.get("uom", "")

        # Convert to dict: timestamp -> pressure in inHg
        pressure_map = {}
        for item in values:
            timestamp = item.get("validTime", "").split("/")[0]  # Get start time
            pressure_value = item.get("value")

            if timestamp and pressure_value is not None:
                # NWS typically returns pressure in Pascals
                # Convert Pascals to inHg (1 Pa = 0.0002953 inHg)
                pressure_inhg = pressure_value * 0.0002953
                pressure_map[timestamp] = pressure_inhg

        logger.debug(f"   NWS: Retrieved {len(pressure_map)} pressure values (unit: {uom})")
        return pressure_map

    except requests.exceptions.Timeout:
        logger.debug(f"   NWS: Timeout fetching grid pressure data")
        return {}
    except requests.exceptions.RequestException as e:
        logger.debug(f"   NWS: Failed to fetch grid pressure data: {e}")
        return {}


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


def fetch_beach_forecast(beach: Dict, session: requests.Session, gridpoint_cache: Dict, forecast_cache: Dict, pressure_cache: Dict) -> Optional[tuple]:
    """
    Fetch forecast for a single beach using shared caches.

    Caching strategy:
    1. First level: Round coordinates to 0.02° (~2.2km) to group nearby beaches
    2. Second level: Use actual NWS grid coordinates (gridId/X/Y) for forecast sharing

    This means beaches that map to the same NWS grid cell share forecast data,
    significantly reducing API calls.

    Returns tuple of (periods, pressure_map, grid_key) or None if fetch failed.
    """
    bid = beach["id"]
    lat = beach["LATITUDE"]
    lon = beach["LONGITUDE"]

    # First-level cache: Round coordinates to reduce gridpoint API calls
    # NWS grid is ~2.5km, so 0.02° (~2.2km) groups nearby beaches
    coord_cache_key = f"{round(lat, 2)},{round(lon, 2)}"

    # Step 1: Get grid point (check cache first)
    if coord_cache_key in gridpoint_cache:
        grid_info = gridpoint_cache[coord_cache_key]
    else:
        grid_info = get_nws_gridpoint(lat, lon, session=session)
        gridpoint_cache[coord_cache_key] = grid_info

    if not grid_info or not grid_info.get("forecastHourly"):
        logger.debug(f"   NWS: No forecast available for beach {beach.get('Name', bid)}")
        return None

    # Second-level cache: Use actual NWS grid coordinates
    # All beaches in the same grid cell get identical forecast data
    grid_key = f"{grid_info['gridId']}/{grid_info['gridX']},{grid_info['gridY']}"

    # Check if we already have forecast for this grid cell
    if grid_key in forecast_cache:
        logger.debug(f"   NWS: Using cached grid forecast for {beach.get('Name', bid)} (grid: {grid_key})")
        return forecast_cache[grid_key]

    # Step 2: Get hourly forecast for this grid cell
    periods = get_nws_hourly_forecast(grid_info["forecastHourly"], session=session)
    if not periods:
        logger.debug(f"   NWS: No periods returned for beach {beach.get('Name', bid)}")
        return None

    # Step 3: Get pressure data from grid data endpoint
    pressure_map = {}
    if grid_info.get("forecastGridData"):
        if grid_key in pressure_cache:
            pressure_map = pressure_cache[grid_key]
        else:
            pressure_map = get_nws_pressure_data(grid_info["forecastGridData"], session=session)
            pressure_cache[grid_key] = pressure_map

    # Cache the forecast by grid cell - all beaches in this cell will share it
    result = (periods, pressure_map, grid_key)
    forecast_cache[grid_key] = result
    return result


def get_nws_supplement_data(beaches: List[Dict], existing_records: List[Dict]) -> List[Dict]:
    """
    Supplement missing fields using NOAA NWS API.
    Fills: temperature, weather, wind_speed_mph, wind_gust_mph, pressure_inhg

    NOTE: Pressure data is often unavailable in the NWS API. This is a known
    limitation - not all NWS forecast offices provide pressure in gridpoint forecasts.
    When pressure is unavailable, the field will remain null.

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
    fields_to_fill = {"temperature", "weather", "wind_speed_mph", "wind_gust_mph", "pressure_inhg"}

    for rec in existing_records:
        bid = rec.get("beach_id")
        ts_iso = normalize_to_utc_iso(rec.get("timestamp"))
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
    key_to_index: Dict[str, int] = {}
    for idx, record in enumerate(updated_records):
        bid = record.get("beach_id")
        ts_iso = normalize_to_utc_iso(record.get("timestamp"))
        if not bid or not ts_iso:
            continue
        key_to_index[f"{bid}_{ts_iso}"] = idx

    # Process beaches that need data
    target_beaches = [b for b in beaches if b["id"] in needed_by_beach]
    logger.info(f"   NWS supplement: processing {len(target_beaches)} beaches...")

    # PRE-GROUP beaches by approximate location to minimize API calls
    # Similar to NOAA handler optimization - group beaches within ~6-7 miles
    logger.info("   NWS: Pre-grouping beaches by location to minimize API calls...")
    location_groups = {}
    beach_to_group = {}  # Track which group each beach belongs to

    for beach in target_beaches:
        # Round coordinates to 0.1° (~6-7 miles) to create location groups
        # This is more aggressive than the 0.02° rounding we use for caching
        rounded_lat = round(beach["LATITUDE"] / 0.1) * 0.1
        rounded_lon = round(beach["LONGITUDE"] / 0.1) * 0.1
        group_key = f"{rounded_lat:.1f},{rounded_lon:.1f}"

        if group_key not in location_groups:
            location_groups[group_key] = []
        location_groups[group_key].append(beach)
        beach_to_group[beach["id"]] = group_key

    logger.info(f"   NWS: Grouped {len(target_beaches)} beaches into {len(location_groups)} location groups")

    # For each group, select a representative beach (westernmost for coastal accuracy)
    group_representatives = {}
    for group_key, group_beaches in location_groups.items():
        # Pick westernmost beach (most negative longitude) as representative
        representative = min(group_beaches, key=lambda b: b["LONGITUDE"])
        group_representatives[group_key] = representative
        logger.debug(f"   NWS: Group {group_key} has {len(group_beaches)} beaches, using {representative.get('Name')} as representative")

    # Cache grid points to avoid redundant lookups for nearby beaches
    # Note: Dicts are not thread-safe for writes, but we'll use a lock
    from threading import Lock
    gridpoint_cache = {}
    forecast_cache = {}
    pressure_cache = {}
    cache_lock = Lock()

    filled_count = 0
    success_count = 0
    timeout_count = 0
    no_coverage_count = 0
    total_beaches = len(target_beaches)

    # Create a shared session with connection pooling
    session = create_session()

    # Store results: group_key -> (periods, pressure_map, grid_key)
    group_forecasts = {}

    # OPTIMIZATION: Only fetch forecasts for representative beaches (one per group)
    # Then share the forecast data with all beaches in the same group
    representatives_list = list(group_representatives.values())
    total_api_calls = len(representatives_list)
    logger.info(f"   NWS: Fetching forecasts for {total_api_calls} location groups (representing {total_beaches} beaches) using {MAX_WORKERS} workers...")
    logger.info(f"   NWS: Expected API call reduction: {((total_beaches - total_api_calls) / total_beaches * 100):.1f}%")

    def process_representative(beach):
        """Thread worker function to fetch forecast for a representative beach."""
        try:
            # Thread-safe cache access
            with cache_lock:
                result = fetch_beach_forecast(beach, session, gridpoint_cache, forecast_cache, pressure_cache)

            if result:
                return (beach["id"], result, "success")
            else:
                return (beach["id"], None, "no_coverage")
        except Exception as e:
            logger.debug(f"   NWS: Error processing representative beach {beach.get('Name', beach['id'])}: {e}")
            return (beach["id"], None, "error")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Only process representative beaches
        futures = {executor.submit(process_representative, beach): beach for beach in representatives_list}

        completed = 0
        for future in as_completed(futures):
            completed += 1

            # Log progress every 10 groups for better visibility
            if completed % 10 == 0 or completed == total_api_calls:
                logger.info(f"   NWS progress: {completed}/{total_api_calls} location groups")

            try:
                rep_beach_id, result, status = future.result()

                if status == "success":
                    # Find which group this representative belongs to
                    group_key = beach_to_group[rep_beach_id]
                    group_forecasts[group_key] = result
                    success_count += 1
                elif status == "no_coverage":
                    no_coverage_count += 1
                else:
                    timeout_count += 1

            except Exception as e:
                logger.debug(f"   NWS: Future exception: {e}")
                timeout_count += 1

    logger.info(f"   NWS: Completed fetching {len(group_forecasts)} location groups")

    # SHARE forecasts from representatives to all beaches in their groups
    logger.info(f"   NWS: Sharing forecast data across {total_beaches} beaches...")
    beach_forecasts = {}

    for beach in target_beaches:
        group_key = beach_to_group[beach["id"]]
        if group_key in group_forecasts:
            # Share the forecast from this group with all beaches in it
            beach_forecasts[beach["id"]] = group_forecasts[group_key]

    logger.info(f"   NWS: Successfully mapped forecasts to {len(beach_forecasts)} beaches")

    # Now process all fetched forecasts and update records
    pacific = pytz.timezone("America/Los_Angeles")

    for beach_id, (periods, pressure_map, grid_key) in beach_forecasts.items():
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

                ts_iso = normalize_to_utc_iso(clean_local_time)
                if ts_iso is None:
                    continue

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

                # Fill pressure data from grid data (if available)
                # NOTE: Pressure data is frequently unavailable in NWS gridData
                if pressure_map:
                    # Try to match the timestamp in pressure_map
                    # Pressure timestamps are in UTC, so convert our local time to UTC
                    period_utc = period_time.astimezone(pytz.UTC)

                    # Try exact match first
                    pressure_timestamp = period_utc.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                    pressure_value = pressure_map.get(pressure_timestamp)

                    # If no exact match, try to find closest timestamp within same hour
                    if pressure_value is None:
                        hour_start = period_utc.replace(minute=0, second=0, microsecond=0)
                        for ts, val in pressure_map.items():
                            try:
                                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                                if ts_dt.replace(minute=0, second=0, microsecond=0) == hour_start:
                                    pressure_value = val
                                    break
                            except:
                                continue

                    if pressure_value is not None:
                        rec["pressure_inhg"] = safe_float(pressure_value)
                        filled_count += 1

            except Exception as e:
                logger.debug(f"   NWS: Error processing period: {e}")
                continue

    elapsed_time = time.time() - start_time

    # Calculate grouping efficiency at multiple levels
    location_groups_count = len(location_groups)
    unique_coord_groups = len(gridpoint_cache)
    unique_grids = len(forecast_cache)
    total_beaches_with_data = len(beach_forecasts)

    # Calculate efficiency metrics
    if total_beaches > 0 and location_groups_count > 0:
        beaches_per_location_group = total_beaches / location_groups_count
        location_api_savings = ((total_beaches - location_groups_count) / total_beaches * 100)
    else:
        beaches_per_location_group = 0
        location_api_savings = 0

    if location_groups_count > 0 and unique_grids > 0:
        groups_per_grid = location_groups_count / unique_grids
        grid_consolidation = ((location_groups_count - unique_grids) / location_groups_count * 100) if location_groups_count > 0 else 0
    else:
        groups_per_grid = 0
        grid_consolidation = 0

    total_api_savings = ((total_beaches - unique_grids) / total_beaches * 100) if total_beaches > 0 else 0

    logger.info(f"   NWS supplement: filled {filled_count} field values")
    logger.info(f"   NWS stats: {total_beaches_with_data} beaches with data, {no_coverage_count} no coverage, {timeout_count} timeouts")
    logger.info(f"   NWS location grouping: {total_beaches} beaches → {location_groups_count} location groups ({beaches_per_location_group:.1f} beaches/group)")
    logger.info(f"   NWS grid consolidation: {location_groups_count} location groups → {unique_grids} unique NWS grids ({groups_per_grid:.1f} groups/grid)")
    logger.info(f"   NWS total efficiency: {total_beaches} beaches served by {unique_grids} API calls = {total_api_savings:.1f}% reduction")
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
