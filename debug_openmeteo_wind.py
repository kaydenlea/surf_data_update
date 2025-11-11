#!/usr/bin/env python3
"""
Debug script to check Open Meteo wind data
"""
import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd

# Initialize Open-Meteo client
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Test location (Santa Monica)
test_lat = 34.0195
test_lon = -118.4912

print("Testing Open Meteo Wind Data...")
print(f"Location: {test_lat}, {test_lon}")
print("=" * 80)

# Make API request with CORRECT parameter names (with underscores)
weather_params = {
    "latitude": [test_lat],
    "longitude": [test_lon],
    "hourly": ["weather_code", "wind_speed_10m", "wind_gusts_10m"],
    "timezone": "America/Los_Angeles",
    "forecast_days": 3
}

print(f"Requesting parameters: {weather_params}")
print("")

url = "https://api.open-meteo.com/v1/forecast"
responses = openmeteo.weather_api(url, params=weather_params)

if responses and len(responses) > 0:
    response = responses[0]
    hourly = response.Hourly()

    # Get timestamps
    timestamps = pd.to_datetime(
        range(hourly.Time(), hourly.TimeEnd(), hourly.Interval()),
        unit="s", utc=True
    ).tz_convert("America/Los_Angeles")

    # Get variables
    weather_code = hourly.Variables(0).ValuesAsNumpy()
    wind_speed_kph = hourly.Variables(1).ValuesAsNumpy()
    wind_gust_kph = hourly.Variables(2).ValuesAsNumpy()

    print(f"Received {len(timestamps)} hourly records")
    print("\nFirst 10 records:")
    print(f"{'Timestamp':<25} {'Weather':<10} {'Wind Speed (km/h)':<20} {'Wind Gust (km/h)':<20} {'Gust >= Speed?':<15}")
    print("-" * 100)

    for i in range(min(10, len(timestamps))):
        ts = timestamps[i]
        wc = weather_code[i] if i < len(weather_code) else None
        ws = wind_speed_kph[i] if i < len(wind_speed_kph) else None
        wg = wind_gust_kph[i] if i < len(wind_gust_kph) else None

        # Convert to mph
        ws_mph = ws * 0.621371 if ws is not None else None
        wg_mph = wg * 0.621371 if wg is not None else None

        gust_ok = "OK" if (wg_mph is not None and ws_mph is not None and wg_mph >= ws_mph) else "BAD"

        ws_str = f"{ws_mph:.1f}" if ws_mph is not None else "None"
        wg_str = f"{wg_mph:.1f}" if wg_mph is not None else "None"

        print(f"{str(ts):<25} {str(wc):<10} {ws_str:<20} {wg_str:<20} {gust_ok:<15}")

    print("\n" + "=" * 80)
    print("SUMMARY:")

    # Calculate statistics
    issue_count = 0
    valid_count = 0

    for i in range(len(timestamps)):
        ws = wind_speed_kph[i] if i < len(wind_speed_kph) else None
        wg = wind_gust_kph[i] if i < len(wind_gust_kph) else None

        if ws is not None and wg is not None:
            if wg < ws:
                issue_count += 1
            valid_count += 1

    print(f"Total records with wind data: {valid_count}")
    print(f"Records where gust < speed: {issue_count}")
    print(f"Percentage with issue: {(issue_count/valid_count*100) if valid_count > 0 else 0:.1f}%")

    if issue_count > valid_count * 0.5:
        print("\n⚠️  WARNING: More than 50% of records have gust < speed")
        print("⚠️  This suggests the wind variables might be SWAPPED in the API request!")
else:
    print("Failed to get response from Open Meteo API")
