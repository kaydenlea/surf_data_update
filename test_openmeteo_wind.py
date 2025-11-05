#!/usr/bin/env python3
"""
Quick test script to fetch Open Meteo wind data for Newport Beach tomorrow.
"""

import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import pytz

# Initialize Open-Meteo client with caching and retry
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Newport Beach coordinates
NEWPORT_BEACH_LAT = 33.6189
NEWPORT_BEACH_LON = -117.9298

# Get tomorrow's date
pacific = pytz.timezone("America/Los_Angeles")
now = datetime.now(pacific)
tomorrow = now + timedelta(days=1)
tomorrow_date = tomorrow.strftime("%Y-%m-%d")

print("=" * 80)
print("OPEN METEO WIND DATA TEST - NEWPORT BEACH")
print("=" * 80)
print(f"Location: Newport Beach, CA ({NEWPORT_BEACH_LAT}, {NEWPORT_BEACH_LON})")
print(f"Date: {tomorrow_date} (tomorrow)")
print("=" * 80)

# Make API request
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": NEWPORT_BEACH_LAT,
    "longitude": NEWPORT_BEACH_LON,
    "hourly": ["windspeed_10m", "windgusts_10m", "winddirection_10m"],
    "timezone": "America/Los_Angeles",
    "start_date": tomorrow_date,
    "end_date": tomorrow_date
}

print("\nFetching data from Open Meteo API...")
responses = openmeteo.weather_api(url, params=params)
response = responses[0]

print(f"[OK] Successfully retrieved data")
print(f"Coordinates: {response.Latitude()} N {response.Longitude()} W")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone: {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()} s")

# Process hourly data
hourly = response.Hourly()
hourly_time = range(hourly.Time(), hourly.TimeEnd(), hourly.Interval())
hourly_windspeed_10m = hourly.Variables(0).ValuesAsNumpy()
hourly_windgusts_10m = hourly.Variables(1).ValuesAsNumpy()
hourly_winddirection_10m = hourly.Variables(2).ValuesAsNumpy()

print("\n" + "=" * 80)
print("HOURLY WIND FORECAST FOR TOMORROW")
print("=" * 80)
print(f"{'Time':<20} {'Wind Speed':<15} {'Wind Gust':<15} {'Direction':<15}")
print(f"{'(Pacific)':<20} {'(mph)':<15} {'(mph)':<15} {'(degrees)':<15}")
print("-" * 80)

for i, timestamp in enumerate(hourly_time):
    dt = datetime.fromtimestamp(timestamp, tz=pacific)

    # Convert km/h to mph
    wind_speed_kph = hourly_windspeed_10m[i]
    wind_gust_kph = hourly_windgusts_10m[i]
    direction = hourly_winddirection_10m[i]

    wind_speed_mph = wind_speed_kph * 0.621371
    wind_gust_mph = wind_gust_kph * 0.621371

    time_str = dt.strftime("%I:%M %p")

    print(f"{time_str:<20} {wind_speed_mph:>6.1f} mph    {wind_gust_mph:>6.1f} mph    {direction:>6.0f} deg")

print("=" * 80)
print("\nWind Level Guidance:")
print("  0-15 mph: Good conditions (green)")
print("  15-25 mph: Moderate wind (yellow/orange)")
print("  25+ mph: Strong wind (red/orange)")
print("=" * 80)
