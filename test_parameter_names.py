#!/usr/bin/env python3
"""
Test to compare old (incorrect) vs new (correct) Open Meteo parameter names
"""
import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import numpy as np

# Initialize Open-Meteo client
cache_session = requests_cache.CachedSession(".cache_test", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Test location (Santa Monica)
test_lat = 34.0195
test_lon = -118.4912

url = "https://api.open-meteo.com/v1/forecast"

print("=" * 100)
print("COMPARING OLD vs NEW PARAMETER NAMES")
print("=" * 100)
print(f"Location: {test_lat}, {test_lon}\n")

# Test 1: OLD (incorrect) parameter names
print("Test 1: OLD parameter names (WITHOUT underscores)")
print("-" * 100)
old_params = {
    "latitude": [test_lat],
    "longitude": [test_lon],
    "hourly": ["weather_code", "windspeed_10m", "windgusts_10m"],  # OLD - no underscores
    "timezone": "America/Los_Angeles",
    "forecast_days": 1
}
print(f"Parameters: {old_params['hourly']}")

try:
    old_responses = openmeteo.weather_api(url, params=old_params)
    old_response = old_responses[0]
    old_hourly = old_response.Hourly()

    old_weather = old_hourly.Variables(0).ValuesAsNumpy()
    old_var1 = old_hourly.Variables(1).ValuesAsNumpy()
    old_var2 = old_hourly.Variables(2).ValuesAsNumpy()

    print(f"Variable 0 (weather_code) sample: {old_weather[0]:.1f}")
    print(f"Variable 1 (windspeed_10m?) sample: {old_var1[0]:.2f} km/h = {old_var1[0]*0.621371:.2f} mph")
    print(f"Variable 2 (windgusts_10m?) sample: {old_var2[0]:.2f} km/h = {old_var2[0]*0.621371:.2f} mph")

    # Check relationship
    bad_count_old = 0
    for i in range(min(24, len(old_var1))):
        if old_var2[i] < old_var1[i]:
            bad_count_old += 1

    print(f"Bad records (var2 < var1): {bad_count_old}/24 = {bad_count_old/24*100:.1f}%")

except Exception as e:
    print(f"ERROR with old parameters: {e}")

print("\n")

# Test 2: NEW (correct) parameter names
print("Test 2: NEW parameter names (WITH underscores)")
print("-" * 100)
new_params = {
    "latitude": [test_lat],
    "longitude": [test_lon],
    "hourly": ["weather_code", "wind_speed_10m", "wind_gusts_10m"],  # NEW - with underscores
    "timezone": "America/Los_Angeles",
    "forecast_days": 1
}
print(f"Parameters: {new_params['hourly']}")

try:
    new_responses = openmeteo.weather_api(url, params=new_params)
    new_response = new_responses[0]
    new_hourly = new_response.Hourly()

    new_weather = new_hourly.Variables(0).ValuesAsNumpy()
    new_var1 = new_hourly.Variables(1).ValuesAsNumpy()
    new_var2 = new_hourly.Variables(2).ValuesAsNumpy()

    print(f"Variable 0 (weather_code) sample: {new_weather[0]:.1f}")
    print(f"Variable 1 (wind_speed_10m) sample: {new_var1[0]:.2f} km/h = {new_var1[0]*0.621371:.2f} mph")
    print(f"Variable 2 (wind_gusts_10m) sample: {new_var2[0]:.2f} km/h = {new_var2[0]*0.621371:.2f} mph")

    # Check relationship
    bad_count_new = 0
    for i in range(min(24, len(new_var1))):
        if new_var2[i] < new_var1[i]:
            bad_count_new += 1

    print(f"Bad records (var2 < var1): {bad_count_new}/24 = {bad_count_new/24*100:.1f}%")

except Exception as e:
    print(f"ERROR with new parameters: {e}")

print("\n" + "=" * 100)
print("COMPARISON:")
print("=" * 100)
print("If the old parameters had significantly more 'bad' records, then the parameter names")
print("were causing the API to return different or incorrectly ordered data.")
print("=" * 100)
