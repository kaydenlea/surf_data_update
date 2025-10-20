#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for all NOAA/Government data sources
Verifies connectivity and data format before production use
"""

import sys
import os
from datetime import datetime, timedelta
import pytz

# Fix Windows encoding
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"

from config import logger

print("=" * 80)
print("TESTING NOAA/GOVERNMENT DATA SOURCES")
print("=" * 80)
print()

# Track overall success
all_tests_passed = True

# ============================================================================
# TEST 1: NOAA NWS API
# ============================================================================

print("TEST 1: NOAA National Weather Service (NWS) API")
print("-" * 80)

try:
    from nws_handler import get_nws_gridpoint, get_nws_hourly_forecast, test_nws_connection

    # Test connection
    if not test_nws_connection():
        print("[FAIL] NWS connection test FAILED")
        all_tests_passed = False
    else:
        print("[OK] NWS connection successful")

        # Test actual data retrieval with San Francisco
        print("\nTesting NWS data retrieval for San Francisco...")
        lat, lon = 37.7749, -122.4194

        grid_info = get_nws_gridpoint(lat, lon)
        if grid_info:
            print(f"[OK] Grid point retrieved: {grid_info['gridId']}, X={grid_info['gridX']}, Y={grid_info['gridY']}")

            # Get forecast
            periods = get_nws_hourly_forecast(grid_info['forecastHourly'])
            if periods and len(periods) > 0:
                print(f"[OK] Forecast periods retrieved: {len(periods)} hours")

                # Show sample data
                sample = periods[0]
                print(f"\nSample forecast period:")
                print(f"  Time: {sample.get('startTime')}")
                print(f"  Temperature: {sample.get('temperature')}°F")
                print(f"  Wind: {sample.get('windSpeed')}")
                print(f"  Conditions: {sample.get('shortForecast')}")
                print(f"  Wind Gust: {sample.get('windGust', 'N/A')}")
                print("[OK] NWS data format looks correct")
            else:
                print("[FAIL] No forecast periods returned")
                all_tests_passed = False
        else:
            print("[FAIL] Failed to get grid point")
            all_tests_passed = False

except Exception as e:
    print(f"[FAIL] NWS test FAILED with error: {e}")
    all_tests_passed = False

print()

# ============================================================================
# TEST 2: NOAA CO-OPS Tides API
# ============================================================================

print("TEST 2: NOAA CO-OPS Tides & Currents API")
print("-" * 80)

try:
    from noaa_tides_handler import (
        get_tide_predictions, get_water_temperature,
        find_nearest_tide_station, test_noaa_tides_connection
    )

    # Test connection
    if not test_noaa_tides_connection():
        print("[FAIL] NOAA Tides connection test FAILED")
        all_tests_passed = False
    else:
        print("[OK] NOAA Tides connection successful")

        # Test station finding
        print("\nTesting tide station matching for San Francisco...")
        lat, lon = 37.7749, -122.4194
        station_info = find_nearest_tide_station(lat, lon)

        if station_info:
            station_id, distance = station_info
            print(f"[OK] Nearest station: {station_id} ({distance:.1f} km away)")

            # Get tide predictions
            today = datetime.now().strftime("%Y%m%d")
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")

            tide_data = get_tide_predictions(station_id, today, tomorrow)
            if tide_data and len(tide_data) > 0:
                print(f"[OK] Tide predictions retrieved: {len(tide_data)} data points")

                # Show sample
                sample = tide_data[0]
                print(f"\nSample tide prediction:")
                print(f"  Time: {sample.get('t')}")
                print(f"  Level: {sample.get('v')} feet")
                print("[OK] Tide data format looks correct")
            else:
                print("[FAIL] No tide predictions returned")
                all_tests_passed = False

            # Test water temperature (may not be available at all stations)
            print("\nTesting water temperature (optional)...")
            water_temp = get_water_temperature(station_id, today, tomorrow)
            if water_temp and len(water_temp) > 0:
                sample = water_temp[0]
                print(f"[OK] Water temperature available: {sample.get('v')}°C at {sample.get('t')}")
            else:
                print("[WARN] Water temperature not available at this station (this is normal)")
        else:
            print("[FAIL] Failed to find nearest tide station")
            all_tests_passed = False

except Exception as e:
    print(f"[FAIL] NOAA Tides test FAILED with error: {e}")
    all_tests_passed = False

print()

# ============================================================================
# TEST 3: USNO Astronomical API
# ============================================================================

print("TEST 3: USNO Astronomical Applications API")
print("-" * 80)

try:
    from usno_handler import get_sun_moon_data, test_usno_connection

    # Test connection
    if not test_usno_connection():
        print("[FAIL] USNO connection test FAILED")
        all_tests_passed = False
    else:
        print("[OK] USNO connection successful")

        # Test actual data retrieval
        print("\nTesting USNO data retrieval for San Francisco...")
        lat, lon = 37.7749, -122.4194
        today = datetime.now().strftime("%Y-%m-%d")

        data = get_sun_moon_data(lat, lon, today)
        if data:
            print(f"[OK] Astronomical data retrieved for {today}")
            print(f"\nSample data:")
            print(f"  Sunrise: {data.get('sunrise')}")
            print(f"  Sunset: {data.get('sunset')}")
            print(f"  Moon phase: {data.get('moon_phase_name')}")

            if data.get('moonrise'):
                print(f"  Moonrise: {data.get('moonrise')}")
            if data.get('moonset'):
                print(f"  Moonset: {data.get('moonset')}")

            print("[OK] USNO data format looks correct")
        else:
            print("[FAIL] Failed to get USNO data")
            all_tests_passed = False

except Exception as e:
    print(f"[FAIL] USNO test FAILED with error: {e}")
    all_tests_passed = False

print()

# ============================================================================
# TEST 4: Integration Test - Multiple Beaches
# ============================================================================

print("TEST 4: Integration Test - California Beaches Coverage")
print("-" * 80)

try:
    # Test a few California beach locations
    test_beaches = [
        {"name": "San Diego", "lat": 32.7157, "lon": -117.1611},
        {"name": "Santa Monica", "lat": 34.0195, "lon": -118.4912},
        {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194},
        {"name": "Santa Cruz", "lat": 36.9741, "lon": -122.0308},
    ]

    print("\nTesting coverage for sample California beaches:")
    print()

    coverage_results = {
        "nws": 0,
        "tides": 0,
        "usno": 0,
    }

    for beach in test_beaches:
        print(f"{beach['name']}:")

        # Test NWS
        try:
            from nws_handler import get_nws_gridpoint
            grid = get_nws_gridpoint(beach['lat'], beach['lon'])
            if grid:
                print(f"  [OK] NWS coverage available")
                coverage_results["nws"] += 1
            else:
                print(f"  [FAIL] NWS coverage NOT available")
        except Exception as e:
            print(f"  [FAIL] NWS error: {e}")

        # Test Tides
        try:
            from noaa_tides_handler import find_nearest_tide_station
            station = find_nearest_tide_station(beach['lat'], beach['lon'])
            if station:
                station_id, dist = station
                print(f"  [OK] Tide station {station_id} ({dist:.1f} km)")
                coverage_results["tides"] += 1
            else:
                print(f"  [FAIL] No tide station within range")
        except Exception as e:
            print(f"  [FAIL] Tide error: {e}")

        # Test USNO
        try:
            from usno_handler import get_sun_moon_data
            today = datetime.now().strftime("%Y-%m-%d")
            usno_data = get_sun_moon_data(beach['lat'], beach['lon'], today)
            if usno_data and usno_data.get('sunrise'):
                print(f"  [OK] USNO data available (sunrise: {usno_data['sunrise']})")
                coverage_results["usno"] += 1
            else:
                print(f"  [FAIL] USNO data NOT available")
        except Exception as e:
            print(f"  [FAIL] USNO error: {e}")

        print()

    # Summary
    total_beaches = len(test_beaches)
    print("Coverage Summary:")
    print(f"  NWS: {coverage_results['nws']}/{total_beaches} beaches")
    print(f"  Tides: {coverage_results['tides']}/{total_beaches} beaches")
    print(f"  USNO: {coverage_results['usno']}/{total_beaches} beaches")

    if all(v == total_beaches for v in coverage_results.values()):
        print("\n[OK] Full coverage for all test beaches!")
    else:
        print("\n[WARN] Partial coverage - some beaches may need fallback to neighbor fill")

except Exception as e:
    print(f"[FAIL] Integration test FAILED with error: {e}")
    all_tests_passed = False

print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("=" * 80)
if all_tests_passed:
    print("[OK][OK][OK] ALL TESTS PASSED [OK][OK][OK]")
    print("=" * 80)
    print("\nThe NOAA data stack is ready for production use!")
    print("\nNext steps:")
    print("  1. Run: python main_noaa.py")
    print("  2. Compare results with your current setup")
    print("  3. Check null counts in database")
    sys.exit(0)
else:
    print("[FAIL][FAIL][FAIL] SOME TESTS FAILED [FAIL][FAIL][FAIL]")
    print("=" * 80)
    print("\nPlease review the errors above before using the NOAA stack in production.")
    sys.exit(1)

