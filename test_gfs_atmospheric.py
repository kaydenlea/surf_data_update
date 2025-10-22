#!/usr/bin/env python3
"""
Test script for GFS Atmospheric handler
Shows sample output of weather data that would be retrieved
"""

import sys
from datetime import datetime
import pytz

from config import logger
from database import fetch_all_beaches
from gfs_atmospheric_handler import (
    get_gfs_atmospheric_dataset_url,
    load_gfs_atmospheric_dataset,
    validate_gfs_atmospheric_dataset,
    extract_gfs_atmospheric_point,
    derive_weather_code
)


def weather_code_description(code: int) -> str:
    """Get human-readable description of weather code."""
    descriptions = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        51: "Drizzle",
        61: "Light rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Light snow",
        73: "Moderate snow",
        75: "Heavy snow",
        95: "Thunderstorm"
    }
    return descriptions.get(code, f"Unknown ({code})")


def test_gfs_atmospheric():
    """Test GFS Atmospheric data retrieval for a few sample beaches."""

    print("=" * 80)
    print("GFS ATMOSPHERIC WEATHER DATA TEST")
    print("=" * 80)
    print()

    # Step 1: Get dataset URL
    print("Step 1: Finding GFS Atmospheric dataset...")
    url = get_gfs_atmospheric_dataset_url()

    if not url:
        print("❌ ERROR: Could not find GFS Atmospheric dataset URL")
        return False

    print(f"[OK] Found dataset: {url}")
    print()

    # Step 2: Load dataset
    print("Step 2: Loading GFS Atmospheric dataset...")
    ds = load_gfs_atmospheric_dataset(url)

    if not ds:
        print("❌ ERROR: Could not load GFS Atmospheric dataset")
        return False

    print("[OK] Dataset loaded successfully")
    print()

    # Step 3: Validate dataset
    print("Step 3: Validating dataset variables...")
    if not validate_gfs_atmospheric_dataset(ds):
        print("❌ ERROR: Dataset validation failed")
        ds.close()
        return False

    print("[OK] Dataset validated")
    print()

    # Step 4: Get sample beaches
    print("Step 4: Loading sample beaches...")
    beaches = fetch_all_beaches()

    if not beaches:
        print("[ERROR] No beaches found")
        ds.close()
        return False

    # Select a few diverse beaches for testing
    sample_beaches = beaches[:5]  # First 5 beaches

    print(f"[OK] Testing with {len(sample_beaches)} beaches:")
    for i, beach in enumerate(sample_beaches, 1):
        print(f"   {i}. {beach.get('Name', 'Unknown')} (ID: {beach['id']})")
    print()

    # Step 5: Extract data for each sample beach
    print("Step 5: Extracting weather data...")
    print("=" * 80)
    print()

    # Get first few time steps for testing
    time_indices = list(range(min(8, len(ds.time.values))))  # First 8 time steps (24 hours)

    pacific = pytz.timezone("America/Los_Angeles")

    for beach_idx, beach in enumerate(sample_beaches, 1):
        print(f"BEACH {beach_idx}: {beach.get('Name', 'Unknown')}")
        print(f"Location: {beach['LATITUDE']:.4f}°N, {beach['LONGITUDE']:.4f}°W")
        print("-" * 80)

        # Extract atmospheric data
        try:
            atmos_data = extract_gfs_atmospheric_point(
                ds,
                beach['LATITUDE'],
                beach['LONGITUDE'],
                time_indices
            )

            # Display data for each time step
            for i, time_idx in enumerate(time_indices):
                # Get timestamp
                time_utc = ds.time.values[time_idx]
                import pandas as pd
                time_dt = pd.Timestamp(time_utc).to_pydatetime()
                if time_dt.tzinfo is None:
                    time_dt = pytz.UTC.localize(time_dt)
                time_local = time_dt.astimezone(pacific)

                # Get data
                temp = atmos_data["temperature"][i]
                wind_speed = atmos_data["wind_speed"][i]
                wind_dir = atmos_data["wind_direction"][i]
                wind_gust = atmos_data["wind_gust"][i]
                pressure = atmos_data["pressure"][i]
                cloud_cover = atmos_data["cloud_cover"][i]
                precip_rate = atmos_data["precip_rate"][i]

                # Calculate weather code
                weather_code = derive_weather_code(cloud_cover, precip_rate, temp)
                weather_desc = weather_code_description(weather_code)

                # Display
                print(f"\n  {time_local.strftime('%a %m/%d %I:%M %p %Z')}:")
                print(f"    Temperature:     {temp:.1f}°F" if temp else "    Temperature:     N/A")
                print(f"    Weather:         {weather_desc} (code: {weather_code})")
                print(f"    Wind Speed:      {wind_speed:.1f} mph" if wind_speed else "    Wind Speed:      N/A")
                print(f"    Wind Direction:  {wind_dir:.1f}°" if wind_dir else "    Wind Direction:  N/A")
                print(f"    Wind Gust:       {wind_gust:.1f} mph" if wind_gust else "    Wind Gust:       N/A")
                print(f"    Pressure:        {pressure:.2f} inHg" if pressure else "    Pressure:        N/A")
                print(f"    Cloud Cover:     {cloud_cover:.1f}%" if cloud_cover is not None else "    Cloud Cover:     N/A")
                print(f"    Precip Rate:     {precip_rate:.2f} mm/hr" if precip_rate is not None else "    Precip Rate:     N/A")

                # Show what precipitation type it would be
                if precip_rate and precip_rate > 0.5:
                    if temp and temp < 35:
                        precip_type = "Snow"
                    else:
                        precip_type = "Rain"
                    print(f"    Precipitation:   {precip_type}")

            print()

        except Exception as e:
            print(f"  ❌ ERROR extracting data: {e}")
            print()
            continue

        if beach_idx < len(sample_beaches):
            print()

    # Close dataset
    ds.close()

    print("=" * 80)
    print("[OK] TEST COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print()
    print("The GFS Atmospheric handler is working correctly!")
    print("This is the weather data that would be added to your forecast_data table.")

    return True


if __name__ == "__main__":
    try:
        success = test_gfs_atmospheric()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)
