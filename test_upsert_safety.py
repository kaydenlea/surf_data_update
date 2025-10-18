#!/usr/bin/env python3
"""
Test that our upsert logic doesn't overwrite previously filled fields.
"""

from database import supabase
from datetime import datetime, timedelta
import pytz

def test_upsert_safety():
    """Verify that partial upserts don't overwrite other fields."""

    # Find a test record
    resp = supabase.table("forecast_data").select(
        "id,beach_id,timestamp,weather,tertiary_swell_height_ft"
    ).not_.is_("weather", "null").not_.is_("tertiary_swell_height_ft", "null").limit(1).execute()

    if not resp.data:
        print("No suitable test record found")
        return

    test_record = resp.data[0]
    record_id = test_record["id"]
    original_weather = test_record["weather"]
    original_tertiary = test_record["tertiary_swell_height_ft"]

    print(f"Test record ID: {record_id}")
    print(f"Original weather: {original_weather}")
    print(f"Original tertiary_swell_height_ft: {original_tertiary}")

    # Test 1: Update only weather field
    print("\nTest 1: Updating only weather field...")
    new_weather = 999 if original_weather != 999 else 888

    update_payload = {
        "id": record_id,
        "beach_id": test_record["beach_id"],
        "timestamp": test_record["timestamp"],
        "weather": new_weather
    }

    supabase.table("forecast_data").upsert(
        update_payload,
        on_conflict="id",
        ignore_duplicates=False
    ).execute()

    # Verify tertiary_swell_height_ft was NOT overwritten
    verify = supabase.table("forecast_data").select(
        "weather,tertiary_swell_height_ft"
    ).eq("id", record_id).single().execute()

    print(f"After update:")
    print(f"  weather: {verify.data['weather']} (expected: {new_weather})")
    print(f"  tertiary_swell_height_ft: {verify.data['tertiary_swell_height_ft']} (expected: {original_tertiary})")

    if verify.data['weather'] == new_weather and verify.data['tertiary_swell_height_ft'] == original_tertiary:
        print("✓ Test 1 PASSED: Partial upsert did not overwrite other fields")
    else:
        print("✗ Test 1 FAILED: Partial upsert corrupted other fields!")

    # Restore original value
    print("\nRestoring original values...")
    restore_payload = {
        "id": record_id,
        "beach_id": test_record["beach_id"],
        "timestamp": test_record["timestamp"],
        "weather": original_weather,
        "tertiary_swell_height_ft": original_tertiary
    }

    supabase.table("forecast_data").upsert(
        restore_payload,
        on_conflict="id",
        ignore_duplicates=False
    ).execute()

    print("✓ Original values restored")

if __name__ == "__main__":
    test_upsert_safety()
