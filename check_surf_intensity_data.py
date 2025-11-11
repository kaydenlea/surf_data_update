#!/usr/bin/env python3
"""
Diagnostic script to check surf intensity data availability.
"""

from database import supabase
from datetime import datetime, timedelta
import pytz

def check_daily_grid_intensity():
    """Check daily_grid_surf_intensity table."""
    print("=" * 80)
    print("CHECKING daily_grid_surf_intensity TABLE")
    print("=" * 80)
    
    try:
        # Check total count
        result = supabase.table("daily_grid_surf_intensity").select("*", count="exact").limit(1).execute()
        print(f"Total records: {result.count}")
        
        # Get date range
        result = supabase.table("daily_grid_surf_intensity").select("date").order("date", desc=False).limit(1).execute()
        if result.data:
            print(f"Earliest date: {result.data[0]['date']}")
        
        result = supabase.table("daily_grid_surf_intensity").select("date").order("date", desc=True).limit(1).execute()
        if result.data:
            print(f"Latest date: {result.data[0]['date']}")
        
        # Check today's data
        today = datetime.now(pytz.timezone('America/Los_Angeles')).date().isoformat()
        result = supabase.table("daily_grid_surf_intensity").select("*").eq("date", today).execute()
        print(f"\nRecords for today ({today}): {len(result.data)}")
        
        if result.data:
            print(f"Sample: {result.data[0]}")
            
    except Exception as e:
        print(f"ERROR: {e}")


def check_grid_forecast_data():
    """Check grid_forecast_data table."""
    print("\n" + "=" * 80)
    print("CHECKING grid_forecast_data TABLE")
    print("=" * 80)
    
    try:
        # Check total count
        result = supabase.table("grid_forecast_data").select("*", count="exact").limit(1).execute()
        print(f"Total records: {result.count}")
        
        # Get timestamp range
        result = supabase.table("grid_forecast_data").select("timestamp").order("timestamp", desc=False).limit(1).execute()
        if result.data:
            print(f"Earliest timestamp: {result.data[0]['timestamp']}")
        
        result = supabase.table("grid_forecast_data").select("timestamp").order("timestamp", desc=True).limit(1).execute()
        if result.data:
            print(f"Latest timestamp: {result.data[0]['timestamp']}")
        
        # Check how many have surf_height_max_ft
        result = supabase.table("grid_forecast_data").select("surf_height_max_ft", count="exact").not_("surf_height_max_ft", "is", "null").limit(1).execute()
        print(f"\nRecords with surf_height_max_ft: {result.count}")
        
        # Check unique grid_ids
        result = supabase.table("grid_forecast_data").select("grid_id").limit(1000).execute()
        grid_ids = set(r['grid_id'] for r in result.data if r.get('grid_id'))
        print(f"Unique grid_ids (sample): {len(grid_ids)}")
        
    except Exception as e:
        print(f"ERROR: {e}")


def check_beaches_grid_mapping():
    """Check if beaches have grid_id assigned."""
    print("\n" + "=" * 80)
    print("CHECKING BEACHES → GRID MAPPING")
    print("=" * 80)
    
    try:
        # Total beaches
        result = supabase.table("beaches").select("*", count="exact").limit(1).execute()
        print(f"Total beaches: {result.count}")
        
        # Beaches with grid_id
        result = supabase.table("beaches").select("id, grid_id", count="exact").not_("grid_id", "is", "null").limit(1).execute()
        print(f"Beaches with grid_id: {result.count}")
        
        # Sample mapping
        result = supabase.table("beaches").select("id, Name, grid_id").not_("grid_id", "is", "null").limit(5).execute()
        print("\nSample beach→grid mappings:")
        for beach in result.data:
            print(f"  Beach {beach['id']} ({beach.get('Name', 'Unknown')}): grid_id={beach['grid_id']}")
            
    except Exception as e:
        print(f"ERROR: {e}")


def check_grid_points():
    """Check grid_points table."""
    print("\n" + "=" * 80)
    print("CHECKING grid_points TABLE")
    print("=" * 80)
    
    try:
        result = supabase.table("grid_points").select("*", count="exact").execute()
        print(f"Total grid points: {result.count}")
        
        if result.data:
            print(f"\nSample grid point: {result.data[0]}")
            
    except Exception as e:
        print(f"ERROR: {e}")


if __name__ == "__main__":
    print("SURF INTENSITY DATA DIAGNOSTIC")
    print("=" * 80)
    print()
    
    check_grid_points()
    check_beaches_grid_mapping()
    check_grid_forecast_data()
    check_daily_grid_intensity()
    
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS:")
    print("=" * 80)
    print("1. If beaches don't have grid_id: Run a script to assign nearest grid points")
    print("2. If grid_forecast_data is empty: Run main_noaa_grid.py to populate")
    print("3. If daily_grid_surf_intensity is empty: Run nowcast_grid.py to populate")
    print("=" * 80)
