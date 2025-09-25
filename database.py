#!/usr/bin/env python3
"""
Database operations for Hybrid Surf Database Update Script
Handles all Supabase interactions including data fetching, cleanup, and upserts
"""

import logging
from datetime import datetime, timedelta, time as dtime
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY, UPSERT_CHUNK
import pytz
from utils import log_step, valid_coord, chunk_iter

# Get shared logger
logger = logging.getLogger("surf_update")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def cleanup_old_data(batch_size: int = 100):
    """Delete stale forecast and daily condition data prior to today's window.

    This run keeps the current-day forecast horizon intact and only removes rows
    older than today's Pacific midnight (forecast_data) or calendar date
    (daily_county_conditions).
    """
    log_step("Cleaning up old data", 1)

    try:
        pacific = pytz.timezone('America/Los_Angeles')
        now_pacific = datetime.now(pacific)
        midnight_today = pacific.localize(datetime.combine(now_pacific.date(), dtime(0, 0)))
        cutoff_date = now_pacific.date()

        logger.info(f"DELETE: Removing forecast_data before {midnight_today.isoformat()} (Pacific)")
        forecast_ok = cleanup_forecast_data_by_date(midnight_today)

        logger.info(f"DELETE: Removing beach_tides_hourly before {midnight_today.isoformat()} (Pacific)")
        tide_ok = delete_tide_data_before(midnight_today)

        logger.info(f"DELETE: Removing daily_county_conditions before {cutoff_date.isoformat()}")
        daily_ok = cleanup_daily_conditions_by_date(cutoff_date)

        log_step("Old data cleanup completed")
        return forecast_ok and tide_ok and daily_ok

    except Exception as e:
        logger.error(f"ERROR: Error during cleanup: {e}")
        return False

def fetch_all_beaches(page_size: int = 1000):
    """Fetch all beaches with valid coordinates."""
    log_step("Fetching beach data", 2)
    
    all_rows = []
    start = 0
    while True:
        try:
            end_idx = start + page_size - 1
            resp = (
                supabase
                .table("beaches")
                .select("id,Name,LATITUDE,LONGITUDE", count="exact")
                .range(start, end_idx)
                .execute()
            )
            rows = resp.data or []
            all_rows.extend(rows)
            
            logger.info(f"   Fetched {len(rows)} beaches (batch {start//page_size + 1})")
            
            if len(rows) < page_size:
                break
            start += page_size
            
        except Exception as e:
            logger.error(f"ERROR: Error fetching beaches: {e}")
            break
    
    # Filter for valid coordinates
    valid_beaches = [
        {"id": b["id"], "Name": b["Name"], "LATITUDE": b["LATITUDE"], "LONGITUDE": b["LONGITUDE"]}
        for b in all_rows
        if valid_coord(b.get("LATITUDE")) and valid_coord(b.get("LONGITUDE"))
    ]
    
    log_step(f"Found {len(valid_beaches)} beaches with valid coordinates")
    return valid_beaches

def fetch_all_counties(page_size: int = 1000):
    """Get unique counties with their centroid coordinates."""
    log_step("Calculating county centroids", 3)
    
    all_rows = []
    start = 0
    while True:
        try:
            end_idx = start + page_size - 1
            resp = (
                supabase
                .table("beaches")
                .select("COUNTY,LATITUDE,LONGITUDE", count="exact")
                .range(start, end_idx)
                .execute()
            )
            rows = resp.data or []
            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            start += page_size
        except Exception as e:
            logger.error(f"ERROR: Error fetching county data: {e}")
            break
    
    # Group by county and calculate centroid coordinates
    county_data = {}
    for row in all_rows:
        county = row.get("COUNTY")
        lat = row.get("LATITUDE")
        lon = row.get("LONGITUDE")
        
        if county and valid_coord(lat) and valid_coord(lon):
            if county not in county_data:
                county_data[county] = {"lats": [], "lons": []}
            county_data[county]["lats"].append(lat)
            county_data[county]["lons"].append(lon)
    
    # Calculate centroids
    counties = []
    for county, coords in county_data.items():
        centroid_lat = sum(coords["lats"]) / len(coords["lats"])
        centroid_lon = sum(coords["lons"]) / len(coords["lons"])
        counties.append({
            "county": county,
            "latitude": centroid_lat,
            "longitude": centroid_lon,
            "beach_count": len(coords["lats"])
        })
    
    log_step(f"Calculated centroids for {len(counties)} counties")
    return counties

def upsert_forecast_data(records, table_name="forecast_data"):
    """Upsert forecast records to database in chunks."""
    logger.info(f"   Uploading {len(records)} records to {table_name}...")
    total_inserted = 0
    
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0
    
    for chunk in chunk_iter(records, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk, 
                on_conflict="beach_id,timestamp"
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"   Upserted chunk of {len(chunk)} records")
        except Exception as e:
            logger.error(f"ERROR: Error upserting {table_name} chunk: {e}")
    
    logger.info(f"   Successfully upserted {total_inserted} records to {table_name}")
    return total_inserted

def upsert_daily_conditions(records, table_name="daily_county_conditions"):
    """Upsert daily condition records to database in chunks."""
    logger.info(f"   Uploading {len(records)} daily records to {table_name}...")
    total_inserted = 0
    
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0
    
    for chunk in chunk_iter(records, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk,
                on_conflict="county,date"
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"   Upserted chunk of {len(chunk)} daily records")
        except Exception as e:
            logger.error(f"ERROR: Error upserting {table_name} chunk: {e}")
    
    logger.info(f"   Successfully upserted {total_inserted} daily records to {table_name}")
    return total_inserted

def upsert_tide_data(records, table_name="beach_tides_hourly"):
    """Upsert tide records (hourly) to database in chunks."""
    logger.info(f"   Uploading {len(records)} tide records to {table_name}...")
    total_inserted = 0

    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0

    for chunk in chunk_iter(records, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk,
                on_conflict="beach_id,timestamp"
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"   Upserted chunk of {len(chunk)} tide records")
        except Exception as e:
            logger.error(f"ERROR: Error upserting {table_name} chunk: {e}")

    logger.info(f"   Successfully upserted {total_inserted} tide records to {table_name}")
    return total_inserted

def delete_all_tide_data(table_name="beach_tides_hourly"):
    """Delete all existing tide records (safe coarse delete)."""
    try:
        supabase.table(table_name).delete().neq("beach_id", None).execute()
        logger.info(f"   Deleted all rows from {table_name}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Failed to delete existing tide data from {table_name}: {e}")
        return False

def delete_tide_data_before(cutoff, table_name="beach_tides_hourly"):
    """Delete tide records older than the given cutoff (datetime or ISO string)."""
    try:
        pacific = pytz.timezone('America/Los_Angeles')
        if isinstance(cutoff, str):
            cleaned = cutoff.replace('Z', '+00:00') if cutoff.endswith('Z') else cutoff
            cutoff_dt = datetime.fromisoformat(cleaned)
        else:
            cutoff_dt = cutoff
        if cutoff_dt.tzinfo is None:
            cutoff_dt = pacific.localize(cutoff_dt)
        else:
            cutoff_dt = cutoff_dt.astimezone(pacific)
        cutoff_iso = cutoff_dt.isoformat()
        resp = supabase.table(table_name).delete().lt("timestamp", cutoff_iso).execute()
        deleted = len(resp.data) if hasattr(resp, 'data') and resp.data is not None else None
        if deleted is not None:
            logger.info(f"   Deleted {deleted} tide rows before {cutoff_iso}")
        else:
            logger.info(f"   Deleted tide rows before {cutoff_iso}")
        try:
            earliest_resp = (
                supabase.table(table_name)
                .select('timestamp')
                .order('timestamp', desc=False)
                .limit(1)
                .execute()
            )
            if earliest_resp.data:
                earliest = earliest_resp.data[0].get('timestamp')
                logger.info(f"   Earliest tide timestamp remaining: {earliest}")
        except Exception as log_err:
            logger.debug(f"   Unable to fetch earliest tide timestamp: {log_err}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Failed to delete outdated tide data before {cutoff}: {e}")
        return False

def get_beach_by_id(beach_id):
    """Get a specific beach by ID."""
    try:
        resp = supabase.table("beaches").select("*").eq("id", beach_id).execute()
        if resp.data and len(resp.data) > 0:
            return resp.data[0]
        return None
    except Exception as e:
        logger.error(f"ERROR: Error fetching beach {beach_id}: {e}")
        return None

def get_beaches_by_county(county_name):
    """Get all beaches in a specific county."""
    try:
        resp = supabase.table("beaches").select("*").eq("COUNTY", county_name).execute()
        return resp.data or []
    except Exception as e:
        logger.error(f"ERROR: Error fetching beaches for county {county_name}: {e}")
        return []

def check_database_connection():
    """Test database connection."""
    try:
        # Simple query to test connection
        resp = supabase.table("beaches").select("id").limit(1).execute()
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"ERROR: Database connection failed: {e}")
        return False

def get_table_record_count(table_name):
    """Get record count for a table."""
    try:
        resp = supabase.table(table_name).select("*", count="exact").execute()
        count = resp.count if resp.count is not None else 0
        logger.info(f"   {table_name}: {count} records")
        return count
    except Exception as e:
        logger.error(f"ERROR: Error counting records in {table_name}: {e}")
        return 0

def cleanup_forecast_data_by_date(cutoff_date):
    """Delete forecast data older than cutoff date."""
    try:
        pacific = pytz.timezone('America/Los_Angeles')
        if isinstance(cutoff_date, datetime):
            cutoff_dt = cutoff_date
        else:
            cutoff_dt = datetime.combine(cutoff_date, dtime(0, 0))
        if cutoff_dt.tzinfo is None:
            cutoff_dt = pacific.localize(cutoff_dt)
        cutoff_iso = cutoff_dt.isoformat()
        resp = supabase.table("forecast_data").delete().lt('timestamp', cutoff_iso).execute()
        deleted = len(resp.data) if hasattr(resp, 'data') and resp.data is not None else None
        if deleted is not None:
            logger.info(f"   Cleaned up {deleted} forecast rows older than {cutoff_iso}")
        else:
            logger.info(f"   Cleaned up forecast data older than {cutoff_iso}")
        try:
            earliest_resp = (
                supabase.table('forecast_data')
                .select('timestamp')
                .order('timestamp', desc=False)
                .limit(1)
                .execute()
            )
            if earliest_resp.data:
                earliest = earliest_resp.data[0].get('timestamp')
                logger.info(f"   Earliest forecast timestamp remaining: {earliest}")
        except Exception as log_err:
            logger.debug(f"   Unable to fetch earliest forecast timestamp: {log_err}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Error cleaning up forecast data: {e}")
        return False

def cleanup_daily_conditions_by_date(cutoff_date):
    """Delete daily conditions older than cutoff date."""
    try:
        cutoff_str = cutoff_date.strftime('%Y-%m-%d')
        resp = supabase.table("daily_county_conditions").delete().lt('date', cutoff_str).execute()
        logger.info(f"   Cleaned up daily conditions older than {cutoff_str}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Error cleaning up daily conditions: {e}")
        return False

def validate_database_schema():
    """Validate that required tables and columns exist."""
    required_tables = ["beaches", "forecast_data", "daily_county_conditions"]
    
    for table in required_tables:
        try:
            # Try to select one record to validate table exists
            resp = supabase.table(table).select("*").limit(1).execute()
            logger.debug(f"   Table '{table}' exists and accessible")
        except Exception as e:
            logger.error(f"ERROR: Table '{table}' validation failed: {e}")
            return False
    
    logger.info("Database schema validation passed")
    return True

def get_database_stats():
    """Get statistics about the database."""
    stats = {}
    tables = ["beaches", "forecast_data", "daily_county_conditions"]
    
    for table in tables:
        stats[table] = get_table_record_count(table)
    
    logger.info("Database statistics:")
    for table, count in stats.items():
        logger.info(f"   {table}: {count:,} records")
    
    return stats
