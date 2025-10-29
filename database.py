#!/usr/bin/env python3
"""
Database operations for Hybrid Surf Database Update Script
Handles all Supabase interactions including data fetching, cleanup, and upserts
"""

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from numbers import Real
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY, UPSERT_CHUNK
import pytz
from utils import log_step, valid_coord, chunk_iter, safe_float

# Get shared logger
logger = logging.getLogger("surf_update")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

_BEACH_COORD_CACHE = None

FIELDS_FOR_NEIGHBOR_FILL = (
    'weather',
    'wind_direction_deg',
    'secondary_swell_height_ft',
    'secondary_swell_period_s',
    'secondary_swell_direction',
    'tertiary_swell_height_ft',
    'tertiary_swell_period_s',
    'tertiary_swell_direction',
)


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
                .select("id,Name,LATITUDE,LONGITUDE,COUNTY", count="exact")
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
        {
            "id": b["id"],
            "Name": b["Name"],
            "LATITUDE": b["LATITUDE"],
            "LONGITUDE": b["LONGITUDE"],
            "COUNTY": b.get("COUNTY", "Unknown")
        }
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



def _normalize_timestamp(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        ts_str = str(ts).strip()
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(ts_str)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)
    return dt.isoformat()


def _haversine_distance(lat1, lon1, lat2, lon2):
    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
    except (TypeError, ValueError):
        return float('inf')
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(min(1.0, math.sqrt(max(0.0, a))))
    return 6371.0 * c


def _get_beach_coord_map():
    global _BEACH_COORD_CACHE
    if _BEACH_COORD_CACHE is not None:
        return _BEACH_COORD_CACHE
    logger.debug('Fetching beach coordinate map for neighbor fill')
    resp = supabase.table('beaches').select('id,LATITUDE,LONGITUDE').execute()
    data = resp.data or []
    coord_map = {}
    for row in data:
        lat = row.get('LATITUDE')
        lon = row.get('LONGITUDE')
        if valid_coord(lat) and valid_coord(lon):
            coord_map[row['id']] = (float(lat), float(lon))
    _BEACH_COORD_CACHE = coord_map
    logger.debug('Loaded %s beach coordinates', len(coord_map))
    return coord_map


def _fill_records_with_neighbors(records, fields=None, fill_surf_min=False):
    if not records:
        return records
    coord_map = _get_beach_coord_map()
    fields = tuple(fields) if fields else FIELDS_FOR_NEIGHBOR_FILL
    grouped = defaultdict(list)
    for idx, rec in enumerate(records):
        ts_norm = _normalize_timestamp(rec.get('timestamp'))
        bid = rec.get('beach_id')
        if ts_norm is None or bid not in coord_map:
            continue
        grouped[(ts_norm)].append(idx)

    for ts_key, indices in grouped.items():
        per_meta = []
        for idx in indices:
            bid = records[idx].get('beach_id')
            per_meta.append(coord_map.get(bid))
        for field in fields:
            donors = []  # (lat, lon, value)
            missing = []  # (idx, lat, lon)
            for meta, rec_idx in zip(per_meta, indices):
                if meta is None:
                    continue
                lat, lon = meta
                value = records[rec_idx].get(field)
                if value is None:
                    missing.append((rec_idx, lat, lon))
                else:
                    donors.append((lat, lon, value))
            if not missing:
                continue
            if not donors:
                continue
            for rec_idx, lat, lon in missing:
                best_value = None
                best_distance = float('inf')
                for d_lat, d_lon, d_value in donors:
                    distance = _haversine_distance(lat, lon, d_lat, d_lon)
                    if distance < best_distance:
                        best_distance = distance
                        best_value = d_value
                if best_value is not None:
                    records[rec_idx][field] = best_value
                    donors.append((lat, lon, best_value))
    if fill_surf_min:
        for rec in records:
            if rec.get('surf_height_min_ft') is None:
                rec['surf_height_min_ft'] = 0.0
    return records


def _prepare_records_for_upsert(records, required_keys, skip_zero_floats=False, allow_zero_fields=None):
    """Drop None (and optionally zero-float) fields before upsert to preserve prior values.

    Args:
        records: List of record dicts
        required_keys: Set of keys that must be present
        skip_zero_floats: If True, skip fields with value 0.0 (except for allow_zero_fields)
        allow_zero_fields: Set of field names where 0 is a valid value (e.g., wind_speed_mph)
    """
    if allow_zero_fields is None:
        allow_zero_fields = set()

    cleaned = []
    for rec in records:
        if any(rec.get(key) is None for key in required_keys):
            logger.debug(f"   Skipping record missing required keys {required_keys}: {rec}")
            continue
        filtered = {}
        for key, value in rec.items():
            if key in required_keys:
                filtered[key] = value
                continue
            if value is None:
                continue
            # Skip zero values EXCEPT for fields where 0 is valid (like wind speed)
            if skip_zero_floats and key not in allow_zero_fields and isinstance(value, Real) and not isinstance(value, bool):
                try:
                    as_float = float(value)
                except (TypeError, ValueError):
                    as_float = None
                if as_float is not None and abs(as_float) < 1e-9:
                    continue
            filtered[key] = value
        cleaned.append(filtered)
    return cleaned

def _deduplicate_records(records, unique_keys):
    """
    Deduplicate records based on unique keys, keeping the last occurrence.
    This merges duplicate records by combining all non-null fields.

    Args:
        records: List of record dicts
        unique_keys: Tuple of keys that uniquely identify a record (e.g., ("beach_id", "timestamp"))

    Returns:
        List of deduplicated records
    """
    if not records:
        return records

    # Use a dict to track records by unique key
    merged = {}
    duplicates_found = 0

    for rec in records:
        # Create a key tuple from the unique keys
        key_values = tuple(rec.get(k) for k in unique_keys)

        # Skip if any unique key is missing
        if any(v is None for v in key_values):
            continue

        if key_values in merged:
            # Merge with existing record: new values override old ones (but keep old non-null values)
            duplicates_found += 1
            existing = merged[key_values]
            for field, value in rec.items():
                if value is not None:
                    existing[field] = value
        else:
            # First time seeing this key
            merged[key_values] = dict(rec)  # Make a copy

    if duplicates_found > 0:
        logger.info(f"   Deduplicated {duplicates_found} duplicate records (merged fields)")

    return list(merged.values())


def upsert_forecast_data(records, table_name="forecast_data"):
    """Upsert forecast records to database in chunks."""
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0

    enriched = _fill_records_with_neighbors(records, fill_surf_min=True)
    # Allow zero values for fields where 0 is valid (calm winds = 0 mph, freezing = 0°F, etc.)
    prepared = _prepare_records_for_upsert(
        enriched,
        {"beach_id", "timestamp"},
        skip_zero_floats=True,
        allow_zero_fields={"wind_speed_mph", "wind_gust_mph", "temperature"}
    )
    if not prepared:
        logger.warning(f"   No forecast records had non-null values to upsert into {table_name}")
        return 0

    # Deduplicate records to avoid "cannot affect row a second time" error
    deduplicated = _deduplicate_records(prepared, ("beach_id", "timestamp"))

    logger.info(f"   Uploading {len(deduplicated)} records to {table_name}...")
    total_inserted = 0

    for chunk in chunk_iter(deduplicated, UPSERT_CHUNK):
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
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0

    prepared = _prepare_records_for_upsert(records, {"county", "date"})
    if not prepared:
        logger.warning(f"   No daily records had non-null values to upsert into {table_name}")
        return 0

    # Deduplicate records
    deduplicated = _deduplicate_records(prepared, ("county", "date"))

    logger.info(f"   Uploading {len(deduplicated)} daily records to {table_name}...")
    total_inserted = 0

    for chunk in chunk_iter(deduplicated, UPSERT_CHUNK):
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
    """Upsert tide records to database in chunks."""
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0

    prepared = _prepare_records_for_upsert(records, {"beach_id", "timestamp"})
    if not prepared:
        logger.warning(f"   No tide records had non-null values to upsert into {table_name}")
        return 0

    # Deduplicate records
    deduplicated = _deduplicate_records(prepared, ("beach_id", "timestamp"))

    logger.info(f"   Uploading {len(deduplicated)} tide records to {table_name}...")
    total_inserted = 0

    for chunk in chunk_iter(deduplicated, UPSERT_CHUNK):
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
            cleaned = cutoff.strip()
            cleaned = cleaned.replace('Z', '+00:00') if cleaned.endswith('Z') else cleaned
            cutoff_dt = datetime.fromisoformat(cleaned)
        else:
            cutoff_dt = cutoff

        if cutoff_dt.tzinfo is None:
            cutoff_dt = pacific.localize(cutoff_dt)
        else:
            cutoff_dt = cutoff_dt.astimezone(pacific)

        cutoff_dt = pacific.normalize(cutoff_dt)
        cutoff_variants = [
            ("utc", cutoff_dt.astimezone(pytz.utc).isoformat().replace('+00:00', 'Z')),
            ("pacific", cutoff_dt.isoformat()),
        ]

        deleted = 0
        used_variant = None
        last_error = None

        for label, cutoff_iso in cutoff_variants:
            try:
                resp = supabase.table(table_name).delete().lt("timestamp", cutoff_iso).execute()
                deleted = len(resp.data) if hasattr(resp, 'data') and resp.data is not None else 0
                used_variant = (label, cutoff_iso)
                if deleted:
                    logger.info(
                        f"   Deleted {deleted} tide rows before {cutoff_iso} using {label} cutoff"
                    )
                    break
                else:
                    logger.debug(
                        f"   No tide rows matched cutoff {cutoff_iso} using {label} comparison"
                    )
            except Exception as delete_err:
                last_error = delete_err
                logger.debug(
                    f"   Tide delete attempt with cutoff {cutoff_iso} ({label}) failed: {delete_err}"
                )

        if used_variant is None:
            raise last_error or RuntimeError("Unable to execute tide deletion request")

        if deleted == 0:
            logger.info(
                f"   No tide rows older than {cutoff_dt.isoformat()} found to delete"
            )

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

def upsert_county_tide_data(records, table_name="county_tides_15min"):
    """Upsert county-based tide records to database in chunks."""
    if not records:
        logger.warning(f"   No records to upsert to {table_name}")
        return 0

    prepared = _prepare_records_for_upsert(records, {"county", "timestamp"})
    if not prepared:
        logger.warning(f"   No county tide records had non-null values to upsert into {table_name}")
        return 0

    # Deduplicate records
    deduplicated = _deduplicate_records(prepared, ("county", "timestamp"))

    logger.info(f"   Uploading {len(deduplicated)} county tide records to {table_name}...")
    total_inserted = 0

    for chunk in chunk_iter(deduplicated, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk,
                on_conflict="county,timestamp"
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"   Upserted chunk of {len(chunk)} county tide records")
        except Exception as e:
            logger.error(f"ERROR: Error upserting {table_name} chunk: {e}")

    logger.info(f"   Successfully upserted {total_inserted} county tide records to {table_name}")
    return total_inserted


def delete_all_county_tide_data(table_name="county_tides_15min"):
    """Delete all existing county tide records."""
    try:
        supabase.table(table_name).delete().neq("county", None).execute()
        logger.info(f"   Deleted all rows from {table_name}")
        return True
    except Exception as e:
        logger.error(f"ERROR: Failed to delete existing county tide data from {table_name}: {e}")
        return False


def delete_county_tide_data_before(cutoff, table_name="county_tides_15min"):
    """Delete county tide records older than the given cutoff."""
    try:
        pacific = pytz.timezone('America/Los_Angeles')
        if isinstance(cutoff, str):
            cleaned = cutoff.strip()
            cleaned = cleaned.replace('Z', '+00:00') if cleaned.endswith('Z') else cleaned
            cutoff_dt = datetime.fromisoformat(cleaned)
        else:
            cutoff_dt = cutoff

        if cutoff_dt.tzinfo is None:
            cutoff_dt = pacific.localize(cutoff_dt)
        else:
            cutoff_dt = cutoff_dt.astimezone(pacific)

        cutoff_dt = pacific.normalize(cutoff_dt)
        cutoff_iso = cutoff_dt.isoformat()

        resp = supabase.table(table_name).delete().lt("timestamp", cutoff_iso).execute()
        deleted = len(resp.data) if hasattr(resp, 'data') and resp.data is not None else 0

        if deleted:
            logger.info(f"   Deleted {deleted} county tide rows before {cutoff_iso}")
        else:
            logger.info(f"   No county tide rows older than {cutoff_iso} found to delete")

        return True
    except Exception as e:
        logger.error(f"ERROR: Failed to delete outdated county tide data before {cutoff}: {e}")
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
        else:
            cutoff_dt = cutoff_dt.astimezone(pacific)

        cutoff_dt = pacific.normalize(cutoff_dt)
        return delete_tide_data_before(cutoff_dt, table_name="forecast_data")
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

def fetch_existing_forecast_records(page_size: int = 1000):
    """
    Fetch all existing forecast records from the database.
    Used by step2_supplement_data.py to enhance wave data with atmospheric/tides.
    
    Returns:
        List of forecast record dicts with beach_id, timestamp, and all existing fields
    """
    logger.info("Fetching existing forecast records from database...")
    
    all_rows = []
    start = 0
    while True:
        try:
            end_idx = start + page_size - 1
            resp = (
                supabase
                .table("forecast_data")
                .select("*", count="exact")
                .range(start, end_idx)
                .execute()
            )
            rows = resp.data or []
            all_rows.extend(rows)
            
            logger.info(f"   Fetched {len(rows)} forecast records (batch {start//page_size + 1})")
            
            if len(rows) < page_size:
                break
            
            start += page_size
        except Exception as e:
            logger.error(f"ERROR: Failed to fetch forecast records: {e}")
            break
    
    # Filter valid records (must have beach_id and timestamp)
    valid_rows = []
    for row in all_rows:
        if row.get("beach_id") and row.get("timestamp"):
            valid_rows.append(row)
    
    logger.info(f"OK: Found {len(valid_rows)} valid forecast records")
    return valid_rows
