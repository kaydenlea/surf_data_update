#!/usr/bin/env python3
"""
Neighbor fill post-processor for `forecast_data`.
Copy missing values from the nearest beach at the same (bucketed) timestamp.
Optionally, if a field has no donors at that time bucket, search nearby buckets.
"""

from __future__ import annotations

import argparse
import math
import time
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import pytz

from config import logger
from database import fetch_all_beaches, supabase
from utils import chunk_iter

# --------------------------------------------------------------------- #
# IMPROVED: Auto-detect all fillable fields from forecast_data table
# --------------------------------------------------------------------- #
# Exclude these columns from neighbor fill (primary keys, timestamps, etc.)
EXCLUDED_FIELDS = {
    "id",
    "beach_id",
    "timestamp",
    "created_at",
    "updated_at",
}

def get_all_fillable_fields() -> Tuple[str, ...]:
    """
    Auto-detect all fillable columns from the forecast_data table.
    Excludes primary keys, identifiers, and timestamps.
    """
    from database import supabase

    try:
        # Get a sample record to find all column names
        result = supabase.table("forecast_data").select("*").limit(1).execute()

        if not result.data:
            # Fallback to known fields if table is empty
            return (
                "primary_swell_height_ft",
                "primary_swell_period_s",
                "primary_swell_direction",
                "secondary_swell_height_ft",
                "secondary_swell_period_s",
                "secondary_swell_direction",
                "tertiary_swell_height_ft",
                "tertiary_swell_period_s",
                "tertiary_swell_direction",
                "surf_height_min_ft",
                "surf_height_max_ft",
                "wave_energy_kj",
                "wind_speed_mph",
                "wind_direction_deg",
                "wind_gust_mph",
                "water_temp_f",
                "tide_level_ft",
                "temperature",
                "weather",
                "pressure_inhg",
            )

        # Extract all field names except excluded ones
        all_fields = set(result.data[0].keys())
        fillable_fields = sorted(all_fields - EXCLUDED_FIELDS)

        return tuple(fillable_fields)

    except Exception as e:
        logger.warning(f"Could not auto-detect fields: {e}, using fallback list")
        # Fallback to comprehensive known fields
        return (
            "primary_swell_height_ft",
            "primary_swell_period_s",
            "primary_swell_direction",
            "secondary_swell_height_ft",
            "secondary_swell_period_s",
            "secondary_swell_direction",
            "tertiary_swell_height_ft",
            "tertiary_swell_period_s",
            "tertiary_swell_direction",
            "surf_height_min_ft",
            "surf_height_max_ft",
            "wave_energy_kj",
            "wind_speed_mph",
            "wind_direction_deg",
            "wind_gust_mph",
            "water_temp_f",
            "tide_level_ft",
            "temperature",
            "weather",
            "pressure_inhg",
        )

# Get all fillable fields at module load time (cached)
FIELDS_FOR_NEIGHBOR_FILL = get_all_fillable_fields()

# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fill missing forecast fields from nearest beaches (AUTO-DETECTS ALL COLUMNS)",
        epilog=f"Auto-detected fields: {', '.join(FIELDS_FOR_NEIGHBOR_FILL)}"
    )
    p.add_argument("--start-iso", help="ISO timestamp (inclusive) to start from. Default: all records (no limit).")
    p.add_argument("--hours-back", type=int, default=None,
                   help="If no --start-iso, how many hours back from now to start (default: None = all records).")
    p.add_argument("--fields", nargs="+",
                   help="Override auto-detection and specify fields to backfill (space-separated list).")
    p.add_argument("--page-size", type=int, default=1000)
    p.add_argument("--batch-size", type=int, default=100,
                   help="Number of rows to update per batch (default: 100, max recommended: 500)")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--per-field", action="store_true",
                   help="Process each field in its own pass.")
    p.add_argument("--time-fallback", type=int, default=6,
                   help="If a field has no donors at a bucket, allow fallback ±N buckets (default: 6, i.e., ±6 hours).")
    p.add_argument("--cadence", default="h",
                   help="Bucketing cadence for timestamps (default 'h'). Examples: '30min','15min'.")
    return p

# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #
def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance (km)."""
    try:
        lat1_rad = math.radians(float(lat1))
        lon1_rad = math.radians(float(lon1))
        lat2_rad = math.radians(float(lat2))
        lon2_rad = math.radians(float(lon2))
    except (TypeError, ValueError):
        return float("inf")
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(min(1.0, math.sqrt(max(0.0, a))))
    return 6371.0 * c

def pacific_midnight_today(now: Optional[datetime] = None) -> datetime:
    tz = pytz.timezone("America/Los_Angeles")
    now = now.astimezone(tz) if now and now.tzinfo else now or datetime.now(tz)
    return tz.localize(datetime.combine(now.date(), datetime.min.time()))

def normalize_timestamp(value: object, freq: str = "h") -> Optional[str]:
    """
    Coerce to UTC, snap to cadence bucket, return tz-naive ISO string key
    so equal buckets compare equal (e.g., '2025-10-02T07:00:00').
    """
    if value is None:
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
        ts = ts.floor(freq)
        return ts.tz_convert(None).isoformat()
    except Exception:
        return None

def has_real_value(value) -> bool:
    """Treat None, NaN, '', 'nan', 'none', 'null' as missing. 0 is valid."""
    if value is None:
        return False
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or stripped.lower() in {"nan", "none", "null"}:
            return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return True


def should_skip_filling(rec: Dict, field: str) -> bool:
    """
    Check if a field should be skipped for filling based on other field values.

    RULE: Skip surf_height_min_ft if surf_height_max_ft is 1 (no surf condition)
    """
    if field == "surf_height_min_ft":
        max_height = rec.get("surf_height_max_ft")
        if max_height is not None:
            try:
                if float(max_height) == 1.0:
                    return True  # Skip filling min if max is 1 (no surf)
            except (ValueError, TypeError):
                pass

    return False

# --------------------------------------------------------------------- #
# Fetch
# --------------------------------------------------------------------- #
def fetch_recent_forecast_records(
    start_iso: str,
    fields: Iterable[str],
    page_size: int = 1000,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> List[Dict]:
    # include id so we can update by primary key
    select_fields = ["id", "beach_id", "timestamp", *fields]
    select_clause = ",".join(select_fields)

    records: List[Dict] = []
    page = 0

    while True:
        start = page * page_size
        end = start + page_size - 1
        resp = (
            supabase
            .table("forecast_data")
            .select(select_clause)
            .gte("timestamp", start_iso)
            .order("timestamp", desc=False)   # deterministic pagination
            .order("beach_id", desc=False)
            .range(start, end)
            .execute()
        )
        batch = resp.data or []
        records.extend(batch)

        if verbose:
            logger.debug("Fetched %s records (cumulative %s)", len(batch), len(records))

        if len(batch) < page_size:
            break
        if limit and len(records) >= limit:
            records = records[:limit]
            break
        page += 1

    return records

# --------------------------------------------------------------------- #
# Fill logic (with ±N bucket fallback)
# --------------------------------------------------------------------- #
def _seed_donors_from_window(
    ts_key: str,
    field: str,
    grouped: Dict[str, List[int]],
    records: List[Dict],
    meta_by_index: Dict[int, Tuple[float, float]],
    sorted_keys: List[str],
    radius: int,
) -> List[Tuple[float, float, object]]:
    """
    Look outward ±radius buckets to collect donors for this field.
    Searches nearest buckets first, but collects from ALL buckets within radius
    to maximize donor availability.
    """
    donors: List[Tuple[float, float, object]] = []
    pos = bisect_left(sorted_keys, ts_key)

    # Collect all buckets within radius, ordered by distance from target
    buckets_to_check = []
    for offset in range(1, radius + 1):
        if pos - offset >= 0:
            buckets_to_check.append((offset, pos - offset))  # (distance, index)
        if pos + offset < len(sorted_keys):
            buckets_to_check.append((offset, pos + offset))

    # Sort by distance (nearest first)
    buckets_to_check.sort(key=lambda x: x[0])

    # Collect donors from all buckets within radius
    for _, bucket_idx in buckets_to_check:
        key = sorted_keys[bucket_idx]
        for idx in grouped.get(key, []):
            meta = meta_by_index.get(idx)
            if not meta:
                continue
            val = records[idx].get(field)
            if has_real_value(val):
                lat, lon = meta
                donors.append((lat, lon, val))

    return donors

def fill_from_neighbors_rowwise(
    records: List[Dict],
    beach_meta: Dict[str, Tuple[float, float]],
    fields: Iterable[str],
    verbose: bool = False,
    time_fallback: int = 0,
    cadence: str = "H",
):
    """
    Optimized row-by-row filling that processes each null individually.

    Key optimizations:
    1. Groups records by timestamp for efficient donor lookup
    2. Processes nulls in optimal order (closest to donors first)
    3. Newly filled values immediately become donors
    4. All work done in-memory, then batch-written to DB

    This guarantees 100% fill rate (if donors exist) while staying fast.
    """
    stats = {
        "total_records": len(records),
        "skipped_bad_timestamp": 0,
        "skipped_no_coords": 0,
        "field_no_donor": Counter(),
        "field_filled": Counter(),
        "examples": defaultdict(list),
        "buckets_built": 0,
    }

    if not records:
        return [], stats

    # Build index: timestamp -> list of record indices
    grouped: Dict[str, List[int]] = defaultdict(list)
    meta_by_index: Dict[int, Tuple[float, float]] = {}

    for idx, rec in enumerate(records):
        ts_key = normalize_timestamp(rec.get("timestamp"), cadence)
        if not ts_key:
            stats["skipped_bad_timestamp"] += 1
            continue

        bid = str(rec.get("beach_id")) if rec.get("beach_id") is not None else None
        if bid not in beach_meta:
            stats["skipped_no_coords"] += 1
            continue

        grouped[ts_key].append(idx)
        meta_by_index[idx] = beach_meta[bid]

    sorted_keys = sorted(grouped.keys())
    stats["buckets_built"] = len(sorted_keys)

    if verbose:
        logger.debug("Built %d time buckets (cadence=%s)", len(sorted_keys), cadence)

    # IMPORTANT: Process surf_height_max_ft before surf_height_min_ft
    # This allows us to check max when deciding whether to fill min
    fields_list = list(fields)
    if "surf_height_max_ft" in fields_list and "surf_height_min_ft" in fields_list:
        # Ensure max is processed before min
        max_idx = fields_list.index("surf_height_max_ft")
        min_idx = fields_list.index("surf_height_min_ft")
        if min_idx < max_idx:
            # Swap them so max comes first
            fields_list[max_idx], fields_list[min_idx] = fields_list[min_idx], fields_list[max_idx]
            if verbose:
                logger.debug("Reordered fields: surf_height_max_ft will be processed before surf_height_min_ft")

    fields = tuple(fields_list)
    changed: Dict[int, Set[str]] = defaultdict(set)

    # Process each field independently
    for field in fields:
        if verbose:
            logger.debug("Processing field: %s", field)

        # For each timestamp bucket
        for ts_key in sorted_keys:
            indices = grouped[ts_key]

            # Find donors and missing records at this timestamp
            donors: List[Tuple[float, float, object]] = []
            missing: List[Tuple[int, float, float]] = []

            for idx in indices:
                meta = meta_by_index.get(idx)
                if not meta:
                    continue

                lat, lon = meta
                val = records[idx].get(field)

                if has_real_value(val):
                    donors.append((lat, lon, val))
                else:
                    # Check if we should skip filling this field for this record
                    if not should_skip_filling(records[idx], field):
                        missing.append((idx, lat, lon))

            if not missing:
                continue

            # If no donors at this timestamp, look in nearby time buckets
            if not donors and time_fallback > 0:
                donors = _seed_donors_from_window(
                    ts_key, field, grouped, records, meta_by_index, sorted_keys, time_fallback
                )

            if not donors:
                stats["field_no_donor"][field] += len(missing)
                if verbose:
                    logger.debug("Bucket %s field %s: %d nulls, no donors available",
                               ts_key, field, len(missing))
                continue

            # OPTIMIZED BATCH FILLING: Fill all nulls at once, then allow filled values
            # to propagate in a second pass if needed
            if not missing:
                continue

            # PASS 1: Fill all missing values from existing donors
            filled_in_pass = []
            for idx, lat, lon in missing:
                # Find nearest donor value
                best_value = None
                best_distance = float("inf")
                for d_lat, d_lon, d_val in donors:
                    d = haversine_distance(lat, lon, d_lat, d_lon)
                    if d < best_distance:
                        best_distance = d
                        best_value = d_val

                if best_value is not None:
                    # Fill the value in-memory
                    records[idx][field] = best_value
                    changed[idx].add(field)
                    stats["field_filled"][field] += 1

                    if verbose and len(stats["examples"][field]) < 5:
                        stats["examples"][field].append({
                            "timestamp": ts_key,
                            "beach_id": records[idx].get("beach_id"),
                            "value": best_value,
                            "distance_km": best_distance,
                        })

                    # Track newly filled values for potential second pass
                    filled_in_pass.append((lat, lon, best_value))

            # PASS 2 (optional): Add newly filled values as donors and try again
            # This allows propagation without the expensive while loop
            if filled_in_pass:
                donors.extend(filled_in_pass)

                # Quick second pass for any remaining nulls
                remaining_nulls = [
                    (idx, lat, lon) for idx, lat, lon in missing
                    if not has_real_value(records[idx].get(field))
                ]

                for idx, lat, lon in remaining_nulls:
                    best_value = None
                    best_distance = float("inf")
                    # Only check newly filled donors (much smaller list)
                    for d_lat, d_lon, d_val in filled_in_pass:
                        d = haversine_distance(lat, lon, d_lat, d_lon)
                        if d < best_distance:
                            best_distance = d
                            best_value = d_val

                    if best_value is not None:
                        records[idx][field] = best_value
                        changed[idx].add(field)
                        stats["field_filled"][field] += 1

    if not changed:
        return [], stats

    # Prepare batch updates (same as before)
    updates: List[Dict] = []
    for record_idx, fields_changed in changed.items():
        rec = records[record_idx]
        payload = {
            "id": rec["id"],
            "beach_id": rec["beach_id"],
            "timestamp": rec["timestamp"],
        }
        for f in fields_changed:
            val = rec.get(f)
            if has_real_value(val):
                payload[f] = val

        if len(payload) > 3:
            updates.append(payload)

    return updates, stats

# --------------------------------------------------------------------- #
# Bulk upsert by id (with retry & null stripping)
# --------------------------------------------------------------------- #
import time
from typing import List, Dict

def upsert_updates(updates: List[Dict], dry_run: bool = False, batch_size: int = 100, verbose: bool = False):
    """
    Batched UPDATE by id using Supabase upsert with conflict resolution.
    Much faster than row-by-row updates.
    - updates: list of {"id": ..., "<field>": value, ...}
    - batch_size: number of rows to update per batch (default 100)
    """
    if dry_run:
        logger.info("Dry-run: skipping DB writes (%s rows)", len(updates))
        return

    if not updates:
        logger.info("Post-fill: no updates to apply")
        return

    # Remove null values and empty updates
    # CRITICAL: This ensures we never overwrite filled fields with nulls
    cleaned_updates = []
    fields_being_updated = set()

    for row in updates:
        # Strip out any None values to prevent overwriting filled data
        update_data = {k: v for k, v in row.items() if v is not None}

        # Track which fields we're updating for logging
        for k in update_data.keys():
            if k not in ('id', 'beach_id', 'timestamp'):
                fields_being_updated.add(k)

        if len(update_data) > 3:  # More than just id, beach_id, timestamp
            cleaned_updates.append(update_data)

    if not cleaned_updates:
        logger.info("Post-fill: no non-empty updates to apply")
        return

    if verbose:
        logger.info("Post-fill: updating fields: %s", sorted(fields_being_updated))

    total = 0
    attempts_max = 3

    # Process in batches
    for batch_num, batch in enumerate(chunk_iter(cleaned_updates, batch_size)):
        delay = 0.5
        for attempt in range(1, attempts_max + 1):
            try:
                # Use upsert with on_conflict to update existing rows
                # This is much faster than individual updates
                supabase.table("forecast_data").upsert(
                    batch,
                    on_conflict="id",  # Primary key conflict resolution
                    ignore_duplicates=False  # Actually update the rows
                ).execute()
                total += len(batch)
                logger.info("Post-fill: batch %d/%d completed (%d rows)",
                           batch_num + 1,
                           (len(cleaned_updates) + batch_size - 1) // batch_size,
                           len(batch))
                break
            except Exception as e:
                if attempt == attempts_max:
                    logger.error("Post-fill: batch %d failed after %d attempts: %s",
                               batch_num + 1, attempts_max, e)
                    raise
                logger.warning("Post-fill: batch %d attempt %d failed, retrying: %s",
                             batch_num + 1, attempt, e)
                time.sleep(delay)
                delay = min(delay * 2, 4.0)

        # Small pause between batches to avoid rate limits
        if batch_num % 10 == 9:
            time.sleep(0.2)

    logger.info("Post-fill: updated %s forecast rows with neighbor data", total)


# --------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> bool:
    args = build_arg_parser().parse_args(argv)
    if args.verbose:
        logger.setLevel("DEBUG")

    fields = tuple(args.fields) if args.fields else FIELDS_FOR_NEIGHBOR_FILL

    logger.info("=" * 80)
    logger.info("NEIGHBOR FILL - AUTO-DETECTS ALL COLUMNS")
    logger.info("=" * 80)
    logger.info(f"Processing {len(fields)} fields: {', '.join(fields)}")
    logger.info("=" * 80)

    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("Post-fill: no beaches available; aborting")
        return False

    # normalize beach IDs to str; skip missing coords
    beach_meta: Dict[str, Tuple[float, float]] = {}
    for b in beaches:
        bid = str(b.get("id"))
        lat, lon = b.get("LATITUDE"), b.get("LONGITUDE")
        if lat is None or lon is None:
            continue
        beach_meta[bid] = (float(lat), float(lon))

    if not beach_meta:
        logger.error("Post-fill: no beaches with coordinates; aborting")
        return False

    if args.start_iso:
        start_iso = args.start_iso
    elif args.hours_back is not None:
        # Start from N hours ago
        tz = pytz.timezone("America/Los_Angeles")
        start_dt = datetime.now(tz) - timedelta(hours=args.hours_back)
        start_iso = start_dt.isoformat()
    else:
        # No time limit - process all records
        # Use a very old date to effectively get everything
        start_iso = "1970-01-01T00:00:00"

    if start_iso == "1970-01-01T00:00:00":
        logger.info("Post-fill: starting neighbor backfill for ALL records (cadence=%s, fallback=%d)",
                    args.cadence, args.time_fallback)
    else:
        logger.info("Post-fill: starting neighbor backfill from %s (cadence=%s, fallback=%d)",
                    start_iso, args.cadence, args.time_fallback)

    # Fetch all records once
    records = fetch_recent_forecast_records(
        start_iso=start_iso, fields=fields, page_size=args.page_size,
        limit=args.limit, verbose=args.verbose,
    )
    logger.info("Post-fill: evaluating %s forecast rows", len(records))

    # Count nulls before filling
    if args.verbose:
        nulls_before = {}
        for field in fields:
            nulls_before[field] = sum(1 for r in records if r.get(field) is None)
        logger.info("Post-fill: nulls before filling -> %s",
                   ", ".join(f"{k}: {v}" for k, v in nulls_before.items()))

    # Single-pass row-by-row filling (optimized)
    updates, stats = fill_from_neighbors_rowwise(
        records, beach_meta, fields,
        verbose=args.verbose, time_fallback=args.time_fallback, cadence=args.cadence
    )

    logger.info(
        "Post-fill: %s records | buckets=%s | skipped bad_ts=%s no_coords=%s",
        stats.get("total_records", 0),
        stats.get("buckets_built", 0),
        stats.get("skipped_bad_timestamp", 0),
        stats.get("skipped_no_coords", 0),
    )

    ff = stats.get("field_filled", Counter())
    if ff:
        logger.info("Post-fill: field fills -> %s", ", ".join(f"{k}: {v}" for k, v in ff.items()))

    nd = stats.get("field_no_donor", Counter())
    if nd:
        logger.info("Post-fill: no-donor counts -> %s", ", ".join(f"{k}: {v}" for k, v in nd.items()))

    # Count nulls after filling (in-memory check)
    if args.verbose:
        nulls_after = {}
        for field in fields:
            nulls_after[field] = sum(1 for r in records if r.get(field) is None)
        logger.info("Post-fill: nulls after filling -> %s",
                   ", ".join(f"{k}: {v}" for k, v in nulls_after.items()))

        # Show improvement
        for field in fields:
            if field in nulls_before:
                improvement = nulls_before[field] - nulls_after[field]
                if improvement > 0:
                    logger.info("Post-fill: %s improved by %d (%.1f%%)",
                              field, improvement,
                              100.0 * improvement / nulls_before[field] if nulls_before[field] > 0 else 0)

    if not updates:
        logger.info("Post-fill: no fields required neighbor fills")
        return True

    # Write all updates in batches
    upsert_updates(updates, dry_run=args.dry_run, batch_size=args.batch_size, verbose=args.verbose)
    logger.info("Post-fill: completed neighbor backfill")
    return True


if __name__ == "__main__":
    success = main()
    raise SystemExit(0 if success else 1)
