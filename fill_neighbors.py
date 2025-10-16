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
# Configurable fields to fill (add primary_* if desired)
# --------------------------------------------------------------------- #
FIELDS_FOR_NEIGHBOR_FILL: Tuple[str, ...] = (
    "weather",
    "wind_direction_deg",
    "secondary_swell_height_ft",
    "secondary_swell_period_s",
    "secondary_swell_direction",
    "tertiary_swell_height_ft",
    "tertiary_swell_period_s",
    "tertiary_swell_direction",
    # "primary_swell_height_ft",
    # "primary_swell_period_s",
    # "primary_swell_direction",
)

# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fill missing forecast fields from nearest beaches")
    p.add_argument("--start-iso", help="ISO timestamp (inclusive) to start from. Default: today 00:00 PT.")
    p.add_argument("--hours-back", type=int, default=0,
                   help="If no --start-iso, shift Pacific midnight back by this many hours.")
    p.add_argument("--fields", nargs="+", choices=FIELDS_FOR_NEIGHBOR_FILL,
                   help="Override the list of fields to backfill.")
    p.add_argument("--page-size", type=int, default=1000)
    p.add_argument("--batch-size", type=int, default=100,
                   help="Number of rows to update per batch (default: 100, max recommended: 500)")
    p.add_argument("--limit", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--per-field", action="store_true",
                   help="Process each field in its own pass.")
    p.add_argument("--time-fallback", type=int, default=0,
                   help="If a field has no donors at a bucket, allow fallback ±N buckets (0=disabled).")
    p.add_argument("--cadence", default="H",
                   help="Bucketing cadence for timestamps (default 'H'). Examples: '30min','15min'.")
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

def normalize_timestamp(value: object, freq: str = "H") -> Optional[str]:
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
    """Look outward ±radius buckets to collect donors for this field."""
    donors: List[Tuple[float, float, object]] = []
    pos = bisect_left(sorted_keys, ts_key)
    lo, hi = pos - 1, pos + 1
    steps = 0
    while (lo >= 0 or hi < len(sorted_keys)) and steps < radius * 2:
        for cand in (lo, hi):
            if cand < 0 or cand >= len(sorted_keys):
                continue
            key = sorted_keys[cand]
            for idx in grouped.get(key, []):
                meta = meta_by_index.get(idx)
                if not meta:
                    continue
                val = records[idx].get(field)
                if has_real_value(val):
                    lat, lon = meta
                    donors.append((lat, lon, val))
        lo -= 1
        hi += 1
        steps += 2
        if donors:
            break  # first found window seeds this bucket
    return donors

def fill_from_neighbors(
    records: List[Dict],
    beach_meta: Dict[str, Tuple[float, float]],
    fields: Iterable[str],
    verbose: bool = False,
    time_fallback: int = 0,
    cadence: str = "H",
):
    stats = {
        "total_records": len(records),
        "skipped_no_timestamp": 0,   # kept for symmetry; we floor all
        "skipped_bad_timestamp": 0,
        "skipped_no_coords": 0,
        "field_no_donor": Counter(),
        "field_filled": Counter(),
        "examples": defaultdict(list),
        "buckets_built": 0,
    }

    if not records:
        return [], stats

    grouped: Dict[str, List[int]] = defaultdict(list)
    meta_by_index: Dict[int, Tuple[float, float]] = {}

    for idx, rec in enumerate(records):
        ts_key = normalize_timestamp(rec.get("timestamp"), cadence)
        if not ts_key:
            stats["skipped_bad_timestamp"] += 1
            continue

        bid = rec.get("beach_id")
        bid = str(bid) if bid is not None else None
        if bid not in beach_meta:
            stats["skipped_no_coords"] += 1
            continue

        grouped[ts_key].append(idx)
        meta_by_index[idx] = beach_meta[bid]

    sorted_keys = sorted(grouped.keys())
    stats["buckets_built"] = len(sorted_keys)

    if verbose:
        logger.debug("Built %d time buckets (cadence=%s)", len(sorted_keys), cadence)

    fields = tuple(fields)
    changed: Dict[int, Set[str]] = defaultdict(set)

    for ts_key, indices in grouped.items():
        per_record_meta: List[Optional[Tuple[float, float]]] = [meta_by_index.get(i) for i in indices]

        for field in fields:
            donors: List[Tuple[float, float, object]] = []
            missing: List[Tuple[int, float, float]] = []

            # split donors vs missing at this bucket
            for local_pos, record_idx in enumerate(indices):
                meta = per_record_meta[local_pos]
                if meta is None:
                    continue
                lat, lon = meta
                val = records[record_idx].get(field)
                if has_real_value(val):
                    donors.append((lat, lon, val))
                else:
                    missing.append((record_idx, lat, lon))

            if not missing:
                continue

            # if no donors here, try ±N bucket fallback
            if not donors and time_fallback > 0:
                donors = _seed_donors_from_window(
                    ts_key, field, grouped, records, meta_by_index, sorted_keys, time_fallback
                )

            if not donors:
                stats["field_no_donor"][field] += len(missing)
                if verbose:
                    logger.debug("Bucket %s field %s has %d targets but no donors", ts_key, field, len(missing))
                continue

            # nearest-neighbor copy within the bucket
            for record_idx, lat, lon in missing:
                best_value, best_distance = None, float("inf")
                for d_lat, d_lon, d_val in donors:
                    d = haversine_distance(lat, lon, d_lat, d_lon)
                    if d < best_distance:
                        best_distance, best_value = d, d_val
                if best_value is not None:
                    records[record_idx][field] = best_value
                    changed[record_idx].add(field)
                    stats["field_filled"][field] += 1
                    if verbose and len(stats["examples"][field]) < 5:
                        stats["examples"][field].append({
                            "timestamp": ts_key,
                            "beach_id": records[record_idx].get("beach_id"),
                            "value": best_value,
                            "distance_km": best_distance,
                        })
                    # new value can seed additional nearest-neighbor matches
                    donors.append((lat, lon, best_value))

    if not changed:
        return [], stats

    # prepare update payloads; include NOT NULL cols to satisfy PostgREST
    updates: List[Dict] = []
    for record_idx, fields_changed in changed.items():
        rec = records[record_idx]
        payload = {
            "id": rec["id"],                    # primary key (conflict target)
            "beach_id": rec["beach_id"],        # NOT NULL in table
            "timestamp": rec["timestamp"],      # NOT NULL in table
        }
        for f in fields_changed:
            val = rec.get(f)
            if has_real_value(val):
                payload[f] = val
        updates.append(payload)

    return updates, stats

# --------------------------------------------------------------------- #
# Bulk upsert by id (with retry & null stripping)
# --------------------------------------------------------------------- #
import time
from typing import List, Dict

def upsert_updates(updates: List[Dict], dry_run: bool = False, batch_size: int = 100):
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
    cleaned_updates = []
    for row in updates:
        update_data = {k: v for k, v in row.items() if v is not None}
        if len(update_data) > 3:  # More than just id, beach_id, timestamp
            cleaned_updates.append(update_data)

    if not cleaned_updates:
        logger.info("Post-fill: no non-empty updates to apply")
        return

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
    else:
        start_dt = pacific_midnight_today()
        if args.hours_back:
            start_dt -= timedelta(hours=args.hours_back)
        start_iso = start_dt.isoformat()

    logger.info("Post-fill: starting neighbor backfill from %s (cadence=%s, fallback=%d)",
                start_iso, args.cadence, args.time_fallback)

    records = fetch_recent_forecast_records(
        start_iso=start_iso, fields=fields, page_size=args.page_size,
        limit=args.limit, verbose=args.verbose,
    )
    logger.info("Post-fill: evaluating %s forecast rows", len(records))

    if args.per_field:
        aggregate_updates: Dict[Tuple[str, str], Dict] = {}
        aggregate_stats: Dict = {}
        for field in fields:
            field_updates, field_stats = fill_from_neighbors(
                records, beach_meta, (field,),
                verbose=args.verbose, time_fallback=args.time_fallback, cadence=args.cadence
            )
            if field_updates:
                for u in field_updates:
                    key = (u['beach_id'], u['timestamp'])
                    entry = aggregate_updates.setdefault(
                        key, {'id': u['id'], 'beach_id': u['beach_id'], 'timestamp': u['timestamp']}
                    )
                    for k, v in u.items():
                        if k not in ('id', 'beach_id', 'timestamp'):
                            entry[k] = v
            # merge stats
            for k, v in field_stats.items():
                if isinstance(v, Counter):
                    aggregate_stats.setdefault(k, Counter()).update(v)
                elif isinstance(v, dict):
                    agg = aggregate_stats.setdefault(k, defaultdict(list))
                    for sk, sv in v.items():
                        agg[sk].extend(sv)
                else:
                    aggregate_stats[k] = aggregate_stats.get(k, 0) + v

        updates, stats = list(aggregate_updates.values()), aggregate_stats
    else:
        updates, stats = fill_from_neighbors(
            records, beach_meta, fields,
            verbose=args.verbose, time_fallback=args.time_fallback, cadence=args.cadence
        )

    logger.info(
        "Post-fill: %s records | buckets=%s | skipped ts=%s bad_ts=%s no_coords=%s",
        stats.get("total_records", 0),
        stats.get("buckets_built", 0),
        stats.get("skipped_no_timestamp", 0),
        stats.get("skipped_bad_timestamp", 0),
        stats.get("skipped_no_coords", 0),
    )
    ff = stats.get("field_filled", Counter())
    if ff:
        logger.info("Post-fill: field fills -> %s", ", ".join(f"{k}: {v}" for k, v in ff.items()))
    nd = stats.get("field_no_donor", Counter())
    if nd:
        logger.info("Post-fill: no-donor counts -> %s", ", ".join(f"{k}: {v}" for k, v in nd.items()))

    if not updates:
        logger.info("Post-fill: no fields required neighbor fills")
        return True

    upsert_updates(updates, dry_run=args.dry_run, batch_size=args.batch_size)
    logger.info("Post-fill: completed neighbor backfill")
    return True


if __name__ == "__main__":
    success = main()
    raise SystemExit(0 if success else 1)
