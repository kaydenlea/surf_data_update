#!/usr/bin/env python3
"""
Neighbor fill post-processor for `forecast_data`.
After the daily/nowcast import completes, copy missing values from the nearest
beach (same timestamp) so any remaining nulls inherit the closest real reading.

This script can also be run standalone for ad-hoc debugging. Use
`python fill_neighbors.py --help` for options.
"""

from __future__ import annotations

import argparse
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import pytz

from config import logger, UPSERT_CHUNK
from database import fetch_all_beaches, supabase
from utils import chunk_iter

FIELDS_FOR_NEIGHBOR_FILL: Tuple[str, ...] = (
    "weather",
    "wind_direction_deg",
    "secondary_swell_height_ft",
    "secondary_swell_period_s",
    "secondary_swell_direction",
    "tertiary_swell_height_ft",
    "tertiary_swell_period_s",
    "tertiary_swell_direction",
)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fill missing forecast fields from nearest beaches")
    parser.add_argument(
        "--start-iso",
        help="ISO timestamp (inclusive) to start processing from. Defaults to today's Pacific midnight.",
    )
    parser.add_argument(
        "--hours-back",
        type=int,
        default=0,
        help="When no start ISO is provided, shift the start window back by this many hours (default 0).",
    )
    parser.add_argument(
        "--fields",
        nargs="+",
        choices=FIELDS_FOR_NEIGHBOR_FILL,
        help="Override the list of fields to backfill. Defaults to all supported fields.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Batch size when fetching rows from Supabase (default 1000).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap on total rows fetched (useful for debugging).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute fills but do not write them back to Supabase.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit detailed diagnostics about skipped rows and fills.",
    )
    return parser


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance between two lat/lon points (kilometres)."""
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
    c = 2 * math.asin(math.sqrt(max(0.0, min(1.0, a))))
    return 6371.0 * c  # Earth radius in km


def pacific_midnight_today(now: Optional[datetime] = None) -> datetime:
    tz = pytz.timezone("America/Los_Angeles")
    now = now.astimezone(tz) if now and now.tzinfo else now or datetime.now(tz)
    return tz.localize(datetime.combine(now.date(), datetime.min.time()))


def normalize_timestamp(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        return pd.Timestamp(value).isoformat()
    except Exception:
        return None


def fetch_recent_forecast_records(
    start_iso: str,
    fields: Iterable[str],
    page_size: int = 1000,
    limit: Optional[int] = None,
    verbose: bool = False,
) -> List[Dict]:
    select_fields = ["beach_id", "timestamp", *fields]
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


def fill_from_neighbors(
    records: List[Dict],
    beach_meta: Dict[int, Tuple[float, float]],
    fields: Iterable[str],
    verbose: bool = False,
):
    stats = {
        "total_records": len(records),
        "skipped_no_timestamp": 0,
        "skipped_bad_timestamp": 0,
        "skipped_no_coords": 0,
        "field_no_donor": Counter(),
        "field_filled": Counter(),
        "examples": defaultdict(list),
    }

    if not records:
        return [], stats

    grouped: Dict[str, List[int]] = defaultdict(list)
    for idx, rec in enumerate(records):
        ts = rec.get("timestamp")
        if not ts:
            stats["skipped_no_timestamp"] += 1
            if verbose:
                logger.debug("Skipping index %s (beach %s) - missing timestamp", idx, rec.get("beach_id"))
            continue

        normalized_ts = normalize_timestamp(ts)
        if not normalized_ts:
            stats["skipped_bad_timestamp"] += 1
            if verbose:
                logger.debug("Skipping index %s (beach %s) - unparseable timestamp %r", idx, rec.get("beach_id"), ts)
            continue

        bid = rec.get("beach_id")
        if bid not in beach_meta:
            stats["skipped_no_coords"] += 1
            if verbose:
                logger.debug("Skipping index %s (beach %s) - no coordinates", idx, bid)
            continue

        grouped[normalized_ts].append(idx)

    fields = tuple(fields)
    changed: Dict[int, Set[str]] = defaultdict(set)

    for ts, indices in grouped.items():
        per_record_meta: List[Optional[Tuple[float, float]]] = []
        for idx in indices:
            beach_id = records[idx].get("beach_id")
            per_record_meta.append(beach_meta.get(beach_id))

        for field in fields:
            donors: List[Tuple[float, float, object]] = []
            missing: List[Tuple[int, float, float]] = []

            for local_pos, record_idx in enumerate(indices):
                meta = per_record_meta[local_pos]
                if meta is None:
                    continue
                lat, lon = meta
                value = records[record_idx].get(field)
                if value is not None:
                    donors.append((lat, lon, value))
                else:
                    missing.append((record_idx, lat, lon))

            if not missing:
                continue

            if not donors:
                stats["field_no_donor"][field] += len(missing)
                if verbose:
                    logger.debug(
                        "Timestamp %s field %s had %s targets but no donors",
                        ts,
                        field,
                        len(missing),
                    )
                continue

            for record_idx, lat, lon in missing:
                best_value = None
                best_distance = float("inf")
                for d_lat, d_lon, d_val in donors:
                    distance = haversine_distance(lat, lon, d_lat, d_lon)
                    if distance < best_distance:
                        best_distance = distance
                        best_value = d_val
                if best_value is not None:
                    records[record_idx][field] = best_value
                    changed[record_idx].add(field)
                    stats["field_filled"][field] += 1
                    if verbose and len(stats["examples"][field]) < 5:
                        stats["examples"][field].append({
                            "timestamp": ts,
                            "beach_id": records[record_idx].get("beach_id"),
                            "value": best_value,
                            "distance_km": best_distance,
                        })
                    donors.append((lat, lon, best_value))

    if not changed:
        return [], stats

    updates = []
    for record_idx, fields_changed in changed.items():
        record = records[record_idx]
        payload = {"beach_id": record["beach_id"], "timestamp": record["timestamp"]}
        for field in fields_changed:
            payload[field] = record.get(field)
        updates.append(payload)

    return updates, stats


def upsert_updates(updates: List[Dict], dry_run: bool = False):
    if dry_run:
        logger.info("Post-fill: dry-run requested - skipping Supabase writes (%s rows)", len(updates))
        return

    total = 0
    for chunk in chunk_iter(updates, UPSERT_CHUNK):
        supabase.table("forecast_data").upsert(
            chunk,
            on_conflict="beach_id,timestamp"
        ).execute()
        total += len(chunk)
    logger.info("Post-fill: upserted %s forecast rows with neighbor data", total)


def summarize_stats(stats: Dict, verbose: bool = False):
    logger.info(
        "Post-fill: processed %s records (skipped %s missing timestamp, %s unparsable timestamp, %s missing coords)",
        stats.get("total_records", 0),
        stats.get("skipped_no_timestamp", 0),
        stats.get("skipped_bad_timestamp", 0),
        stats.get("skipped_no_coords", 0),
    )

    field_filled = stats.get("field_filled", Counter())
    if field_filled:
        logger.info(
            "Post-fill: field fills -> %s",
            ", ".join(f"{k}: {v}" for k, v in field_filled.items()),
        )

    field_no_donor = stats.get("field_no_donor", Counter())
    if field_no_donor:
        logger.info(
            "Post-fill: no-donor counts -> %s",
            ", ".join(f"{k}: {v}" for k, v in field_no_donor.items()),
        )

    if verbose:
        examples = stats.get("examples", {})
        for field, samples in examples.items():
            for sample in samples:
                logger.debug(
                    "Example fill (%s): beach=%s ts=%s value=%s (distance %.2f km)",
                    field,
                    sample.get("beach_id"),
                    sample.get("timestamp"),
                    sample.get("value"),
                    sample.get("distance_km"),
                )


def main(argv: Optional[List[str]] = None) -> bool:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.verbose:
        logger.setLevel("DEBUG")

    fields = tuple(args.fields) if args.fields else FIELDS_FOR_NEIGHBOR_FILL

    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("Post-fill: no beaches available; aborting")
        return False

    beach_meta = {}
    for beach in beaches:
        lat = beach.get("LATITUDE")
        lon = beach.get("LONGITUDE")
        if lat is None or lon is None:
            continue
        beach_meta[beach["id"]] = (lat, lon)

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

    logger.info("Post-fill: starting neighbor backfill from %s", start_iso)

    records = fetch_recent_forecast_records(
        start_iso=start_iso,
        fields=fields,
        page_size=args.page_size,
        limit=args.limit,
        verbose=args.verbose,
    )
    logger.info("Post-fill: evaluating %s forecast rows", len(records))

    updates, stats = fill_from_neighbors(records, beach_meta, fields, verbose=args.verbose)

    if not updates:
        logger.info("Post-fill: no fields required neighbor fills")
        summarize_stats(stats, verbose=args.verbose)
        return True

    summarize_stats(stats, verbose=args.verbose)
    upsert_updates(updates, dry_run=args.dry_run)
    logger.info("Post-fill: completed neighbor backfill")
    return True


if __name__ == "__main__":
    success = main()
    raise SystemExit(0 if success else 1)
