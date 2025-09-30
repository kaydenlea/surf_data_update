#!/usr/bin/env python3
"""
Neighbor fill post-processor for forecast_data.
After the daily/nowcast import completes, copy missing values from the nearest
beach (same timestamp) so any remaining nulls inherit the closest real reading.
"""

import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Set

import pytz

from config import logger, UPSERT_CHUNK
from database import supabase, fetch_all_beaches
from utils import chunk_iter

FIELDS_FOR_NEIGHBOR_FILL = (
    "weather",
    "wind_direction_deg",
    "secondary_swell_height_ft",
    "secondary_swell_period_s",
    "secondary_swell_direction",
    "tertiary_swell_height_ft",
    "tertiary_swell_period_s",
    "tertiary_swell_direction",
)


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
    earth_radius_km = 6371.0
    return earth_radius_km * c


def pacific_midnight_today() -> datetime:
    tz = pytz.timezone("America/Los_Angeles")
    now = datetime.now(tz)
    return tz.localize(datetime.combine(now.date(), datetime.min.time()))


def fetch_recent_forecast_records(start_iso: str, page_size: int = 1000) -> List[Dict]:
    """Fetch forecast_data rows on/after the given ISO timestamp."""
    records: List[Dict] = []
    page = 0

    while True:
        start = page * page_size
        end = start + page_size - 1
        resp = (
            supabase
            .table("forecast_data")
            .select("beach_id,timestamp,weather,wind_direction_deg,secondary_swell_height_ft,"
                    "secondary_swell_period_s,secondary_swell_direction,"
                    "tertiary_swell_height_ft,tertiary_swell_period_s,tertiary_swell_direction")
            .gte("timestamp", start_iso)
            .range(start, end)
            .execute()
        )
        batch = resp.data or []
        records.extend(batch)
        if len(batch) < page_size:
            break
        page += 1

    return records


def fill_from_neighbors(records: List[Dict], beach_meta: Dict[int, Tuple[float, float]]):
    """Fill configured fields using nearest neighbor values for each timestamp."""
    if not records:
        return []

    # Group records by timestamp string (stored as ISO strings in Supabase)
    grouped: Dict[str, List[int]] = defaultdict(list)
    for idx, rec in enumerate(records):
        ts = rec.get("timestamp")
        bid = rec.get("beach_id")
        if not ts or bid not in beach_meta:
            continue
        grouped[ts].append(idx)

    changed: Dict[int, Set[str]] = defaultdict(set)

    for ts, indices in grouped.items():
        # Precompute metadata per record index for this timestamp
        per_record_meta: List[Tuple[float, float] or None] = []
        for idx in indices:
            beach_id = records[idx].get("beach_id")
            lat_lon = beach_meta.get(beach_id)
            if not lat_lon:
                per_record_meta.append(None)
            else:
                per_record_meta.append(lat_lon)

        for field in FIELDS_FOR_NEIGHBOR_FILL:
            available = []  # (lat, lon, value)
            missing = []    # (record_idx, lat, lon)

            for local_pos, record_idx in enumerate(indices):
                meta = per_record_meta[local_pos]
                if meta is None:
                    continue
                lat, lon = meta
                value = records[record_idx].get(field)
                if value is not None:
                    available.append((lat, lon, value))
                else:
                    missing.append((record_idx, lat, lon))

            if not available or not missing:
                continue

            for record_idx, lat, lon in missing:
                best_value = None
                best_distance = float("inf")
                for av_lat, av_lon, value in available:
                    distance = haversine_distance(lat, lon, av_lat, av_lon)
                    if distance < best_distance:
                        best_distance = distance
                        best_value = value
                if best_value is not None:
                    records[record_idx][field] = best_value
                    changed[record_idx].add(field)

    if not changed:
        return []

    updates = []
    for record_idx, fields in changed.items():
        record = records[record_idx]
        payload = {"beach_id": record["beach_id"], "timestamp": record["timestamp"]}
        for field in fields:
            payload[field] = record.get(field)
        updates.append(payload)

    return updates


def upsert_updates(updates: List[Dict]):
    total = 0
    for chunk in chunk_iter(updates, UPSERT_CHUNK):
        supabase.table("forecast_data").upsert(
            chunk,
            on_conflict="beach_id,timestamp"
        ).execute()
        total += len(chunk)
    logger.info(f"Post-fill: upserted {total} forecast rows with neighbor data")


def main():
    logger.info("Post-fill: starting neighbor backfill")

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

    start_dt = pacific_midnight_today()
    start_iso = start_dt.isoformat()
    records = fetch_recent_forecast_records(start_iso)
    logger.info(f"Post-fill: evaluating {len(records)} forecast rows on/after {start_iso}")

    updates = fill_from_neighbors(records, beach_meta)
    if not updates:
        logger.info("Post-fill: no fields required neighbor fills")
        return True

    upsert_updates(updates)
    logger.info("Post-fill: completed neighbor backfill")
    return True


if __name__ == "__main__":
    ok = main()
    raise SystemExit(0 if ok else 1)
