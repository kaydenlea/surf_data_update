#!/usr/bin/env python3
"""
Hourly tide updater for Waves & Waders.

Pulls sea level (tide) from Open‑Meteo Marine API per beach and upserts to
`beach_tides_hourly` starting at 12:00 AM of the current Pacific day for
`DAYS_FORECAST` days. Applies +TIDE_ADJUSTMENT_FT to tide_level_ft.
"""

import time
from datetime import datetime, timedelta
import pandas as pd

import openmeteo_requests
import requests_cache
from retry_requests import retry

from config import (
    logger, DAYS_FORECAST, OPENMETEO_MARINE_URL,
    OPENMETEO_REQUEST_DELAY, OPENMETEO_RETRY_DELAY, OPENMETEO_MAX_RETRIES,
    TIDE_ADJUSTMENT_FT
)
from utils import (
    chunk_iter, safe_float, meters_to_feet, api_request_with_retry
)
from database import (
    fetch_all_beaches, upsert_tide_data, delete_all_tide_data,
    delete_tide_data_before
)

import pytz

# Reuse Open‑Meteo client with caching and retry
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=3, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def pacific_midnight_today(now=None):
    tz = pytz.timezone('America/Los_Angeles')
    now = now or datetime.now(tz)
    if now.tzinfo is None:
        now = tz.localize(now)
    else:
        now = now.astimezone(tz)
    today = now.date()
    return tz.localize(datetime.combine(today, datetime.min.time()))

def derive_date_range(days=DAYS_FORECAST, midnight=None):
    base_start = midnight or pacific_midnight_today()
    if days < 1:
        raise ValueError('days must be >= 1 for tide date range')
    end = base_start + timedelta(days=days - 1)
    # Open-Meteo uses date strings inclusive
    return base_start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def update_tides_for_beaches(beaches, day_start):
    if not beaches:
        logger.error("No beaches provided for tide update")
        return 0

    start_date, end_date = derive_date_range(midnight=day_start)
    logger.info(f"TIDES: Fetching hourly tides from {start_date} 00:00 PT through {end_date} 23:00 PT")

    total = 0
    for batch in chunk_iter(beaches, 25):
        lats = [b["LATITUDE"] for b in batch]
        lons = [b["LONGITUDE"] for b in batch]

        params = {
            "latitude": lats,
            "longitude": lons,
            "hourly": ["sea_level_height_msl"],
            "timezone": "America/Los_Angeles",
            "start_date": start_date,
            "end_date": end_date
        }

        try:
            mrs = api_request_with_retry(openmeteo.weather_api, OPENMETEO_MARINE_URL, params=params,
                                         max_retries=OPENMETEO_MAX_RETRIES)
        except Exception as e:
            logger.error(f"TIDES: Marine API batch failed: {e}")
            continue

        # Build records
        to_upsert = []
        for i, beach in enumerate(batch):
            try:
                mh = mrs[i].Hourly()
                # Times are in UTC epoch but reflect local tz when timezone parameter is set
                timestamps = pd.to_datetime(
                    range(mh.Time(), mh.TimeEnd(), mh.Interval()), unit="s", utc=True
                ).tz_convert("America/Los_Angeles")

                tide_level_m = mh.Variables(0).ValuesAsNumpy()

                for j, ts_local in enumerate(timestamps):
                    # Only keep timestamps on/after today's midnight (defensive)
                    if ts_local < day_start:
                        continue

                    raw_m = safe_float(tide_level_m[j])
                    raw_ft = safe_float(meters_to_feet(raw_m)) if raw_m is not None else None
                    adjusted_ft = (raw_ft + TIDE_ADJUSTMENT_FT) if raw_ft is not None else None

                    to_upsert.append({
                        "beach_id": beach["id"],
                        "timestamp": pd.Timestamp(ts_local).isoformat(),
                        "tide_level_m": raw_m,
                        "tide_level_ft": adjusted_ft,
                    })
            except Exception as e:
                logger.error(f"TIDES: Failed for beach {beach.get('Name','?')} ({beach['id']}): {e}")

        if to_upsert:
            inserted = upsert_tide_data(to_upsert)
            total += inserted

        time.sleep(OPENMETEO_REQUEST_DELAY)

    logger.info(f"TIDES: Upserted {total} tide rows across {len(beaches)} beaches")
    return total

def main():
    beaches = fetch_all_beaches()
    if not beaches:
        logger.error("TIDES: No beaches found, aborting")
        return False
    # Optional delete can be controlled via env; default removes rows before today's midnight
    import os
    day_start = pacific_midnight_today()
    tide_delete_mode = os.environ.get("TIDE_DELETE", "outdated")
    if tide_delete_mode == "all":
        delete_all_tide_data()
    else:
        delete_tide_data_before(day_start)
    _ = update_tides_for_beaches(beaches, day_start)
    return True

if __name__ == "__main__":
    ok = main()
    raise SystemExit(0 if ok else 1)
