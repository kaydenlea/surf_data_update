#!/usr/bin/env python3
"""
CDIP Nowcast Data Updater (Grid Version)
Replaces grid-based forecast data with current CDIP nowcast conditions.
Behaves the same as nowcast.py but targets grid_forecast_data instead of per-beach data.
"""

import sys
import xarray as xr
import numpy as np
import pandas as pd
import pytz
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Set

# Import shared configuration and utilities
try:
    from config import SUPABASE_URL, SUPABASE_KEY, logger, UPSERT_CHUNK
    from utils import chunk_iter, safe_float, normalize_surf_range
    from database import supabase  # Shared Supabase client
except ImportError:
    print("ERROR: Could not import required modules - ensure config.py, utils.py, and database.py are available")
    sys.exit(1)

from swell_ranking import get_surf_height_range, calculate_wave_energy_kj
from noaa_grid_handler import fetch_grid_points_from_db

# Configuration
CDIP_NOWCAST_URLS = {
    'socal': "http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/socal_alongshore_nowcast.nc",
    'norcal': "http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/norcal_alongshore_nowcast.nc"
}

# Constants
M_TO_FT = 3.28084
PACIFIC_TZ = pytz.timezone('America/Los_Angeles')
GRID_FORECAST_TABLE = "grid_forecast_data"
GRID_DAILY_TABLE = "daily_grid_surf_intensity"
GRID_REFRESH_FUNCTION = "refresh_daily_grid_surf_intensity"
GRID_ID_FIELD = "grid_id"


def _normalize_longitude(lon: float) -> float:
    """Convert NOAA 0-360 style longitudes to -180 to 180 range."""
    try:
        lon_val = float(lon)
    except (TypeError, ValueError):
        return lon
    return lon_val - 360.0 if lon_val > 180.0 else lon_val


def load_cdip_nowcast_dataset(url: str, region_name: str) -> Optional[Dict]:
    """Load a single CDIP nowcast dataset."""
    try:
        logger.info(f"Loading CDIP {region_name} nowcast from {url}")
        ds = xr.open_dataset(url)

        # Variable names for CDIP nowcast
        time_name = "waveTime"
        lat_name = "metaLatitude"
        lon_name = "metaLongitude"
        hs_name = "waveHs"    # significant wave height (m)
        tp_name = "waveTp"    # peak period (s)
        dp_name = "waveDp"    # peak direction (degrees from)

        # Convert times to Pacific timezone
        t_utc = pd.to_datetime(ds[time_name].values)
        t_local = (
            pd.Series(t_utc)
            .dt.tz_localize("UTC")
            .dt.tz_convert("America/Los_Angeles")
        )

        # Get site metadata
        lats = ds[lat_name].values
        lons = ds[lon_name].values

        # Extract wave data - shape: (time, sites)
        hs_m = ds[hs_name].values
        tp_s = ds[tp_name].values
        dp_deg = ds[dp_name].values

        # Extract spectral energy density if available
        wave_energy_density = None
        wave_frequencies = None
        try:
            if "waveEnergyDensity" in ds.variables:
                wave_energy_density = ds["waveEnergyDensity"].values  # (time, frequency, sites)
                wave_frequencies = ds["waveFrequency"].values  # Hz
                logger.info(f"CDIP {region_name}: Found spectral energy density data")
        except Exception as e:
            logger.warning(f"CDIP {region_name}: Could not load energy density: {e}")

        ds.close()

        dataset_info = {
            'region': region_name,
            'times': t_local,
            'lats': lats,
            'lons': lons,
            'hs_m': hs_m,
            'tp_s': tp_s,
            'dp_deg': dp_deg,
            'wave_energy_density': wave_energy_density,
            'wave_frequencies': wave_frequencies
        }

        logger.info(f"CDIP {region_name} nowcast loaded: {len(t_local)} timesteps, {len(lats)} sites")
        return dataset_info

    except Exception as e:
        logger.error(f"Failed to load CDIP {region_name} nowcast: {e}")
        return None


def combine_cdip_datasets(datasets: List[Dict]) -> Optional[Dict]:
    """Combine multiple CDIP datasets into one."""
    if not datasets:
        return None

    logger.info(f"Combining {len(datasets)} CDIP nowcast datasets...")

    reference_times = datasets[0]['times']

    combined_data = {
        'times': reference_times,
        'lats': [],
        'lons': [],
        'regions': [],
        'hs_m': None,
        'tp_s': None,
        'dp_deg': None,
        'wave_energy_density': None,
        'wave_frequencies': None
    }

    for dataset in datasets:
        if not dataset['times'].equals(reference_times):
            logger.warning(f"Time mismatch in {dataset['region']} dataset - skipping")
            continue

        combined_data['lats'].extend(dataset['lats'])
        combined_data['lons'].extend(dataset['lons'])
        combined_data['regions'].extend([dataset['region']] * len(dataset['lats']))

        if combined_data['hs_m'] is None:
            combined_data['hs_m'] = dataset['hs_m']
            combined_data['tp_s'] = dataset['tp_s']
            combined_data['dp_deg'] = dataset['dp_deg']
            if dataset['wave_energy_density'] is not None:
                combined_data['wave_energy_density'] = dataset['wave_energy_density']
                combined_data['wave_frequencies'] = dataset['wave_frequencies']
        else:
            combined_data['hs_m'] = np.concatenate([combined_data['hs_m'], dataset['hs_m']], axis=1)
            combined_data['tp_s'] = np.concatenate([combined_data['tp_s'], dataset['tp_s']], axis=1)
            combined_data['dp_deg'] = np.concatenate([combined_data['dp_deg'], dataset['dp_deg']], axis=1)

            if dataset['wave_energy_density'] is not None and combined_data['wave_energy_density'] is not None:
                combined_data['wave_energy_density'] = np.concatenate(
                    [combined_data['wave_energy_density'], dataset['wave_energy_density']], axis=2)

    for key in ['lats', 'lons']:
        combined_data[key] = np.array(combined_data[key])

    total_sites = len(combined_data['lats'])
    logger.info(f"Combined CDIP nowcast data: {len(reference_times)} timesteps, {total_sites} sites")

    return combined_data


def find_nearest_cdip_site(cdip_data: Dict, target_lat: float, target_lon: float) -> Optional[int]:
    """Find the nearest CDIP site to the target coordinates."""
    lats = cdip_data['lats']
    lons = cdip_data['lons']

    distances = (lats - target_lat) ** 2 + (lons - target_lon) ** 2
    nearest_idx = int(np.argmin(distances))

    # Quality control: reject if too far (>25km / ~0.5 degrees)
    if distances[nearest_idx] > 0.25:
        return None

    return nearest_idx


def get_grid_locations_from_database() -> List[Dict]:
    """Fetch grid locations using existing Supabase utilities."""
    try:
        grid_points = fetch_grid_points_from_db() or []
        normalized = []
        for gp in grid_points:
            normalized.append({
                "id": gp["id"],
                "LATITUDE": safe_float(gp.get("latitude")),
                "LONGITUDE": safe_float(_normalize_longitude(gp.get("longitude")))
            })
        logger.info(f"Loaded {len(normalized)} grid points from database")
        return normalized
    except Exception as e:
        logger.error(f"Failed to load grid points: {e}")
        return []


def get_existing_grid_records_for_update(table_name: str = GRID_FORECAST_TABLE) -> List[Dict]:
    """Get existing grid forecast records that might need CDIP nowcast updates."""
    try:
        now_pacific = pd.Timestamp.now(tz="America/Los_Angeles")
        nowcast_start = now_pacific - pd.Timedelta(hours=24)

        logger.info(f"Fetching existing grid forecast records from {nowcast_start} onwards...")

        response = supabase.table(table_name).select('*').gte(
            'timestamp', nowcast_start.isoformat()
        ).execute()

        records = response.data or []
        logger.info(f"Found {len(records)} existing grid forecast records to potentially update")
        return records

    except Exception as e:
        logger.error(f"Failed to fetch existing grid forecast records: {e}")
        return []


def update_grid_records_with_cdip_nowcast(existing_records: List[Dict], grid_points: List[Dict], cdip_data: Dict) -> Tuple[List[Dict], Set[str]]:
    """Update existing grid forecast records with CDIP nowcast data."""
    logger.info("Updating grid forecast records with CDIP nowcast data...")

    grid_lookup = {gp["id"]: gp for gp in grid_points}

    updated_count = 0
    updated_records = []
    updated_dates: Set[str] = set()

    for record in existing_records:
        updated_record = record.copy()

        try:
            grid_id = record[GRID_ID_FIELD]
            record_timestamp = pd.Timestamp(record['timestamp'])

            if record_timestamp.tz is None:
                record_timestamp = record_timestamp.tz_localize("America/Los_Angeles")
            else:
                record_timestamp = record_timestamp.tz_convert("America/Los_Angeles")

            grid_point = grid_lookup.get(grid_id)
            if not grid_point or grid_point["LATITUDE"] is None or grid_point["LONGITUDE"] is None:
                updated_records.append(updated_record)
                continue

            cdip_idx = find_nearest_cdip_site(cdip_data, grid_point['LATITUDE'], grid_point['LONGITUDE'])
            if cdip_idx is None:
                updated_records.append(updated_record)
                continue

            matching_time_idx = None
            for idx, cdip_time in enumerate(cdip_data['times']):
                time_diff = abs((cdip_time - record_timestamp).total_seconds())
                if time_diff <= 3600:
                    matching_time_idx = idx
                    break

            if matching_time_idx is None:
                updated_records.append(updated_record)
                continue

            hs_m = cdip_data['hs_m'][matching_time_idx, cdip_idx]
            tp_s = cdip_data['tp_s'][matching_time_idx, cdip_idx]
            dp_deg = cdip_data['dp_deg'][matching_time_idx, cdip_idx]

            if np.isnan(hs_m) or np.isnan(tp_s):
                updated_records.append(updated_record)
                continue

            hs_ft = hs_m * M_TO_FT
            wave_energy_kj = calculate_wave_energy_kj(hs_ft, tp_s)
            rmin_ft, rmax_ft = get_surf_height_range(hs_m)
            rmin_ft, rmax_ft = normalize_surf_range(rmin_ft, rmax_ft)

            updated_record.update({
                "primary_swell_height_ft": safe_float(hs_ft),
                "primary_swell_period_s": safe_float(tp_s),
                "primary_swell_direction": safe_float(dp_deg) if not np.isnan(dp_deg) else None,
                "surf_height_min_ft": safe_float(rmin_ft),
                "surf_height_max_ft": safe_float(rmax_ft),
                "wave_energy_kj": safe_float(wave_energy_kj),
            })

            updated_count += 1
            updated_dates.add(record_timestamp.date().isoformat())
            logger.debug(f"Updated record for grid_id {grid_id} at {record_timestamp}")

        except Exception as e:
            logger.error(f"Error updating record for grid_id {record.get(GRID_ID_FIELD)}: {e}")

        updated_records.append(updated_record)

    logger.info(f"Updated {updated_count} grid records with CDIP nowcast data")
    return updated_records, updated_dates


def selective_upsert_grid_updates(records: List[Dict], table_name: str = GRID_FORECAST_TABLE):
    """Selectively update only the grid records that were modified with CDIP data."""
    if not records:
        logger.warning("No grid records to update")
        return 0

    logger.info(f"Selectively updating {len(records)} grid forecast records...")

    def _prune_record(rec: Dict) -> Dict:
        filtered = {}
        for key, value in rec.items():
            if key in {GRID_ID_FIELD, "timestamp"}:
                filtered[key] = value
            elif value is not None:
                filtered[key] = value
        return filtered

    cleaned_records = [
        _prune_record(rec) for rec in records
        if rec.get(GRID_ID_FIELD) and rec.get("timestamp")
    ]

    if not cleaned_records:
        logger.warning("No grid records contained non-null updates after pruning")
        return 0

    total_updated = 0
    batch_size = 50

    for chunk in chunk_iter(cleaned_records, batch_size):
        try:
            supabase.table(table_name).upsert(
                chunk,
                on_conflict=f"{GRID_ID_FIELD},timestamp"
            ).execute()
            total_updated += len(chunk)
            logger.debug(f"Updated batch of {len(chunk)} grid records")
        except Exception as e:
            logger.error(f"Error updating grid batch: {e}")
            for record in chunk:
                try:
                    supabase.table(table_name).upsert(
                        record,
                        on_conflict=f"{GRID_ID_FIELD},timestamp"
                    ).execute()
                    total_updated += 1
                except Exception as record_error:
                    logger.debug(f"Failed to update grid record: {record_error}")

    logger.info(f"Successfully updated {total_updated} grid forecast records with CDIP nowcast data")
    return total_updated


def delete_previous_day_grid_surf_intensity():
    """Delete all grid surf intensity data from dates before today."""
    try:
        now_pacific = pd.Timestamp.now(tz="America/Los_Angeles")
        today = now_pacific.date().isoformat()

        logger.info(f"Deleting all grid surf intensity data before {today}")
        supabase.table(GRID_DAILY_TABLE).delete().lt(
            "date", today
        ).execute()
        logger.info("Deleted previous grid surf intensity records")
    except Exception as e:
        logger.error(f"Failed to delete previous grid surf intensity: {e}")


def get_all_grid_forecast_dates(table_name: str = GRID_FORECAST_TABLE) -> Set[str]:
    """Get all unique dates that exist in the grid forecast table."""
    try:
        logger.info("Fetching all unique grid forecast dates...")
        page_size = 1000
        start = 0
        dates: Set[str] = set()

        while True:
            end = start + page_size - 1
            response = (
                supabase
                .table(table_name)
                .select("timestamp")
                .range(start, end)
                .execute()
            )

            rows = response.data or []
            if not rows:
                break

            for record in rows:
                timestamp = pd.Timestamp(record['timestamp'])
                if timestamp.tz is None:
                    timestamp = timestamp.tz_localize("America/Los_Angeles")
                else:
                    timestamp = timestamp.tz_convert("America/Los_Angeles")
                dates.add(timestamp.date().isoformat())

            if len(rows) < page_size:
                break
            start += page_size

        if not dates:
            logger.warning("No grid forecast data found")

        logger.info(f"Found {len(dates)} unique grid forecast dates")
        return dates
    except Exception as e:
        logger.error(f"Failed to fetch grid forecast dates: {e}")
        return set()


def refresh_daily_grid_surf_intensity_for_dates(target_dates: Set[str]):
    """Invoke Supabase routine to refresh aggregated grid surf intensity for each target date."""
    if not target_dates:
        logger.info("No target dates to refresh for daily grid surf intensity")
        return

    for target_date in sorted(target_dates):
        try:
            logger.info(f"Refreshing daily grid surf intensity for {target_date}")
            supabase.rpc(
                GRID_REFRESH_FUNCTION,
                {"target_date": target_date}
            ).execute()
        except Exception as e:
            logger.error(f"Failed to refresh grid surf intensity for {target_date}: {e}")


def create_cdip_nowcast_grid_records(grid_points: List[Dict], cdip_data: Dict) -> List[Dict]:
    """Create fully CDIP-driven grid records (not used in main flow, kept for parity)."""
    logger.info(f"Creating CDIP nowcast records for {len(grid_points)} grid points...")

    records_dict = {}

    for time_idx in range(len(cdip_data['times'])):
        timestamp = cdip_data['times'].iloc[time_idx]

        pacific_hour = timestamp.hour
        interval_index = (pacific_hour // 3) % 8
        pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
        target_pacific_hour = pacific_intervals[interval_index]

        clean_pacific_time = pd.Timestamp.combine(
            timestamp.date(),
            pd.Timestamp(f"{target_pacific_hour:02d}:00:00").time()
        ).tz_localize("America/Los_Angeles")

        logger.info(f"Processing timestamp {time_idx + 1}/{len(cdip_data['times'])}: {clean_pacific_time}")

        for grid_point in grid_points:
            try:
                unique_key = f"{grid_point['id']}_{clean_pacific_time.isoformat()}"
                if unique_key in records_dict:
                    continue

                cdip_idx = find_nearest_cdip_site(cdip_data, grid_point['LATITUDE'], grid_point['LONGITUDE'])
                if cdip_idx is None:
                    continue

                hs_m = cdip_data['hs_m'][time_idx, cdip_idx]
                tp_s = cdip_data['tp_s'][time_idx, cdip_idx]
                dp_deg = cdip_data['dp_deg'][time_idx, cdip_idx]

                if np.isnan(hs_m) or np.isnan(tp_s):
                    continue

                hs_ft = hs_m * M_TO_FT
                surf_min_ft, surf_max_ft = get_surf_height_range(hs_m)
                surf_min_ft, surf_max_ft = normalize_surf_range(surf_min_ft, surf_max_ft)
                wave_energy_kj = calculate_wave_energy_kj(hs_ft, tp_s)

                record = {
                    GRID_ID_FIELD: grid_point["id"],
                    "timestamp": clean_pacific_time.isoformat(),
                    "primary_swell_height_ft": safe_float(hs_ft),
                    "primary_swell_period_s": safe_float(tp_s),
                    "primary_swell_direction": safe_float(dp_deg) if not np.isnan(dp_deg) else None,
                    "surf_height_min_ft": safe_float(surf_min_ft),
                    "surf_height_max_ft": safe_float(surf_max_ft),
                    "wave_energy_kj": safe_float(wave_energy_kj),
                }

                records_dict[unique_key] = record

            except Exception as e:
                logger.error(f"Error creating CDIP nowcast record for grid {grid_point['id']}: {e}")

    return list(records_dict.values())


def selective_upsert_cdip_grid_updates(records: List[Dict], table_name: str = GRID_FORECAST_TABLE):
    """Upsert CDIP nowcast records directly (full replacement)."""
    if not records:
        logger.warning("No CDIP grid records to upsert")
        return 0

    logger.info(f"Upserting {len(records)} CDIP grid nowcast records...")

    total_inserted = 0

    for chunk in chunk_iter(records, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk,
                on_conflict=f"{GRID_ID_FIELD},timestamp"
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"Upserted chunk of {len(chunk)} records")
        except Exception as e:
            logger.error(f"Error upserting chunk: {e}")

    logger.info(f"Successfully upserted {total_inserted} CDIP grid nowcast records")
    return total_inserted


def main():
    """Main execution function for grid-based CDIP nowcast updates."""
    try:
        logger.info("Starting CDIP grid nowcast selective update...")

        delete_previous_day_grid_surf_intensity()

        existing_records = get_existing_grid_records_for_update()
        if not existing_records:
            logger.info("No existing grid forecast records found in nowcast time window")
            return True

        grid_points = get_grid_locations_from_database()
        if not grid_points:
            logger.error("No grid points loaded from database")
            return False

        cdip_datasets = []
        for region, url in CDIP_NOWCAST_URLS.items():
            dataset = load_cdip_nowcast_dataset(url, region)
            if dataset:
                cdip_datasets.append(dataset)

        if not cdip_datasets:
            logger.error("No CDIP datasets loaded successfully")
            return False

        combined_cdip_data = combine_cdip_datasets(cdip_datasets)
        if not combined_cdip_data:
            logger.error("Failed to combine CDIP datasets")
            return False

        updated_records, updated_dates = update_grid_records_with_cdip_nowcast(existing_records, grid_points, combined_cdip_data)

        selective_upsert_grid_updates(updated_records)

        all_dates = get_all_grid_forecast_dates()
        if updated_dates:
            all_dates.update(updated_dates)
        refresh_daily_grid_surf_intensity_for_dates(all_dates)

        logger.info("CDIP grid nowcast selective update completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error in grid nowcast execution: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if success:
        logger.info("CDIP grid nowcast script completed successfully")
        sys.exit(0)
    else:
        logger.error("CDIP grid nowcast script failed")
        sys.exit(1)
