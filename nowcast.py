#!/usr/bin/env python3
"""
CDIP Nowcast Data Updater for Supabase
Replaces existing forecast data with current CDIP nowcast conditions
Uses existing config.py and database patterns for consistency
"""

import sys
import xarray as xr
import numpy as np
import pandas as pd
import pytz
from datetime import datetime, timezone
from supabase import create_client, Client
from typing import List, Dict, Optional, Tuple

# Import your existing config and database utilities
try:
    from config import SUPABASE_URL, SUPABASE_KEY, logger, UPSERT_CHUNK
    from utils import chunk_iter, safe_float, normalize_surf_range
    from database import supabase  # Use existing supabase client
except ImportError:
    print("ERROR: Could not import required modules - make sure config.py, utils.py, and database.py are available")
    sys.exit(1)

# Use the same compact surf-range logic used elsewhere
from swell_ranking import get_surf_height_range, calculate_wave_energy_kj

# Configuration
CDIP_NOWCAST_URLS = {
    'socal': "http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/socal_alongshore_nowcast.nc",
    'norcal': "http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/norcal_alongshore_nowcast.nc"
}

# Constants
M_TO_FT = 3.28084
PACIFIC_TZ = pytz.timezone('America/Los_Angeles')

def load_cdip_nowcast_dataset(url: str, region_name: str) -> Optional[Dict]:
    """Load a single CDIP nowcast dataset."""
    try:
        logger.info(f"Loading CDIP {region_name} nowcast from {url}")
        ds = xr.open_dataset(url)
        
        # Variable names for CDIP nowcast
        time_name = "waveTime"
        site_dim = "siteCount" 
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
    
    # Use first dataset's times as reference
    reference_times = datasets[0]['times']
    
    # Combine all data
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
        # Check time compatibility
        if not dataset['times'].equals(reference_times):
            logger.warning(f"Time mismatch in {dataset['region']} dataset - skipping")
            continue
        
        # Extend location data
        combined_data['lats'].extend(dataset['lats'])
        combined_data['lons'].extend(dataset['lons'])
        combined_data['regions'].extend([dataset['region']] * len(dataset['lats']))
        
        # Concatenate wave data along site dimension
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
            
            # Concatenate energy density along site dimension (axis=2)
            if dataset['wave_energy_density'] is not None and combined_data['wave_energy_density'] is not None:
                combined_data['wave_energy_density'] = np.concatenate(
                    [combined_data['wave_energy_density'], dataset['wave_energy_density']], axis=2)
    
    # Convert lists to arrays
    for key in ['lats', 'lons']:
        combined_data[key] = np.array(combined_data[key])
    
    total_sites = len(combined_data['lats'])
    logger.info(f"Combined CDIP nowcast data: {len(reference_times)} timesteps, {total_sites} sites")
    
    return combined_data

def calculate_spectral_energy(cdip_data: Dict, site_idx: int, time_idx: int) -> Optional[float]:
    """Calculate total wave energy from CDIP spectral energy density."""
    if cdip_data['wave_energy_density'] is None:
        return None
    
    try:
        # Get spectral energy density for this site and time
        energy_spectrum = cdip_data['wave_energy_density'][time_idx, :, site_idx]
        frequencies = cdip_data['wave_frequencies']
        
        # Remove NaN values
        valid_mask = ~np.isnan(energy_spectrum)
        if not np.any(valid_mask):
            return None
        
        clean_spectrum = energy_spectrum[valid_mask]
        clean_freqs = frequencies[valid_mask]
        
        if len(clean_spectrum) < 2:
            return None
        
        # Integrate energy density over frequency
        total_energy_m2 = np.trapz(clean_spectrum, clean_freqs)
        
        # Convert to kJ/m² using seawater density
        rho_g = 1025 * 9.81  # N/m³
        energy_kj_per_m2 = (total_energy_m2 * rho_g) / 1000
        
        return float(energy_kj_per_m2)
        
    except Exception as e:
        logger.debug(f"Error calculating spectral energy: {e}")
        return None

def find_nearest_cdip_site(cdip_data: Dict, target_lat: float, target_lon: float) -> Optional[int]:
    """Find the nearest CDIP site to a beach location."""
    lats = cdip_data['lats']
    lons = cdip_data['lons']
    
    # Calculate distances
    distances = (lats - target_lat)**2 + (lons - target_lon)**2
    nearest_idx = int(np.argmin(distances))
    
    # Quality control: reject if too far (>25km / ~0.5 degrees)
    if distances[nearest_idx] > 0.25:
        return None
        
    return nearest_idx

def get_beach_locations_from_database() -> List[Dict]:
    """Fetch beach locations using existing database utilities."""
    try:
        from database import fetch_all_beaches
        beaches = fetch_all_beaches()
        logger.info(f"Loaded {len(beaches)} beaches from database")
        return beaches
    except Exception as e:
        logger.error(f"Failed to load beaches: {e}")
        return []

def get_existing_forecast_records_for_update(table_name: str = "forecast_data") -> List[Dict]:
    """Get existing forecast records that might need CDIP nowcast updates."""
    try:
        # Get current time window for nowcast updates (last 24 hours)
        now_pacific = pd.Timestamp.now(tz="America/Los_Angeles")
        nowcast_start = now_pacific - pd.Timedelta(hours=24)
        
        logger.info(f"Fetching existing forecast records from {nowcast_start} onwards...")
        
        # Only fetch records within the nowcast time window
        response = supabase.table(table_name).select('*').gte(
            'timestamp', nowcast_start.isoformat()
        ).execute()
        
        records = response.data or []
        logger.info(f"Found {len(records)} existing forecast records to potentially update")
        return records
        
    except Exception as e:
        logger.error(f"Failed to fetch existing forecast records: {e}")
        return []

def update_records_with_cdip_nowcast(existing_records: List[Dict], beaches: List[Dict], cdip_data: Dict) -> List[Dict]:
    """Update existing forecast records with CDIP nowcast data where timestamps match."""
    
    logger.info("Updating existing forecast records with CDIP nowcast data...")
    
    # Create beach lookup
    beach_lookup = {b["id"]: b for b in beaches}
    
    # Track what we update
    updated_count = 0
    updated_records = []
    
    for record in existing_records:
        updated_record = record.copy()
        
        try:
            beach_id = record['beach_id']
            record_timestamp = pd.Timestamp(record['timestamp'])
            
            # Ensure timezone awareness
            if record_timestamp.tz is None:
                record_timestamp = record_timestamp.tz_localize("America/Los_Angeles")
            
            beach = beach_lookup.get(beach_id)
            if not beach:
                updated_records.append(updated_record)
                continue
            
            # Find nearest CDIP site
            cdip_idx = find_nearest_cdip_site(cdip_data, beach['LATITUDE'], beach['LONGITUDE'])
            if cdip_idx is None:
                updated_records.append(updated_record)
                continue
            
            # Find matching CDIP timestamp (within 1 hour tolerance)
            matching_time_idx = None
            for idx, cdip_time in enumerate(cdip_data['times']):
                time_diff = abs((cdip_time - record_timestamp).total_seconds())
                if time_diff <= 3600:  # Within 1 hour
                    matching_time_idx = idx
                    break
            
            if matching_time_idx is None:
                updated_records.append(updated_record)
                continue
            
            # Extract CDIP data
            hs_m = cdip_data['hs_m'][matching_time_idx, cdip_idx]
            tp_s = cdip_data['tp_s'][matching_time_idx, cdip_idx]
            dp_deg = cdip_data['dp_deg'][matching_time_idx, cdip_idx]
            
            # Only update if CDIP has valid data
            if np.isnan(hs_m) or np.isnan(tp_s):
                updated_records.append(updated_record)
                continue
            
            # Update the record with CDIP nowcast data
            hs_ft = hs_m * M_TO_FT
            
            # Surf-Forecast-like energy index based on CDIP H,T
            wave_energy_kj = calculate_wave_energy_kj(hs_ft, tp_s)
            
            # Compute compact Surfline-style height range (in feet)
            rmin_ft, rmax_ft = get_surf_height_range(hs_m)
            rmin_ft, rmax_ft = normalize_surf_range(rmin_ft, rmax_ft)

            # Update ONLY the wave-related fields with CDIP nowcast data
            updated_record.update({
                "primary_swell_height_ft": safe_float(hs_ft),
                "primary_swell_period_s": safe_float(tp_s),
                "primary_swell_direction": safe_float(dp_deg) if not np.isnan(dp_deg) else None,
                "surf_height_min_ft": safe_float(rmin_ft),
                "surf_height_max_ft": safe_float(rmax_ft),
                "wave_energy_kj": safe_float(wave_energy_kj),
            })
            
            # Keep ALL other existing fields (wind, weather, secondary swells, etc.)
            updated_count += 1
            logger.debug(f"Updated record for beach {beach_id} at {record_timestamp}")
            
        except Exception as e:
            logger.error(f"Error updating record for beach {record.get('beach_id')}: {e}")
        
        updated_records.append(updated_record)
    
    logger.info(f"Updated {updated_count} existing records with CDIP nowcast data")
    return updated_records

def selective_upsert_cdip_updates(records: List[Dict], table_name: str = "forecast_data"):
    """Selectively update only the records that were modified with CDIP data."""
    if not records:
        logger.warning("No records to update")
        return 0
    
    logger.info(f"Selectively updating {len(records)} forecast records...")
    
    total_updated = 0
    
    # Process in smaller batches to be safer
    batch_size = 50
    for chunk in chunk_iter(records, batch_size):
        try:
            # Use upsert to update existing records
            supabase.table(table_name).upsert(
                chunk,
                on_conflict="beach_id,timestamp"
            ).execute()
            
            total_updated += len(chunk)
            logger.debug(f"Updated batch of {len(chunk)} records")
            
        except Exception as e:
            logger.error(f"Error updating batch: {e}")
            # Try individual updates as fallback
            for record in chunk:
                try:
                    supabase.table(table_name).upsert(
                        record,
                        on_conflict="beach_id,timestamp"
                    ).execute()
                    total_updated += 1
                except Exception as record_error:
                    logger.debug(f"Failed to update record: {record_error}")
    
    logger.info(f"Successfully updated {total_updated} forecast records with CDIP nowcast data")
    return total_updated

def create_cdip_nowcast_records(beaches: List[Dict], cdip_data: Dict) -> List[Dict]:
    """Create nowcast records for all beaches and timestamps - REPLACES existing data."""
    logger.info(f"Creating CDIP nowcast records for {len(beaches)} beaches...")
    
    # Use a dictionary to prevent duplicates by (beach_id, timestamp)
    records_dict = {}
    
    for time_idx in range(len(cdip_data['times'])):
        timestamp = cdip_data['times'].iloc[time_idx]
        
        # Align to 3-hour intervals (matching your forecast format)
        pacific_hour = timestamp.hour
        interval_index = (pacific_hour // 3) % 8
        pacific_intervals = [0, 3, 6, 9, 12, 15, 18, 21]
        target_pacific_hour = pacific_intervals[interval_index]
        
        clean_pacific_time = pd.Timestamp.combine(
            timestamp.date(),
            pd.Timestamp(f"{target_pacific_hour:02d}:00:00").time()
        ).tz_localize("America/Los_Angeles")
        
        logger.info(f"Processing timestamp {time_idx + 1}/{len(cdip_data['times'])}: {clean_pacific_time}")
        
        for beach in beaches:
            try:
                # Create unique key to prevent duplicates
                unique_key = f"{beach['id']}_{clean_pacific_time.isoformat()}"
                
                # Skip if we already have a record for this beach/time combination
                if unique_key in records_dict:
                    continue
                
                # Find nearest CDIP site
                cdip_idx = find_nearest_cdip_site(cdip_data, beach['LATITUDE'], beach['LONGITUDE'])
                if cdip_idx is None:
                    continue
                
                # Extract wave data
                hs_m = cdip_data['hs_m'][time_idx, cdip_idx]
                tp_s = cdip_data['tp_s'][time_idx, cdip_idx]
                dp_deg = cdip_data['dp_deg'][time_idx, cdip_idx]
                
                # Skip if no valid data
                if np.isnan(hs_m) or np.isnan(tp_s):
                    continue
                
                # Convert to feet
                hs_ft = hs_m * M_TO_FT
                
                # Calculate compact Surfline-style surf height range (feet)
                surf_min_ft, surf_max_ft = get_surf_height_range(hs_m)
                surf_min_ft, surf_max_ft = normalize_surf_range(surf_min_ft, surf_max_ft)

                # Calculate wave energy (spectral if available, otherwise parametric)
                # Use index consistently even when spectral is available
                wave_energy_kj = calculate_wave_energy_kj(hs_ft, tp_s)
                
                record = {
                    "beach_id": beach["id"],
                    "timestamp": clean_pacific_time.isoformat(),
                    
                    # Primary swell (CDIP nowcast data - REPLACES forecast)
                    "primary_swell_height_ft": safe_float(hs_ft),
                    "primary_swell_period_s": safe_float(tp_s),
                    "primary_swell_direction": safe_float(dp_deg) if not np.isnan(dp_deg) else None,
                    
                    # Secondary/tertiary swells (not available in nowcast)
                    "secondary_swell_height_ft": None,
                    "secondary_swell_period_s": None,
                    "secondary_swell_direction": None,
                    "tertiary_swell_height_ft": None,
                    "tertiary_swell_period_s": None,
                    "tertiary_swell_direction": None,
                    
                    # Surf data (REPLACED with CDIP-based calculations)
                    "surf_height_min_ft": safe_float(surf_min_ft),
                    "surf_height_max_ft": safe_float(surf_max_ft),
                    "wave_energy_kj": safe_float(wave_energy_kj),
                    
                    # Wind/weather data (not available in CDIP nowcast)
                    "wind_speed_mph": None,
                    "wind_direction_deg": None,
                    "wind_gust_mph": None,
                    "water_temp_f": None,
                    "tide_level_ft": None,
                    "temperature": None,
                    "weather": None,
                    "pressure_inhg": None,
                }
                
                # Store in dictionary to prevent duplicates
                records_dict[unique_key] = record
                
            except Exception as e:
                logger.error(f"Error creating record for beach {beach.get('Name', 'Unknown')}: {e}")
    
    # Convert dictionary values to list
    all_records = list(records_dict.values())
    logger.info(f"Created {len(all_records)} unique CDIP nowcast records (duplicates removed)")
    return all_records

def upsert_cdip_nowcast_data(records: List[Dict], table_name: str = "forecast_data"):
    """Upsert CDIP nowcast records using existing database pattern."""
    logger.info(f"Upserting {len(records)} CDIP nowcast records to {table_name}...")
    
    if not records:
        logger.warning("No records to upsert")
        return 0
    
    total_inserted = 0
    
    # Use same chunking pattern as your database.py
    for chunk in chunk_iter(records, UPSERT_CHUNK):
        try:
            supabase.table(table_name).upsert(
                chunk, 
                on_conflict="beach_id,timestamp"  # Replace existing records
            ).execute()
            total_inserted += len(chunk)
            logger.debug(f"Upserted chunk of {len(chunk)} records")
        except Exception as e:
            logger.error(f"Error upserting chunk: {e}")
    
    logger.info(f"Successfully upserted {total_inserted} CDIP nowcast records")
    return total_inserted

def main():
    """Main execution function - selectively updates existing forecast records with CDIP nowcast data."""
    try:
        logger.info("Starting CDIP nowcast selective update...")
        
        # Get existing forecast records that might need updates
        existing_records = get_existing_forecast_records_for_update()
        if not existing_records:
            logger.info("No existing forecast records found in nowcast time window")
            return True  # Not an error, just nothing to update
        
        # Get beach locations using existing database utilities
        beaches = get_beach_locations_from_database()
        if not beaches:
            logger.error("No beaches loaded from database")
            return False
        
        # Load CDIP nowcast datasets
        cdip_datasets = []
        for region, url in CDIP_NOWCAST_URLS.items():
            dataset = load_cdip_nowcast_dataset(url, region)
            if dataset:
                cdip_datasets.append(dataset)
        
        if not cdip_datasets:
            logger.error("No CDIP datasets loaded successfully")
            return False
        
        # Combine datasets
        combined_cdip_data = combine_cdip_datasets(cdip_datasets)
        if not combined_cdip_data:
            logger.error("Failed to combine CDIP datasets")
            return False
        
        # Update existing records with CDIP nowcast data (preserves all other data)
        updated_records = update_records_with_cdip_nowcast(existing_records, beaches, combined_cdip_data)
        
        # Selectively update only the modified records
        selective_upsert_cdip_updates(updated_records)
        
        logger.info("CDIP nowcast selective update completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("CDIP nowcast script completed successfully")
        sys.exit(0)
    else:
        logger.error("CDIP nowcast script failed")
        sys.exit(1)
