#!/usr/bin/env python3
"""
NOAA GFSwave integration with CDIP data enhancement for Hybrid Surf Database Update Script
Handles NOAA dataset discovery, testing, and data extraction with rate limiting
Enhanced with CDIP data for more accurate swell forecasts
"""

import time
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import pytz

from config import (
    logger, NOAA_BASE_URLS, NOAA_VARS, NOAA_DATASET_TEST_DELAY, 
    NOAA_BATCH_DELAY, NOAA_RETRY_DELAY
)
from utils import (
    enforce_noaa_rate_limit, safe_float, meters_to_feet, mps_to_mph,
    nonempty_record, normalize_surf_range
)
from swell_ranking import (
    rank_swell_trains, calculate_wave_energy_kj, get_surf_height_range
)

# CDIP Constants
CDIP_URLS = {
    'socal': "https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/socal_alongshore_forecast.nc",
    'norcal': "https://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/model/MOP_alongshore/norcal_alongshore_forecast.nc"
}
M_TO_FT = 3.28084

def nearest_valid_value(series, index):
    """Return the closest non-null/non-NaN entry around the given index."""
    if series is None:
        return None
    try:
        values = np.asarray(series)
    except Exception:
        values = series
    try:
        n = len(values)
    except TypeError:
        return None
    if n == 0:
        return None

    def _value_at(pos):
        if pos < 0 or pos >= n:
            return None
        try:
            val = values[pos]
        except Exception:
            return None
        if np.ma.is_masked(val):
            return None
        if isinstance(val, np.ma.MaskedArray):
            val = val.data
        if isinstance(val, np.ndarray):
            if val.size != 1:
                return None
            val = val.item()
        if isinstance(val, np.generic):
            val = val.item()
        if val is None:
            return None
        try:
            if np.isnan(val):
                return None
        except Exception:
            pass
        return val

    current = _value_at(index)
    if current is not None:
        return current
    for offset in range(1, n):
        left = index - offset
        if left >= 0:
            val = _value_at(left)
            if val is not None:
                return val
        right = index + offset
        if right < n:
            val = _value_at(right)
            if val is not None:
                return val
    return None

def load_single_cdip_dataset(url, region_name):
    """Load a single CDIP dataset (SoCal or NorCal)."""
    try:
        logger.info(f"   Loading CDIP {region_name} dataset from {url}")
        ds = xr.open_dataset(url)
        
        # Dimension/variable names for CDIP
        time_name = "waveTime"
        site_dim = "siteCount"
        lat_name = "metaLatitude"
        lon_name = "metaLongitude"
        hs_name = "waveHs"   # meters
        tp_name = "waveTp"   # seconds
        dp_name = "waveDp"   # degrees (from)
        
        # Convert times to Pacific timezone
        t_utc = pd.to_datetime(ds[time_name].values)
        t_local = (
            pd.Series(t_utc)
            .dt.tz_localize("UTC")
            .dt.tz_convert("America/Los_Angeles")
        )
        
        # Get site locations
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Extract all site data
        hs_m = ds[hs_name].values  # shape: (time, sites)
        tp_s = ds[tp_name].values
        dp_deg = ds[dp_name].values
        
        # Extract spectral wave energy density if available
        wave_energy_density = None
        try:
            if "waveEnergyDensity" in ds.variables:
                wave_energy_density = ds["waveEnergyDensity"].values  # shape: (time, frequency, sites)
                logger.info(f"   CDIP {region_name}: Found spectral energy density data")
            else:
                logger.info(f"   CDIP {region_name}: No spectral energy density data available")
        except Exception as e:
            logger.warning(f"   CDIP {region_name}: Could not load energy density: {e}")
        
        # Get frequency bins for energy density if available
        wave_frequencies = None
        if wave_energy_density is not None:
            try:
                if "waveFrequency" in ds.variables:
                    wave_frequencies = ds["waveFrequency"].values  # Hz
            except Exception:
                pass
        
        ds.close()
        
        dataset_info = {
            'region': region_name,
            'times': t_local,
            'lats': lats,
            'lons': lons,
            'hs_m': hs_m,
            'tp_s': tp_s,
            'dp_deg': dp_deg,
            'wave_energy_density': wave_energy_density,  # (time, frequency, sites) or None
            'wave_frequencies': wave_frequencies         # frequency bins in Hz or None
        }
        
        logger.info(f"   CDIP {region_name} loaded: {len(t_local)} timesteps, {len(lats)} sites")
        return dataset_info
        
    except Exception as e:
        logger.warning(f"   Failed to load CDIP {region_name} from {url}: {e}")
        return None

def load_cdip_data():
    """Load and combine CDIP forecast data for both Northern and Southern California."""
    logger.info("   Loading CDIP forecast data for California coastline...")
    
    cdip_datasets = []
    
    # Load both SoCal and NorCal datasets
    for region, url in CDIP_URLS.items():
        dataset = load_single_cdip_dataset(url, region)
        if dataset is not None:
            cdip_datasets.append(dataset)
    
    if not cdip_datasets:
        logger.warning("   No CDIP datasets loaded successfully")
        return None
    
    # Combine datasets
    logger.info(f"   Combining {len(cdip_datasets)} CDIP datasets...")
    
    # Use the first dataset's times as reference
    reference_times = cdip_datasets[0]['times']
    
    # Combine all locations
    all_lats = []
    all_lons = []
    all_hs_m = []
    all_tp_s = []
    all_dp_deg = []
    all_energy_density = []
    all_frequencies = None
    regions = []
    
    for dataset in cdip_datasets:
        # Check time compatibility
        if not dataset['times'].equals(reference_times):
            logger.warning(f"   Time mismatch in {dataset['region']} dataset - interpolating...")
            # For now, skip datasets with different times - could add interpolation later
            continue
        
        all_lats.extend(dataset['lats'])
        all_lons.extend(dataset['lons'])
        regions.extend([dataset['region']] * len(dataset['lats']))
        
        # Concatenate data arrays along site dimension
        if len(all_hs_m) == 0:
            all_hs_m = dataset['hs_m']
            all_tp_s = dataset['tp_s']
            all_dp_deg = dataset['dp_deg']
            if dataset['wave_energy_density'] is not None:
                all_energy_density = dataset['wave_energy_density']
                all_frequencies = dataset['wave_frequencies']
        else:
            all_hs_m = np.concatenate([all_hs_m, dataset['hs_m']], axis=1)
            all_tp_s = np.concatenate([all_tp_s, dataset['tp_s']], axis=1)
            all_dp_deg = np.concatenate([all_dp_deg, dataset['dp_deg']], axis=1)
            
            # Concatenate energy density along site dimension (axis=2)
            if dataset['wave_energy_density'] is not None and len(all_energy_density) > 0:
                all_energy_density = np.concatenate([all_energy_density, dataset['wave_energy_density']], axis=2)
            elif dataset['wave_energy_density'] is not None:
                all_energy_density = dataset['wave_energy_density']
                all_frequencies = dataset['wave_frequencies']
    
    if len(all_lats) == 0:
        logger.warning("   No compatible CDIP datasets found")
        return None
    
    combined_data = {
        'times': reference_times,
        'lats': np.array(all_lats),
        'lons': np.array(all_lons),
        'regions': regions,
        'hs_m': all_hs_m,
        'tp_s': all_tp_s,
        'dp_deg': all_dp_deg,
        'wave_energy_density': all_energy_density if len(all_energy_density) > 0 else None,
        'wave_frequencies': all_frequencies
    }
    
    total_sites = len(all_lats)
    logger.info(f"   CDIP combined data loaded: {len(reference_times)} timesteps, {total_sites} sites")
    
    # Log regional breakdown
    region_counts = {}
    for region in regions:
        region_counts[region] = region_counts.get(region, 0) + 1
    
    for region, count in region_counts.items():
        logger.info(f"     {region.upper()}: {count} sites")
    
    return combined_data

def find_nearest_cdip_site(cdip_data, target_lat, target_lon):
    """Find the nearest CDIP site to a beach location from combined NorCal/SoCal datasets."""
    if cdip_data is None:
        return None
        
    lats = cdip_data['lats']
    lons = cdip_data['lons']
    
    # Calculate distances
    distances = (lats - target_lat)**2 + (lons - target_lon)**2
    nearest_idx = int(np.argmin(distances))
    
    # Check if the nearest site is reasonable (within ~50km / ~0.5 degrees)
    if distances[nearest_idx] > 0.25:  # ~25km threshold
        return None
    
    # Log which region the site came from
    if 'regions' in cdip_data:
        region = cdip_data['regions'][nearest_idx]
        logger.debug(f"   Selected CDIP site from {region.upper()} region")
        
    return nearest_idx

def calculate_cdip_wave_energy(cdip_data, site_idx, time_idx):
    """Calculate wave energy per foot of crest per wave from CDIP spectra.

    Steps:
      1) Integrate spectral variance (m^2/Hz over Hz) -> variance (m^2)
      2) Convert to energy per unit area: E_area = variance * rho * g  [kJ/m^2]
      3) Convert to per-crest-per-wave: E_crest = E_area * Cg * T       [kJ/m]
         with deep-water Cg = g*T/(4*pi) using CDIP peak period T
      4) Convert to per foot of crest: divide by 3.28084
    """
    if cdip_data is None or cdip_data.get('wave_energy_density') is None:
        return None
    
    try:
        energy_spectrum = cdip_data['wave_energy_density'][time_idx, :, site_idx]
        frequencies = cdip_data.get('wave_frequencies')
        if frequencies is None or len(energy_spectrum) == 0:
            return None

        valid_mask = ~np.isnan(energy_spectrum)
        if not np.any(valid_mask):
            return None

        clean_spectrum = energy_spectrum[valid_mask]
        clean_freqs = frequencies[valid_mask]
        if len(clean_spectrum) < 2:
            return None

        total_variance_m2 = np.trapz(clean_spectrum, clean_freqs)

        # Energy per unit surface area (kJ/m^2)
        rho_g = 1025 * 9.81  # N/m^3
        energy_kj_per_m2 = (total_variance_m2 * rho_g) / 1000.0

        # Use CDIP peak period to estimate group velocity (deep water)
        T = cdip_data['tp_s'][time_idx, site_idx]
        if T is None or np.isnan(T) or T <= 0:
            return None
        g = 9.81
        Cg = g * T / (4.0 * np.pi)

        # Convert to per-crest-length per wave (kJ/m), then to kJ/ft
        energy_kj_per_m = energy_kj_per_m2 * Cg * T
        energy_kj_per_ft = energy_kj_per_m / 3.28084
        return float(energy_kj_per_ft)

    except Exception as e:
        logger.debug(f"   Error calculating CDIP wave energy: {e}")
        return None
    """Interpolate CDIP data to match GFS 3-hour intervals."""
    if len(cdip_values) == 0 or len(gfs_times) == 0:
        return np.full(len(gfs_times), np.nan)
    
    # Convert to numeric timestamps for interpolation
    cdip_numeric = pd.Series(cdip_times).astype('int64').values
    gfs_numeric = pd.Series(gfs_times).astype('int64').values
    
    # Remove any NaN values from CDIP data
    valid_mask = ~np.isnan(cdip_values)
    if not np.any(valid_mask):
        return np.full(len(gfs_times), np.nan)
    
    cdip_clean_times = cdip_numeric[valid_mask]
    cdip_clean_values = cdip_values[valid_mask]
    
    # Interpolate to GFS times
    interpolated = np.interp(gfs_numeric, cdip_clean_times, cdip_clean_values, 
                           left=np.nan, right=np.nan)
    
    return interpolated

def interpolate_cdip_to_gfs_times(cdip_times, cdip_values, gfs_times):
    """Interpolate CDIP data to match GFS 3-hour intervals."""
    if len(cdip_values) == 0 or len(gfs_times) == 0:
        return np.full(len(gfs_times), np.nan)
    
    # Convert to numeric timestamps for interpolation
    cdip_numeric = pd.Series(cdip_times).astype('int64').values
    gfs_numeric = pd.Series(gfs_times).astype('int64').values
    
    # Remove any NaN values from CDIP data
    valid_mask = ~np.isnan(cdip_values)
    if not np.any(valid_mask):
        return np.full(len(gfs_times), np.nan)
    
    cdip_clean_times = cdip_numeric[valid_mask]
    cdip_clean_values = cdip_values[valid_mask]
    
    # Interpolate to GFS times
    interpolated = np.interp(gfs_numeric, cdip_clean_times, cdip_clean_values, 
                           left=np.nan, right=np.nan)
    
    return interpolated

def enhance_with_cdip_data(beach, grid_data, cdip_data):
    """Enhance NOAA grid data with CDIP data for better accuracy."""
    if cdip_data is None:
        return grid_data  # Return original data if CDIP unavailable
    
    # Find nearest CDIP site (not exact point - closest available point)
    cdip_idx = find_nearest_cdip_site(cdip_data, beach["LATITUDE"], beach["LONGITUDE"])
    if cdip_idx is None:
        logger.debug(f"   No suitable CDIP site for {beach['Name']} (lat={beach['LATITUDE']:.3f}, lon={beach['LONGITUDE']:.3f})")
        return grid_data
    
    # Calculate distance for logging
    distance_km = ((cdip_data['lats'][cdip_idx] - beach['LATITUDE'])**2 + 
                   (cdip_data['lons'][cdip_idx] - beach['LONGITUDE'])**2)**0.5 * 111
    
    logger.debug(f"   Using CDIP site {cdip_idx} for {beach['Name']} "
                f"(CDIP lat={cdip_data['lats'][cdip_idx]:.3f}, lon={cdip_data['lons'][cdip_idx]:.3f}, "
                f"distance≈{distance_km:.1f}km)")
    
    # Extract CDIP data for this site
    cdip_hs_m = cdip_data['hs_m'][:, cdip_idx]
    cdip_tp_s = cdip_data['tp_s'][:, cdip_idx]
    cdip_dp_deg = cdip_data['dp_deg'][:, cdip_idx]
    
    # Convert CDIP times to match GFS timezone (Pacific)
    gfs_times_pacific = []
    for ts in grid_data['time_vals']:
        if hasattr(ts, 'tz_convert'):
            pacific_time = ts.tz_convert("America/Los_Angeles")
        else:
            # Assume UTC and convert
            pacific_time = pd.Timestamp(ts).tz_localize("UTC").tz_convert("America/Los_Angeles")
        gfs_times_pacific.append(pacific_time)
    
    # Interpolate CDIP data to GFS 3-hour intervals
    cdip_hs_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_hs_m, gfs_times_pacific)
    cdip_tp_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_tp_s, gfs_times_pacific)
    cdip_dp_interp = interpolate_cdip_to_gfs_times(cdip_data['times'], cdip_dp_deg, gfs_times_pacific)
    
    # Create enhanced grid data
    enhanced_data = grid_data.copy()
    
    # Replace primary swell with CDIP data where available
    valid_cdip = ~np.isnan(cdip_hs_interp)
    if np.any(valid_cdip):
        logger.debug(f"   Replacing {np.sum(valid_cdip)} timesteps with CDIP data for {beach['Name']}")
        
        # Convert CDIP height - it's already in meters, so keep in meters for consistency with NOAA
        cdip_hs_for_noaa = cdip_hs_interp  # CDIP data is already in meters, no conversion needed
        
        # Replace swell_1 (primary) with CDIP data
        enhanced_data['swell_1_height'] = np.where(valid_cdip, cdip_hs_for_noaa, grid_data['swell_1_height'])
        enhanced_data['swell_1_period'] = np.where(valid_cdip, cdip_tp_interp, grid_data['swell_1_period'])
        enhanced_data['swell_1_direction'] = np.where(valid_cdip, cdip_dp_interp, grid_data['swell_1_direction'])
        
        # Also update significant wave height with CDIP when available
        enhanced_data['sig_wave_height'] = np.where(valid_cdip, cdip_hs_for_noaa, grid_data['sig_wave_height'])
    
    return enhanced_data

def test_noaa_url(url):
    """Test a single NOAA URL and return success/failure with details - RATE LIMITED."""
    try:
        logger.info(f"      Testing: {url}")
        
        # Enforce rate limiting BEFORE each request
        enforce_noaa_rate_limit()
        
        try:
            # Try to open dataset with proper OpenDAP backend specification
            ds_test = xr.open_dataset(
                url, 
                engine='netcdf4',  # Explicitly specify netcdf4 engine for OpenDAP
                decode_times=True,
                chunks=None  # Disable dask chunking for OpenDAP
            )
            
            # Verify we can access time variable
            time_var = ds_test.time
            time_len = len(time_var)
            
            # Verify we can access a swell variable
            swell_var = ds_test.swell_2
            swell_shape = swell_var.shape
            
            ds_test.close()
            logger.info(f"      SUCCESS: {time_len} time steps, swell shape: {swell_shape}")
            return True, f"Working dataset with {time_len} time steps"
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if it's a rate limit error
            if 'rate limit' in error_str or 'over rate limit' in error_str or 'limit exceeded' in error_str:
                logger.error(f"      RATE LIMITED by NOAA! Waiting {NOAA_RETRY_DELAY} seconds...")
                time.sleep(NOAA_RETRY_DELAY)
                return False, "NOAA rate limit exceeded"
            else:
                raise e
        
    except Exception as e:
        error_msg = str(e)
        
        # Check for HTML rate limit response in error message
        if 'over rate limit' in error_msg.lower() or 'doctype html' in error_msg.lower():
            logger.error(f"      NOAA RATE LIMIT DETECTED! Must wait before retrying.")
            logger.error(f"      Suggestion: Wait 1 hour before running again")
            return False, "NOAA rate limit - HTML response detected"
        
        logger.warning(f"      FAILED: {error_msg[:200]}")
        return False, error_msg

def get_noaa_dataset_url():
    """Get the current NOAA GFSwave dataset URL with RATE LIMITED testing."""
    logger.info("   Searching for available NOAA GFSwave dataset (rate limited)...")
    
    # Use UTC time for NOAA dataset dating (NOAA uses UTC for dataset naming)
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y%m%d")
    
    # Also show PST time for user reference
    import pytz
    pst = pytz.timezone('America/Los_Angeles')
    now_pst = now_utc.astimezone(pst)
    
    logger.info(f"   Current UTC time: {now_utc.isoformat()}")
    logger.info(f"   Current PST time: {now_pst.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"   Using dataset date: {today_str} (UTC-based for NOAA compatibility)")
    
    runs_to_try = ["18z", "12z", "06z", "00z"]  # Try newest model cycles first

    # Try today first
    logger.info(f"   Trying today's data: {today_str}")
    for base_url in NOAA_BASE_URLS:
        logger.info(f"   Base URL: {base_url}")
        for run in runs_to_try:
            # CORRECTED URL FORMAT - includes the full dataset filename
            url = f"{base_url}/{today_str}/gfswave.wcoast.0p16_{run}"
            logger.info(f"   Testing URL: {url}")
            success, message = test_noaa_url(url)
            if success:
                logger.info(f"   FOUND: Using {today_str} {run}")
                return url
            
            # Add delay between tests to be extra conservative
            time.sleep(NOAA_DATASET_TEST_DELAY)
    
    # Try yesterday - REDUCED base URLs to minimize requests
    yesterday_date = now_utc - timedelta(days=1)
    yesterday_str = yesterday_date.strftime("%Y%m%d")
    logger.info(f"   Trying yesterday's data: {yesterday_str}")
    
    # REDUCED: Only try first 2 base URLs
    for base_url in NOAA_BASE_URLS[:2]:
        logger.info(f"   Base URL: {base_url}")
        for run in runs_to_try:
            url = f"{base_url}/{yesterday_str}/gfswave.wcoast.0p16_{run}"
            logger.info(f"   Testing URL: {url}")
            success, message = test_noaa_url(url)
            if success:
                logger.info(f"   FOUND: Using {yesterday_str} {run} (yesterday)")
                return url
            
            time.sleep(NOAA_DATASET_TEST_DELAY)
    
    # Try a few more days back - MINIMAL testing to reduce requests
    logger.info("   Trying additional fallback dates...")
    for days_back in [2, 3]:  # Reduced from [2,3,4] to minimize requests
        fallback_date = now_utc - timedelta(days=days_back)
        fallback_str = fallback_date.strftime("%Y%m%d")
        logger.info(f"   Trying {days_back} days back: {fallback_str}")
        
        base_url = NOAA_BASE_URLS[0]
        for run in runs_to_try:
            url = f"{base_url}/{fallback_str}/gfswave.wcoast.0p16_{run}"
            logger.info(f"   Testing URL: {url}")
            success, message = test_noaa_url(url)
            if success:
                logger.info(f"   FOUND: Using {fallback_str} {run} ({days_back} days back)")
                return url

            time.sleep(NOAA_DATASET_TEST_DELAY)
    
    # Comprehensive error message
    logger.error("   EXHAUSTED ALL OPTIONS:")
    logger.error(f"      Current UTC time: {now_utc.isoformat()}")
    logger.error(f"      Tried dates: {today_str}, {yesterday_str}, and {days_back} days back")
    logger.error(f"      Tried base URLs: {len(NOAA_BASE_URLS)} different protocols")
    logger.error(f"      Tried runs: {', '.join(runs_to_try)}")
    logger.error("      Possible causes:")
    logger.error("        - Network connectivity issues")
    logger.error("        - NOAA server maintenance")
    logger.error("        - OpenDAP service unavailable") 
    logger.error("        - Firewall blocking OpenDAP protocol")
    logger.error("        - Missing netcdf4 library (try: pip install netcdf4)")
    logger.error("        - Rate limit penalty box (wait 1+ hours)")
    logger.error("        - Dataset not yet available for today")
    
    raise Exception("No NOAA GFSwave dataset available - exhausted all URL combinations")

def find_nearest_ocean_point(ds, lat0, lon0):
    """Find the nearest valid ocean grid point for a beach location - OPTIMIZED."""
    lon0_360 = lon0 % 360  # Convert longitude to 0-360 format for NOAA data
    
    # Try center point first (most likely to work)
    try:
        test_var = "swell_2"  # Use swell_2 to test
        val = ds[test_var].isel(time=0).sel(lat=lat0, lon=lon0_360, method="nearest").values
        
        if not np.isnan(val):
            grid_lat = float(ds.lat.sel(lat=lat0, method="nearest").values)
            grid_lon = float(ds.lon.sel(lon=lon0_360, method="nearest").values)
            return grid_lat, grid_lon
    except Exception:
        pass
    
    # REDUCED offset search to minimize server requests
    reduced_offsets = [
        (0, 0.1), (0, -0.1), (0.05, 0), (-0.05, 0),  # Essential offsets only
        (0.05, 0.1), (-0.05, -0.1)  # Diagonal fallbacks
    ]
    
    for dlat, dlon in reduced_offsets:
        try:
            lat = lat0 + dlat
            lon = (lon0_360 + dlon) % 360
            
            # Test if this point has valid data
            test_var = "swell_2"  # Use swell_2 to test
            val = ds[test_var].isel(time=0).sel(lat=lat, lon=lon, method="nearest").values
            
            if not np.isnan(val):
                grid_lat = float(ds.lat.sel(lat=lat, method="nearest").values)
                grid_lon = float(ds.lon.sel(lon=lon, method="nearest").values)
                return grid_lat, grid_lon
                
        except Exception:
            continue
    
    return None, None

def load_noaa_dataset(url):
    """Load NOAA dataset with proper error handling and rate limiting."""
    logger.info("   Loading NOAA GFSwave dataset with rate limiting...")
    
    # Enforce rate limiting before dataset open
    enforce_noaa_rate_limit()
    
    try:
        # Load dataset with proper OpenDAP settings
        ds = xr.open_dataset(
            url,
            engine='netcdf4',  # Explicitly use netcdf4 for OpenDAP
            decode_times=True,
            chunks=None  # Disable dask chunking for OpenDAP
        )
        logger.info(f"   NOAA dataset loaded: {len(ds.time)} time steps")
        logger.info(f"   Available variables: {list(ds.data_vars.keys())}")
        return ds
        
    except Exception as e:
        logger.error(f"ERROR: Failed to load NOAA dataset from {url}: {e}")
        raise

def get_noaa_data_bulk_optimized(ds, beaches):
    """
    Extract NOAA data for all beaches efficiently with RATE LIMITING, location grouping,
    and CDIP data enhancement for better accuracy.
    EDIT: Restrict time dimension to [now_utc, now_utc + 7 days).
    """
    logger.info("   RATE-LIMITED: Bulk extracting NOAA data for all beaches with CDIP enhancement...")

    # Load CDIP data once for all beaches (both NorCal and SoCal)
    cdip_data = load_cdip_data()

    # ---- Filter to 7-day window starting at today's local midnight (Pacific) ----
    time_vals_full = pd.to_datetime(ds.time.values)
    # Make sure the index is UTC-aware
    if time_vals_full.tz is None:
        time_vals_full = time_vals_full.tz_localize("UTC")
    else:
        time_vals_full = time_vals_full.tz_convert("UTC")

    # Compute Pacific midnight for "today" and convert to UTC
    pacific_tz = pytz.timezone("America/Los_Angeles")
    pacific_today_midnight = pd.Timestamp.now(pacific_tz).normalize()
    window_start = pacific_today_midnight.tz_convert("UTC")
    window_end = window_start + pd.Timedelta(days=7)

    mask = (time_vals_full >= window_start) & (time_vals_full < window_end)
    sel_idx = np.where(mask)[0]
    if sel_idx.size == 0:
        logger.warning("   NOAA: No timesteps in the requested 7-day window.")
        return []

    filtered_time_vals = time_vals_full[sel_idx]
    # -------------------------------------------

    # Step 1: Group beaches by approximate location to minimize unique grid points
    logger.info("   Grouping beaches by location to minimize server requests...")
    location_groups = {}
    
    for beach in beaches:
        # Round coordinates more aggressively to create larger groups
        # 0.1 degree is roughly 6-7 miles, acceptable for wave data
        rounded_lat = round(beach["LATITUDE"] / 0.1) * 0.1
        rounded_lon = round(beach["LONGITUDE"] / 0.1) * 0.1
        location_key = f"{rounded_lat:.1f},{rounded_lon:.1f}"
        
        if location_key not in location_groups:
            location_groups[location_key] = []
        location_groups[location_key].append(beach)
    
    logger.info(f"   Grouped {len(beaches)} beaches into {len(location_groups)} location groups")
    
    # Step 2: Process each location group with rate limiting
    grid_data_cache = {}
    
    group_count = 0
    for location_key, group_beaches in location_groups.items():
        group_count += 1
        beach_count = len(group_beaches)
        
        logger.info(f"   Loading location group {group_count}/{len(location_groups)}: {location_key} (serves {beach_count} beaches)...")
        
        # Use first beach as representative for grid point search
        representative_beach = group_beaches[0]
        
        # Enforce rate limiting before grid point search
        enforce_noaa_rate_limit()
        
        # Find grid point for this location
        grid_lat, grid_lon = find_nearest_ocean_point(
            ds, 
            representative_beach["LATITUDE"], 
            representative_beach["LONGITUDE"]
        )
        
        if grid_lat is None or grid_lon is None:
            logger.warning(f"   No valid ocean point for location {location_key}")
            continue
        
        # Enforce rate limiting before bulk data extraction
        enforce_noaa_rate_limit()
        
        try:
            # BULK EXTRACT all variables for this grid point at once (SLICED to 7-day window)
            grid_data = extract_grid_point_data(ds, grid_lat, grid_lon, sel_idx, filtered_time_vals)
            
            # ENHANCE with CDIP data for the first beach (representative)
            enhanced_grid_data = enhance_with_cdip_data(representative_beach, grid_data, cdip_data)
            
            grid_data_cache[location_key] = enhanced_grid_data
            logger.info(f"   Location {location_key} loaded and enhanced successfully")
            
        except Exception as e:
            logger.error(f"   Failed to load location {location_key}: {e}")
            grid_data_cache[location_key] = None
        
        # Add delay between location groups to be extra conservative
        if group_count < len(location_groups):
            logger.info(f"   Rate limiting: waiting {NOAA_BATCH_DELAY}s before next location...")
            time.sleep(NOAA_BATCH_DELAY)
    
    # Step 3: Process all beaches using cached grid data
    logger.info("   Processing all beaches using cached and enhanced grid data...")
    all_records = []
    
    for location_key, group_beaches in location_groups.items():
        if location_key not in grid_data_cache or grid_data_cache[location_key] is None:
            continue
            
        base_grid_data = grid_data_cache[location_key]
        
        for beach in group_beaches:
            try:
                # For each beach, further enhance with its specific CDIP data
                beach_enhanced_data = enhance_with_cdip_data(beach, base_grid_data, cdip_data)
                
                # Process this beach using the enhanced grid data
                beach_records = process_beach_with_cached_data(
                    beach, beach_enhanced_data, location_key, cdip_data
                )
                all_records.extend(beach_records)
                
            except Exception as e:
                logger.error(f"   Error processing {beach['Name']} with cached data: {e}")
    
    logger.info(f"   RATE-LIMITED: Processed {len(beaches)} beaches -> {len(all_records)} records (CDIP NorCal/SoCal enhanced)")
    return all_records

def extract_grid_point_data(ds, grid_lat, grid_lon, sel_idx, filtered_time_vals):
    """
    Extract all NOAA variables for a single grid point, sliced to the provided time index (sel_idx).
    """
    try:
        # Select the point once
        s1h = ds["swell_1"].sel(lat=grid_lat, lon=grid_lon).values
        s1p = ds["swper_1"].sel(lat=grid_lat, lon=grid_lon).values
        s1d = ds["swdir_1"].sel(lat=grid_lat, lon=grid_lon).values

        s2h = ds["swell_2"].sel(lat=grid_lat, lon=grid_lon).values
        s2p = ds["swper_2"].sel(lat=grid_lat, lon=grid_lon).values
        s2d = ds["swdir_2"].sel(lat=grid_lat, lon=grid_lon).values

        s3h = ds["swell_3"].sel(lat=grid_lat, lon=grid_lon).values
        s3p = ds["swper_3"].sel(lat=grid_lat, lon=grid_lon).values
        s3d = ds["swdir_3"].sel(lat=grid_lat, lon=grid_lon).values

        hsig = ds["htsgwsfc"].sel(lat=grid_lat, lon=grid_lon).values
        wspd = ds["windsfc"].sel(lat=grid_lat, lon=grid_lon).values
        wdir = ds["wdirsfc"].sel(lat=grid_lat, lon=grid_lon).values

        # Slice with the time index mask so we only keep the next 7 days
        grid_data = {
            'time_vals': filtered_time_vals,
            'swell_1_height': s1h[sel_idx],
            'swell_1_period': s1p[sel_idx],
            'swell_1_direction': s1d[sel_idx],
            'swell_2_height': s2h[sel_idx],
            'swell_2_period': s2p[sel_idx],
            'swell_2_direction': s2d[sel_idx],
            'swell_3_height': s3h[sel_idx],
            'swell_3_period': s3p[sel_idx],
            'swell_3_direction': s3d[sel_idx],
            'sig_wave_height': hsig[sel_idx],
            'wind_speed_mps': wspd[sel_idx],
            'wind_direction_deg': wdir[sel_idx],
        }
        return grid_data
    except Exception as e:
        logger.error(f"Error extracting grid point data at {grid_lat}, {grid_lon}: {e}")
        raise

def process_beach_with_cached_data(beach, grid_data, grid_key, cdip_data=None):
    """
    Process a single beach using pre-loaded and CDIP-enhanced grid data.
    FIXED: Store timestamps aligned to Pacific 3-hour intervals (0, 3, 6, 9, 12, 15, 18, 21).
    Enhanced: Use CDIP spectral energy density when available.
    """
    beach_id = beach["id"]
    name = beach["Name"]
    lat0 = beach["LATITUDE"]
    lon0 = beach["LONGITUDE"]
    
    # Find CDIP site for this specific beach
    cdip_idx = find_nearest_cdip_site(cdip_data, lat0, lon0) if cdip_data else None
    
    records = []
    time_vals = grid_data['time_vals']
    
    # Process each timestamp using cached data
    for i, ts_utc in enumerate(time_vals):
        ts = pd.Timestamp(ts_utc)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        
        # Align to the nearest 3-hour boundary in Pacific time (midnight-anchored)
        local = ts.tz_convert("America/Los_Angeles")
        hour = int(local.strftime('%H'))
        remainder = hour % 3
        lower = hour - remainder
        upper = lower + 3
        target_hour = lower if (hour - lower) <= (upper - hour) else upper
        if target_hour >= 24:
            target_hour = 0
            local = (local + pd.Timedelta(days=1))
        clean_pacific_time = pd.Timestamp(
            year=local.year, month=local.month, day=local.day,
            hour=target_hour, minute=0, second=0, tz="America/Los_Angeles"
        )
        
        # Use the clean timestamp
        final_timestamp = clean_pacific_time.isoformat()

        # Prepare all 3 swell trains for ranking using cached/enhanced data
        swell1_height_m = nearest_valid_value(grid_data['swell_1_height'], i)
        swell1_period_s = nearest_valid_value(grid_data['swell_1_period'], i)
        swell1_direction = nearest_valid_value(grid_data['swell_1_direction'], i)

        swell2_height_m = nearest_valid_value(grid_data['swell_2_height'], i)
        swell2_period_s = nearest_valid_value(grid_data['swell_2_period'], i)
        swell2_direction = nearest_valid_value(grid_data['swell_2_direction'], i)

        swell3_height_m = nearest_valid_value(grid_data['swell_3_height'], i)
        swell3_period_s = nearest_valid_value(grid_data['swell_3_period'], i)
        swell3_direction = nearest_valid_value(grid_data['swell_3_direction'], i)

        swell_trains = [
            {
                'height_ft': safe_float(meters_to_feet(swell1_height_m)) if swell1_height_m is not None else None,
                'period_s': safe_float(swell1_period_s),
                'direction_deg': safe_float(swell1_direction),
                'beach_lat': lat0,
                'beach_lon': lon0,
                'source': 'swell_1_cdip_enhanced'  # Indicate CDIP enhancement
            },
            {
                'height_ft': safe_float(meters_to_feet(swell2_height_m)) if swell2_height_m is not None else None,
                'period_s': safe_float(swell2_period_s),
                'direction_deg': safe_float(swell2_direction),
                'beach_lat': lat0,
                'beach_lon': lon0,
                'source': 'swell_2'
            },
            {
                'height_ft': safe_float(meters_to_feet(swell3_height_m)) if swell3_height_m is not None else None,
                'period_s': safe_float(swell3_period_s),
                'direction_deg': safe_float(swell3_direction),
                'beach_lat': lat0,
                'beach_lon': lon0,
                'source': 'swell_3'
            }
        ]

        # DYNAMICALLY RANK the swell trains for this timestamp
        primary, secondary, tertiary = rank_swell_trains(swell_trains)
        
        # Store primary swell values for energy calculation
        primary_height = primary['height_ft'] if primary else None
        primary_period = primary['period_s'] if primary else None
        
        # NOAA surf height (compact display range in feet) - enhanced with CDIP where available
        sig_wave_height_m = nearest_valid_value(grid_data['sig_wave_height'], i)
        sig_wave_height_m = safe_float(sig_wave_height_m) if sig_wave_height_m is not None else None
        surf_min_ft, surf_max_ft = get_surf_height_range(sig_wave_height_m)
        surf_min_ft, surf_max_ft = normalize_surf_range(surf_min_ft, surf_max_ft)

        # Wind speed: defer to Open-Meteo supplement (do not use NOAA here)
        wind_speed_mph = None
        wind_direction = nearest_valid_value(grid_data['wind_direction_deg'], i)
        wind_direction = safe_float(wind_direction) if wind_direction is not None else None
        
        # Calculate wave energy using CDIP spectral data if available, otherwise fallback
        wave_energy_kj = None
        if cdip_data and 'wave_energy_density' in cdip_data and cdip_data['wave_energy_density'] is not None:
            # Find the matching time index in CDIP data
            try:
                cdip_time_idx = None
                pacific_time_for_match = clean_pacific_time.tz_convert("America/Los_Angeles")
                for idx, cdip_time in enumerate(cdip_data['times']):
                    if abs((cdip_time - pacific_time_for_match).total_seconds()) < 1800:  # Within 30 minutes
                        cdip_time_idx = idx
                        break

                if cdip_time_idx is not None and cdip_idx is not None:
                    cdip_wave_energy = calculate_cdip_wave_energy(cdip_data, cdip_idx, cdip_time_idx)
                    if cdip_wave_energy is not None:
                        wave_energy_kj = cdip_wave_energy
                        logger.debug(f"   Using CDIP spectral energy: {wave_energy_kj:.1f} kJ/ft")
            except Exception as e:
                logger.debug(f"   Error using CDIP spectral energy, falling back: {e}")

        if wave_energy_kj is None:
            # Standard calculation using PRIMARY (highest ranked) swell
            wave_energy_kj = calculate_wave_energy_kj(primary_height, primary_period)

        record = {
            "beach_id": beach_id,
            "timestamp": final_timestamp,  # Clean Pacific intervals: 00:00, 03:00, 06:00, etc.
            
            # DYNAMICALLY RANKED swell data (in feet and seconds) - CDIP-enhanced for accuracy
            "primary_swell_height_ft": primary['height_ft'] if primary else None,
            "primary_swell_period_s": primary['period_s'] if primary else None,
            "primary_swell_direction": primary['direction_deg'] if primary else None,
            
            "secondary_swell_height_ft": secondary['height_ft'] if secondary else None,
            "secondary_swell_period_s": secondary['period_s'] if secondary else None,
            "secondary_swell_direction": secondary['direction_deg'] if secondary else None,
            
            "tertiary_swell_height_ft": tertiary['height_ft'] if tertiary else None,
            "tertiary_swell_period_s": tertiary['period_s'] if tertiary else None,
            "tertiary_swell_direction": tertiary['direction_deg'] if tertiary else None,
            
            # NOAA surf data (in feet) - enhanced with CDIP significant wave height
            "surf_height_min_ft": surf_min_ft,
            "surf_height_max_ft": surf_max_ft,
            "wave_energy_kj": wave_energy_kj,
            
            # Wind data (speed from Open-Meteo supplement; direction from NOAA)
            "wind_speed_mph": wind_speed_mph,
            "wind_direction_deg": wind_direction,
            
            # These will be filled by Open-Meteo later
            "wind_gust_mph": None,
            "water_temp_f": None,
            "tide_level_ft": None,
            "temperature": None,
            "weather": None,
            "pressure_inhg": None,
        }
        
        if nonempty_record(record):
            records.append(record)
    
    return records

def validate_noaa_dataset(ds):
    """Validate that NOAA dataset has required variables."""
    required_vars = ['swell_1', 'swell_2', 'swell_3', 'swper_1', 'swper_2', 'swper_3',
                     'swdir_1', 'swdir_2', 'swdir_3', 'htsgwsfc', 'windsfc', 'wdirsfc']
    
    missing_vars = []
    for var in required_vars:
        if var not in ds.data_vars:
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"ERROR: Missing required NOAA variables: {missing_vars}")
        return False
    
    logger.info("NOAA dataset validation passed")
    return True
