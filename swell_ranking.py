#!/usr/bin/env python3
"""
Swell ranking and wave energy analysis for Hybrid Surf Database Update Script
Handles dynamic swell prioritization and surf impact calculations
"""

import math
import logging
from utils import safe_float, calculate_wave_energy_kj as util_wave_energy_kj

# Get shared logger
logger = logging.getLogger("surf_update")

def calculate_surf_size_score(height_ft, period_s, direction_deg=None, beach_lat=None, beach_lon=None):
    """
    Calculate surf size score for dynamic swell ranking.
    Based on wave energy and period, similar to Swellnet's algorithm.
    Higher score = more impactful for surfing.
    
    Args:
        height_ft: Wave height in feet
        period_s: Wave period in seconds
        direction_deg: Wave direction in degrees (optional for future directional scoring)
        beach_lat: Beach latitude (optional for future directional scoring)
        beach_lon: Beach longitude (optional for future directional scoring)
    
    Returns:
        float: Surf impact score (higher = more significant for surfing)
    """
    if height_ft is None or period_s is None:
        return 0
    
    try:
        # Base energy calculation (height^2 * period)
        # This gives more weight to longer period swells which are more powerful
        energy_score = (height_ft ** 2) * period_s
        
        # Period bonus - longer periods get exponential bonus
        # Based on the principle that longer period swells have more energy and are better for surfing
        period_bonus = 1.0
        if period_s >= 8:
            period_bonus = 1.2  # 20% bonus for 8+ second periods
        if period_s >= 12:
            period_bonus = 1.5  # 50% bonus for 12+ second periods
        if period_s >= 16:
            period_bonus = 2.0  # 100% bonus for 16+ second periods (groundswell)
            
        # Size bonus - bigger swells get bonus for surfability
        size_bonus = 1.0
        if height_ft >= 2:
            size_bonus = 1.1  # 10% bonus for 2+ foot waves
        if height_ft >= 4:
            size_bonus = 1.3  # 30% bonus for 4+ foot waves
        if height_ft >= 6:
            size_bonus = 1.5  # 50% bonus for 6+ foot waves
            
        # Final surf size score
        surf_score = energy_score * period_bonus * size_bonus
        
        return round(surf_score, 2)
        
    except Exception as e:
        logger.error(f"Error calculating surf size score: {e}")
        return 0

def calculate_wave_energy_kj(wave_height_ft, wave_period_s, direction_deg=None, beach_normal_deg=None):
    """
    Calculate wave energy in kilojoules using deep-water approximation.
    Delegates to utils.calculate_wave_energy_kj (now uses H^2*T^2 with coefficient).
    """
    try:
        return util_wave_energy_kj(wave_height_ft, wave_period_s, direction_deg, beach_normal_deg)
    except Exception as e:
        logger.error(f"Error calculating wave energy: {e}")
        return None

def rank_swell_trains(swell_data_list):
    """
    Dynamically rank swell trains by surf impact score.
    This replaces the static NOAA swell ordering with dynamic ranking based on actual wave conditions.
    
    Args:
        swell_data_list: List of swell dictionaries with height_ft, period_s, direction_deg
    
    Returns:
        list: [primary, secondary, tertiary] swells ordered by surf impact
    """
    if not swell_data_list:
        return [None, None, None]
    
    # Calculate scores for each swell
    scored_swells = []
    for swell in swell_data_list:
        if swell and 'height_ft' in swell and 'period_s' in swell:
            score = calculate_surf_size_score(
                swell['height_ft'], 
                swell['period_s'],
                swell.get('direction_deg'),
                swell.get('beach_lat'),
                swell.get('beach_lon')
            )
            scored_swells.append((score, swell))
    
    # Sort by score (highest first)
    scored_swells.sort(key=lambda x: x[0], reverse=True)
    
    # Extract top 3 swells
    primary = scored_swells[0][1] if len(scored_swells) > 0 else None
    secondary = scored_swells[1][1] if len(scored_swells) > 1 else None
    tertiary = scored_swells[2][1] if len(scored_swells) > 2 else None
    
    return [primary, secondary, tertiary]

def create_swell_train_data(height_m, period_s, direction_deg, beach_lat, beach_lon, source_name):
    """
    Create a standardized swell train data structure.
    
    Args:
        height_m: Wave height in meters
        period_s: Wave period in seconds
        direction_deg: Wave direction in degrees
        beach_lat: Beach latitude
        beach_lon: Beach longitude
        source_name: Name of the swell source (e.g., 'swell_1', 'swell_2')
    
    Returns:
        dict: Standardized swell train data
    """
    height_ft = safe_float(height_m * 3.28084) if height_m is not None else None
    
    return {
        'height_ft': height_ft,
        'period_s': safe_float(period_s),
        'direction_deg': safe_float(direction_deg),
        'beach_lat': beach_lat,
        'beach_lon': beach_lon,
        'source': source_name
    }

def analyze_swell_conditions(primary, secondary, tertiary):
    """
    Analyze overall swell conditions and provide surf quality indicators.
    
    Args:
        primary: Primary swell data dict
        secondary: Secondary swell data dict  
        tertiary: Tertiary swell data dict
    
    Returns:
        dict: Analysis results with quality indicators
    """
    analysis = {
        'total_swells': 0,
        'dominant_period': None,
        'dominant_height': None,
        'has_groundswell': False,
        'has_windswell': False,
        'mixed_swells': False,
        'quality_score': 0
    }
    
    swells = [s for s in [primary, secondary, tertiary] if s and s.get('height_ft')]
    analysis['total_swells'] = len(swells)
    
    if not swells:
        return analysis
    
    # Get dominant swell characteristics
    if primary and primary.get('height_ft'):
        analysis['dominant_height'] = primary['height_ft']
        analysis['dominant_period'] = primary['period_s']
    
    # Classify swell types
    periods = [s['period_s'] for s in swells if s.get('period_s')]
    if periods:
        max_period = max(periods)
        min_period = min(periods)
        
        analysis['has_groundswell'] = max_period >= 12
        analysis['has_windswell'] = min_period < 8
        analysis['mixed_swells'] = len(set([p//4 for p in periods])) > 1  # Different period bands
    
    # Calculate overall quality score
    quality_factors = []
    
    if analysis['has_groundswell']:
        quality_factors.append(2.0)  # Groundswell is high quality
    if analysis['dominant_period'] and analysis['dominant_period'] >= 10:
        quality_factors.append(1.5)  # Good period
    if analysis['dominant_height'] and analysis['dominant_height'] >= 3:
        quality_factors.append(1.2)  # Good size
    if analysis['mixed_swells']:
        quality_factors.append(1.1)  # Multiple swells can be good
    
    analysis['quality_score'] = round(sum(quality_factors), 2) if quality_factors else 0
    
    return analysis

def get_surf_height_range(significant_wave_height_m, swell_height_ft=None):
    """
    Calculate a compact surf height range from significant wave height.
    Returns Surfline-style bands (e.g., 2-3 ft, 3-5 ft) instead of very
    wide ranges.

    Args:
        significant_wave_height_m: Significant wave height in meters
        swell_height_ft: Optional swell height for validation (unused here)

    Returns:
        tuple: (surf_min_ft, surf_max_ft)
    """
    if significant_wave_height_m is None:
        return None, None

    try:
        hs_ft = safe_float(significant_wave_height_m * 3.28084)
        if hs_ft is None:
            return None, None

        # Width buckets by size
        if hs_ft < 3:
            width = 1
        elif hs_ft < 6:
            width = 2
        else:
            width = 3

        band_min = max(0, math.floor(hs_ft - (width / 2.0)))
        band_max = band_min + width

        if band_max <= band_min:
            band_max = band_min + 1

        return float(band_min), float(band_max)

    except Exception as e:
        logger.error(f"Error calculating surf height range: {e}")
        return None, None

def validate_swell_data(swell_dict):
    """
    Validate swell data for completeness and reasonableness.
    
    Args:
        swell_dict: Dictionary containing swell data
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not swell_dict:
        return False, "Empty swell data"
    
    height_ft = swell_dict.get('height_ft')
    period_s = swell_dict.get('period_s')
    
    # Check for required fields
    if height_ft is None and period_s is None:
        return False, "Missing height and period"
    
    # Validate height range (0-50 feet is reasonable)
    if height_ft is not None:
        if height_ft < 0 or height_ft > 50:
            return False, f"Height out of range: {height_ft} feet"
    
    # Validate period range (2-25 seconds is reasonable)
    if period_s is not None:
        if period_s < 2 or period_s > 25:
            return False, f"Period out of range: {period_s} seconds"
    
    # Validate direction if present (0-360 degrees)
    direction = swell_dict.get('direction_deg')
    if direction is not None:
        if direction < 0 or direction > 360:
            return False, f"Direction out of range: {direction} degrees"
    
    return True, "Valid swell data"

def log_swell_ranking_debug(swells_before, swells_after):
    """
    Log debug information about swell ranking changes.
    
    Args:
        swells_before: Original swell order (e.g., [swell_1, swell_2, swell_3])
        swells_after: Ranked swell order [primary, secondary, tertiary]
    """
    if logger.level <= 10:  # DEBUG level
        logger.debug("Swell ranking debug:")
        
        for i, (before, after) in enumerate(zip(swells_before, swells_after)):
            if before and after:
                before_score = calculate_surf_size_score(before.get('height_ft'), before.get('period_s'))
                after_score = calculate_surf_size_score(after.get('height_ft'), after.get('period_s'))
                
                logger.debug(f"  Position {i+1}: {before.get('source', 'unknown')} -> {after.get('source', 'unknown')}")
                logger.debug(f"    Height: {before.get('height_ft', 0):.1f}ft -> {after.get('height_ft', 0):.1f}ft")
                logger.debug(f"    Period: {before.get('period_s', 0):.1f}s -> {after.get('period_s', 0):.1f}s")
                logger.debug(f"    Score: {before_score:.1f} -> {after_score:.1f}")

def get_period_classification(period_s):
    """
    Classify wave period into standard surf forecasting categories.
    
    Args:
        period_s: Wave period in seconds
    
    Returns:
        str: Period classification
    """
    if period_s is None:
        return "Unknown"
    
    if period_s < 6:
        return "Wind Chop"
    elif period_s < 8:
        return "Wind Swell"
    elif period_s < 12:
        return "Mixed Swell"
    elif period_s < 16:
        return "Ground Swell"
    else:
        return "Long Period Ground Swell"

def get_height_classification(height_ft):
    """
    Classify wave height into standard surf size categories.
    
    Args:
        height_ft: Wave height in feet
    
    Returns:
        str: Height classification
    """
    if height_ft is None:
        return "Unknown"
    
    if height_ft < 1:
        return "Flat"
    elif height_ft < 2:
        return "Small"
    elif height_ft < 4:
        return "Knee to Waist"
    elif height_ft < 6:
        return "Waist to Chest"
    elif height_ft < 8:
        return "Chest to Head"
    elif height_ft < 12:
        return "Head High Plus"
    else:
        return "Overhead"
