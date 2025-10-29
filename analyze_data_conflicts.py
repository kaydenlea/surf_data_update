#!/usr/bin/env python3
"""
Analyze which data sources provide which fields and identify conflicts/overrides
"""

import sys
from config import logger

def analyze_data_source_conflicts():
    """Analyze which fields are provided by each data source and identify conflicts."""

    logger.info("=" * 80)
    logger.info("DATA SOURCE FIELD ANALYSIS - What Gets Overwritten?")
    logger.info("=" * 80)
    logger.info("")

    # Define fields provided by each data source in your pipeline
    data_sources = {
        "1. NOAA GFSwave": {
            "order": 1,
            "fields": {
                "primary_swell_height_ft": "✓ UNIQUE (only source)",
                "primary_swell_period_s": "✓ UNIQUE (only source)",
                "primary_swell_direction": "✓ UNIQUE (only source)",
                "secondary_swell_height_ft": "✓ UNIQUE (only source)",
                "secondary_swell_period_s": "✓ UNIQUE (only source)",
                "secondary_swell_direction": "✓ UNIQUE (only source)",
                "tertiary_swell_height_ft": "✓ UNIQUE (only source)",
                "tertiary_swell_period_s": "✓ UNIQUE (only source)",
                "tertiary_swell_direction": "✓ UNIQUE (only source)",
                "surf_height_min_ft": "✓ UNIQUE (only source)",
                "surf_height_max_ft": "✓ UNIQUE (only source)",
                "wave_energy_kj": "✓ UNIQUE (only source)",
                "wind_speed_mph": "⚠ CONFLICT (will be overwritten by Open-Meteo)",
                "wind_direction_deg": "⚠ CONFLICT (will be overwritten by Open-Meteo)",
                "wind_gust_mph": "⚠ CONFLICT (will be overwritten by Open-Meteo)",
            }
        },
        "2. Open-Meteo": {
            "order": 2,
            "fields": {
                "temperature": "⚠ CONFLICT (will be overwritten by GFS Atmospheric)",
                "weather": "⚠ CONFLICT (will be overwritten by GFS Atmospheric)",
                "wind_speed_mph": "⚠ OVERWRITES NOAA wind_speed_mph",
                "wind_direction_deg": "⚠ OVERWRITES NOAA wind_direction_deg",
                "wind_gust_mph": "⚠ OVERWRITES NOAA wind_gust_mph",
                "pressure_inhg": "⚠ CONFLICT (will be overwritten by GFS Atmospheric)",
            }
        },
        "3. GFS Atmospheric": {
            "order": 3,
            "fields": {
                "temperature": "⚠ OVERWRITES Open-Meteo temperature",
                "weather": "⚠ OVERWRITES Open-Meteo weather",
                "pressure_inhg": "⚠ OVERWRITES Open-Meteo pressure_inhg",
                # Note: Wind fields disabled in your config
            }
        },
        "4. NOAA CO-OPS Tides": {
            "order": 4,
            "fields": {
                "tide_level_ft": "✓ UNIQUE (only source)",
                "water_temp_f": "✓ UNIQUE (only source)",
            }
        },
    }

    # Print detailed breakdown
    for source_name in sorted(data_sources.keys()):
        source = data_sources[source_name]
        logger.info(f"{source_name} (runs {['first', 'second', 'third', 'fourth'][source['order']-1]})")
        logger.info("-" * 80)
        for field, status in source["fields"].items():
            logger.info(f"  {field:35s} {status}")
        logger.info("")

    # Analyze conflicts
    logger.info("=" * 80)
    logger.info("CONFLICT ANALYSIS - Field Override Chain")
    logger.info("=" * 80)
    logger.info("")

    conflicts = {
        "wind_speed_mph": [
            "1. NOAA GFSwave provides (ocean wind, 25km resolution)",
            "2. Open-Meteo OVERWRITES (blended atmospheric model)",
            "FINAL: Open-Meteo value (more accurate atmospheric wind)",
        ],
        "wind_direction_deg": [
            "1. NOAA GFSwave provides (ocean wind direction)",
            "2. Open-Meteo OVERWRITES (atmospheric wind direction)",
            "FINAL: Open-Meteo value",
        ],
        "wind_gust_mph": [
            "1. NOAA GFSwave provides (estimated or from gustsfc)",
            "2. Open-Meteo OVERWRITES (atmospheric gust data)",
            "FINAL: Open-Meteo value",
        ],
        "temperature": [
            "1. Open-Meteo provides (blended model)",
            "2. GFS Atmospheric OVERWRITES (NOAA GFS 25km)",
            "FINAL: GFS Atmospheric value",
        ],
        "weather": [
            "1. Open-Meteo provides (WMO weather code)",
            "2. GFS Atmospheric OVERWRITES (derived from GFS cloud/precip)",
            "FINAL: GFS Atmospheric value",
        ],
        "pressure_inhg": [
            "1. Open-Meteo provides (blended model)",
            "2. GFS Atmospheric OVERWRITES (NOAA GFS surface pressure)",
            "FINAL: GFS Atmospheric value",
        ],
    }

    for field, chain in conflicts.items():
        logger.info(f"Field: {field}")
        for step in chain:
            logger.info(f"  {step}")
        logger.info("")

    # Summary
    logger.info("=" * 80)
    logger.info("SUMMARY - Final Data Sources After Deduplication")
    logger.info("=" * 80)
    logger.info("")

    final_sources = {
        "NOAA GFSwave (Ocean/Wave Data)": [
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
        ],
        "Open-Meteo (Wind Data)": [
            "wind_speed_mph",
            "wind_direction_deg",
            "wind_gust_mph",
        ],
        "GFS Atmospheric (Weather Data)": [
            "temperature",
            "weather",
            "pressure_inhg",
        ],
        "NOAA CO-OPS Tides (Tide Data)": [
            "tide_level_ft",
            "water_temp_f",
        ],
    }

    for source, fields in final_sources.items():
        logger.info(f"{source}:")
        logger.info(f"  {len(fields)} fields")
        for field in fields:
            logger.info(f"    - {field}")
        logger.info("")

    # Impact analysis
    logger.info("=" * 80)
    logger.info("IMPACT ANALYSIS - Should You Care?")
    logger.info("=" * 80)
    logger.info("")

    impacts = {
        "Wind Overwrite (NOAA → Open-Meteo)": {
            "status": "✓ GOOD",
            "reason": "Open-Meteo blends HRRR (3km) + GFS atmospheric models, more accurate than NOAA GFSwave ocean winds",
            "recommendation": "Keep current behavior - Open-Meteo wind is better for surf forecasting",
        },
        "Temperature Overwrite (Open-Meteo → GFS)": {
            "status": "⚠ NEUTRAL",
            "reason": "Both sources are similar quality. GFS is pure NOAA, Open-Meteo blends multiple models",
            "recommendation": "Either is fine. Current behavior (GFS wins) ensures consistency with other NOAA data",
        },
        "Weather Overwrite (Open-Meteo → GFS)": {
            "status": "⚠ NEUTRAL",
            "reason": "GFS derives from cloud cover + precip, Open-Meteo provides processed codes. Similar quality",
            "recommendation": "Current behavior is fine",
        },
        "Pressure Overwrite (Open-Meteo → GFS)": {
            "status": "⚠ NEUTRAL",
            "reason": "Minimal difference between sources for pressure",
            "recommendation": "Current behavior is fine",
        },
    }

    for scenario, info in impacts.items():
        logger.info(f"{scenario}")
        logger.info(f"  Status:         {info['status']}")
        logger.info(f"  Reason:         {info['reason']}")
        logger.info(f"  Recommendation: {info['recommendation']}")
        logger.info("")

    # Potential improvements
    logger.info("=" * 80)
    logger.info("POTENTIAL IMPROVEMENTS (Optional)")
    logger.info("=" * 80)
    logger.info("")

    improvements = [
        {
            "title": "Option 1: Keep NOAA Wind for Coastal Accuracy",
            "description": "NOAA GFSwave wind is specifically for ocean/coastal, might be better than Open-Meteo for surf conditions",
            "action": "Reverse order: Run Open-Meteo before NOAA, so NOAA wind overwrites Open-Meteo",
            "benefit": "Coastal-specific wind data",
            "drawback": "NOAA is 25km resolution vs Open-Meteo's HRRR 3km",
        },
        {
            "title": "Option 2: Use Open-Meteo for Everything Atmospheric",
            "description": "Open-Meteo already blends HRRR+GFS+ECMWF, might be better than pure GFS",
            "action": "Disable GFS Atmospheric handler, let Open-Meteo be final atmospheric source",
            "benefit": "Single, consistent atmospheric source with best model blending",
            "drawback": "Lose direct NOAA GFS data",
        },
        {
            "title": "Option 3: Current Setup (Recommended)",
            "description": "Keep current behavior - it's already well-optimized",
            "action": "No changes needed",
            "benefit": "Good balance of sources, each provides what they're best at",
            "drawback": "Some minor overwrites, but minimal impact",
        },
    ]

    for i, opt in enumerate(improvements, 1):
        logger.info(f"{opt['title']}")
        logger.info(f"  Description: {opt['description']}")
        logger.info(f"  Action:      {opt['action']}")
        logger.info(f"  Benefit:     {opt['benefit']}")
        logger.info(f"  Drawback:    {opt['drawback']}")
        logger.info("")

    return True


if __name__ == "__main__":
    try:
        analyze_data_source_conflicts()
        sys.exit(0)
    except Exception as e:
        logger.error(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
