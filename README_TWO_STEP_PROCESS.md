# Two-Step Surf Data Update Process

The surf data update has been split into two independent scripts that can be run separately:

## Overview

### Step 1: GFS Wave Data Fetch (`step1_gfs_fetch.py`)
- Fetches NOAA GFSwave data (primary swell/wave/wind forecasts)
- Takes the longest time due to NOAA rate limiting
- Populates base forecast data in database

### Step 2: Data Enhancement (`step2_enhance_data.py`)
- Enhances existing forecast data with supplemental sources
- Adds weather, temperature, tides, water temp, sun/moon data
- Faster than Step 1

## Usage

### Run Both Steps (Complete Update)

```bash
# Step 1: Fetch GFS wave data (takes ~60-90 minutes)
python step1_gfs_fetch.py

# Step 2: Enhance with weather/tides/astronomy (takes ~30-40 minutes)
python step2_enhance_data.py
```

### Run Only Step 1 (GFS Data Only)

If you only want to update the core wave/swell forecasts:

```bash
python step1_gfs_fetch.py
```

This will give you:
- Primary/secondary/tertiary swell (height, period, direction)
- Surf height range
- Wave energy
- Wind direction and gust

### Run Only Step 2 (Enhancement Only)

If you already have GFS data and only want to update weather/tides:

```bash
python step2_enhance_data.py
```

**Prerequisites:** Step 1 must have been run at least once to populate base forecast data.

This will add/update:
- Weather conditions
- Air temperature
- Wind speed (supplemented)
- Atmospheric pressure
- Tide levels
- Water temperature
- Sunrise/sunset times
- Moonrise/moonset times
- Moon phase

## Advantages of Two-Step Process

### 1. **Flexibility**
- Run GFS updates less frequently (e.g., once per day)
- Run enhancements more frequently (e.g., every 6 hours) for updated weather/tides

### 2. **Faster Updates**
- If GFS data is already fresh, skip Step 1 and only run Step 2
- Reduces total update time by 60-70% when only weather/tides need updating

### 3. **Better Error Recovery**
- If Step 1 fails, you can fix it and retry without re-running enhancements
- If Step 2 fails, GFS data is already saved and you can retry enhancement

### 4. **Development & Testing**
- Test weather/tide integration separately without waiting for GFS fetch
- Develop new enhancement features without touching GFS logic

## Data Sources by Step

### Step 1 Data Sources (Public Domain)
- **NOAA GFSwave** - Wave/swell/surf/wind forecasts

### Step 2 Data Sources (Public Domain)
- **NOAA NWS** - Weather/temperature/wind/pressure
- **NOAA CO-OPS** - Tides/water temperature
- **USNO** - Sun/moon astronomical data

## Example Workflows

### Daily Full Update (Recommended)
```bash
# Run once per day for complete data refresh
python step1_gfs_fetch.py && python step2_enhance_data.py
```

### Quick Weather/Tide Refresh
```bash
# Run multiple times per day for latest weather/tides (GFS data already current)
python step2_enhance_data.py
```

### GFS-Only Update (Development)
```bash
# Test GFS improvements without re-running enhancements
python step1_gfs_fetch.py
```

## Logging

Each script creates its own log entries in `surf_update_hybrid.log`:
- Step 1 logs will show "STEP 1: NOAA GFS WAVE DATA FETCH"
- Step 2 logs will show "STEP 2: DATA ENHANCEMENT - NWS/CO-OPS/USNO"

## Exit Codes

Both scripts use standard exit codes:
- `0` - Success
- `1` - Completed with errors
- `2` - Fatal error

## Original Combined Script

The original `main_noaa.py` still exists and runs both steps together in one execution:

```bash
python main_noaa.py
```

Use this if you prefer the all-in-one approach.

## Timing Estimates

Based on 1,336 beaches and 189 location groups:

| Script | Approximate Time |
|--------|-----------------|
| Step 1 (GFS fetch) | 60-90 minutes |
| Step 2 (Enhancement) | 30-40 minutes |
| Total (both steps) | 90-130 minutes |

*Note: Times vary based on NOAA server response times and network conditions.*

## Troubleshooting

### "No existing forecast data found"
**Problem:** Step 2 can't find any forecast data in database.
**Solution:** Run Step 1 first to populate GFS wave data.

### "No valid ocean point for location"
**Problem:** Some beaches can't find NOAA grid data.
**Solution:** This is expected for ~5-10% of beaches in harbors/bays. The fallback system will copy data from nearest neighbor beaches.

### Rate Limit Errors
**Problem:** NOAA blocking requests temporarily.
**Solution:** Wait 1 hour and retry. The scripts have built-in rate limiting to prevent this.

## Questions?

- Both scripts have detailed logging and progress indicators
- Check `surf_update_hybrid.log` for detailed execution logs
- Each script shows a summary on completion with data sources used
