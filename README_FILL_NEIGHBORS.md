# Fill Neighbors - Auto-Detection for All Null Columns

## Overview

The fill neighbors scripts have been **IMPROVED** to automatically detect and fill **ALL null columns** in the `forecast_data` table, not just a hardcoded list.

## What Changed

### Before
- Only filled a small hardcoded list of fields:
  - `weather`, `wind_direction_deg`
  - `secondary_swell_height_ft`, `secondary_swell_period_s`, `secondary_swell_direction`
  - `tertiary_swell_height_ft`, `tertiary_swell_period_s`, `tertiary_swell_direction`

### After
- **Auto-detects ALL columns** in the `forecast_data` table
- Fills **every field** that has null values, including:
  - All swell data (primary, secondary, tertiary)
  - All surf metrics (height min/max, wave energy)
  - All weather data (temperature, weather, pressure)
  - All wind data (speed, direction, gust)
  - All water data (water temp, tide level)
  - **Any future columns you add to the table**

## Two Scripts Available

### 1. `fill_neighbors.py` - Fast Batch Processing

**Best for:** Normal use, handles most cases efficiently

```bash
# Fill all null values from the last 24 hours
python fill_neighbors.py --hours-back 24

# Fill all records (entire table)
python fill_neighbors.py

# Dry run to see what would be filled
python fill_neighbors.py --dry-run --verbose

# Custom time window
python fill_neighbors.py --start-iso "2025-10-19T00:00:00"
```

**Features:**
- Fast in-memory batch processing
- Processes all fields in one pass
- Time bucketing (hourly by default)
- Fallback to nearby time buckets if no donors found
- Updates database in batches

**Options:**
- `--hours-back N` - Process last N hours only
- `--time-fallback N` - Search ±N hour buckets for donors (default: 6)
- `--batch-size N` - Rows per database update (default: 100)
- `--dry-run` - Preview changes without writing
- `--verbose` - Show detailed progress
- `--fields field1 field2` - Override auto-detection (process specific fields only)

### 2. `fill_neighbors_slow.py` - Guaranteed 100% Fill

**Best for:** Final cleanup pass after fast script, ensures zero nulls

```bash
# Fill remaining nulls one-by-one
python fill_neighbors_slow.py
```

**Features:**
- Processes each null individually
- Writes immediately after each fill
- Newly filled values become donors instantly
- Guarantees 100% fill rate (if any donors exist)
- Slower but more thorough

**When to use:**
- After running `fill_neighbors.py` to catch any remaining nulls
- When you need guaranteed complete coverage
- For final cleanup before deploying data

## Auto-Detection Logic

Both scripts now:

1. **Query the database** to get all column names from `forecast_data`
2. **Exclude system columns:**
   - `id`, `beach_id`, `timestamp`
   - `created_at`, `updated_at`
3. **Fill everything else** that has null values

**This means:** If you add new columns to `forecast_data` in the future, these scripts will automatically fill them without code changes!

## Typical Workflow

```bash
# Step 1: Run your data update
python step1_gfs_fetch.py
python step2_enhance_data.py

# Step 2: Fill nulls using fast batch processing
python fill_neighbors.py --hours-back 24 --verbose

# Step 3: Final cleanup for any remaining nulls
python fill_neighbors_slow.py
```

## How It Works

### Nearest Neighbor Algorithm

For each null value:
1. Finds all beaches with non-null values at the same timestamp
2. Calculates distance to each donor beach (haversine formula)
3. Copies value from the nearest beach
4. If no donors at exact timestamp, searches nearby time buckets (±6 hours by default)

### Example

```
Beach A (San Francisco Bay) has null wind_speed at 3pm
  → Beach B (Ocean Beach, 5km away) has wind_speed: 15 mph
  → Beach A gets filled with 15 mph
  → Beach C (Golden Gate, 2km away) has null wind_speed
  → Beach C gets filled with 15 mph (from A or B, whichever is closer)
```

This propagates values from beaches with data to those without, ensuring comprehensive coverage.

## Performance

### Fast Script (`fill_neighbors.py`)
- **1,336 beaches, 76,000 records:** ~30-60 seconds
- **All fields processed in parallel**
- **Memory-efficient:** processes in batches

### Slow Script (`fill_neighbors_slow.py`)
- **Per-field processing:** 1-5 minutes per field
- **Total time:** 10-30 minutes for all fields
- **Guaranteed completion:** fills every possible null

## Field Coverage

Auto-detected fields typically include:

**Swell Data:**
- `primary_swell_height_ft`, `primary_swell_period_s`, `primary_swell_direction`
- `secondary_swell_height_ft`, `secondary_swell_period_s`, `secondary_swell_direction`
- `tertiary_swell_height_ft`, `tertiary_swell_period_s`, `tertiary_swell_direction`

**Surf Metrics:**
- `surf_height_min_ft`, `surf_height_max_ft`
- `wave_energy_kj`

**Wind Data:**
- `wind_speed_mph`, `wind_direction_deg`, `wind_gust_mph`

**Weather Data:**
- `temperature`, `weather`, `pressure_inhg`

**Water Data:**
- `water_temp_f`, `tide_level_ft`

**And any future columns you add!**

## Logging

Both scripts show detailed progress:

```
================================================================================
NEIGHBOR FILL - AUTO-DETECTS ALL COLUMNS
================================================================================
Processing 20 fields: pressure_inhg, primary_swell_direction, primary_swell_height_ft, ...
================================================================================
Post-fill: evaluating 76152 forecast rows
Post-fill: field fills -> primary_swell_height_ft: 1250, temperature: 3420, ...
Post-fill: updated 5430 forecast rows with neighbor data
```

## Troubleshooting

### "No donors found"
- Normal for very recent data where few beaches have values yet
- Run enhancement scripts first to populate data
- Increase `--time-fallback` to search wider time window

### "Could not auto-detect fields"
- Fallback list will be used automatically
- Check database connection
- Ensure `forecast_data` table has at least one record

### Still have nulls after both scripts
- Possible if NO beaches have data for that field at any timestamp
- Check your data sources are providing the field
- Some fields (like tide data) may not be available for all locations

## Integration with Data Update

You can run these scripts:

1. **Manually** - After each data update
2. **Scheduled** - Via cron/scheduler after main update scripts
3. **On-demand** - When you notice missing data

Recommended: Run `fill_neighbors.py` after every data update to ensure complete coverage.
