# Understanding Null Fields and Data Coverage

## Overview

After running your data update scripts, you may still see some null values in certain fields. This is **expected** for certain data types due to limited availability from source APIs. This document explains which fields may have nulls and what to do about them.

## Fields That Should NEVER Be Null (After Fill)

These fields are provided by NOAA GFSwave and should have 100% coverage:

- ✅ `primary_swell_height_ft`, `primary_swell_period_s`, `primary_swell_direction`
- ✅ `secondary_swell_height_ft`, `secondary_swell_period_s`, `secondary_swell_direction`
- ✅ `tertiary_swell_height_ft`, `tertiary_swell_period_s`, `tertiary_swell_direction`
- ✅ `surf_height_min_ft`, `surf_height_max_ft`
- ✅ `wave_energy_kj`
- ✅ `wind_speed_mph` - **FIXED** - Now populated from GFS data
- ✅ `wind_direction_deg`, `wind_gust_mph`

**If these are null:** Run `fill_neighbors.py` to fill from nearest beaches.

## Fields That May Have Some Nulls

### 1. `temperature` (Air Temperature)

**Source:** NOAA NWS
**Expected Coverage:** 90-95% of beaches
**Why some nulls:**
- NWS grid system doesn't cover some offshore points
- Very remote beaches in Northern California may lack coverage
- Bays/harbors sometimes outside NWS coverage area

**Solution:**
```bash
python fill_neighbors.py --hours-back 24
```
This will copy temperature from nearest beach with data.

### 2. `water_temp_f` (Water Temperature)

**Source:** NOAA CO-OPS Tide Stations
**Expected Coverage:** 50-70% of beaches
**Why many nulls:**
- Only **14 tide stations** in California have water temp sensors
- Sensors at: San Diego, La Jolla, Los Angeles, Long Beach, Santa Barbara, Monterey, Santa Cruz, San Francisco, Point Reyes, Arena Cove, Crescent City
- Most beaches are far from these stations (200km+ max distance)
- Water temperature varies significantly by location, so long-distance filling is less accurate

**Solution:**
```bash
python fill_neighbors.py --hours-back 24
```
This will fill from nearest station data, but accuracy decreases with distance.

**Alternative:** Accept that water_temp_f will be null for many beaches. Consider using a fallback in your application (e.g., seasonal average, or just show "N/A").

### 3. `weather` (Weather Description)

**Source:** NOAA NWS
**Expected Coverage:** 90-95% of beaches
**Why some nulls:** Same as temperature - limited NWS grid coverage

**Solution:** Run `fill_neighbors.py` to fill from nearest beaches.

### 4. `tide_level_ft` (Tide Level)

**Source:** NOAA CO-OPS Tide Stations
**Expected Coverage:** 100% of beaches (within 200km of 14 stations)
**Why might have nulls:** Rare, usually only if API is down

**Solution:** Run `fill_neighbors.py` if any nulls appear.

### 5. `pressure_inhg` (Atmospheric Pressure)

**Source:** NOAA NWS
**Expected Coverage:** 90-95% of beaches
**Why some nulls:** Same as temperature - limited NWS grid coverage

**Solution:** Run `fill_neighbors.py` to fill from nearest beaches.

## Recommended Workflow to Minimize Nulls

### Step 1: Run Data Update Scripts

```bash
# Option A: Two-step process
python step1_gfs_fetch.py
python step2_enhance_data.py

# Option B: Combined script
python main_noaa.py
```

**Expected nulls after this step:**
- `temperature`: 5-10% of records
- `weather`: 5-10% of records
- `water_temp_f`: 30-50% of records
- `pressure_inhg`: 5-10% of records

### Step 2: Fill Remaining Nulls (Fast)

```bash
python fill_neighbors.py --hours-back 24 --verbose
```

This fills most remaining nulls by copying from nearest beaches.

**Expected nulls after this step:**
- `temperature`: <1% of records
- `weather`: <1% of records
- `water_temp_f`: 20-30% of records (still many, this is normal)
- `pressure_inhg`: <1% of records

### Step 3: Final Cleanup (Slow but Thorough)

```bash
python fill_neighbors_slow.py
```

Processes each null individually for guaranteed maximum fill rate.

**Expected nulls after this step:**
- `temperature`: 0% (unless ALL beaches at a timestamp lack data)
- `weather`: 0%
- `water_temp_f`: 15-25% (this is as good as it gets)
- `pressure_inhg`: 0%

## Why Water Temperature Stays Null

Water temperature is the **hardest field to fill** because:

1. **Limited sensors:** Only 14 stations out of 1,336 beaches
2. **Geographic variance:** Water temp changes significantly over short distances
3. **Coastal vs offshore:** Bay beaches vs ocean beaches have very different water temps
4. **Upwelling effects:** Northern California upwelling creates localized cold spots

**Example:**
- San Francisco Bay beaches: ~60°F
- Ocean Beach (just outside bay): ~55°F
- Half Moon Bay (20 miles south): ~58°F

Copying water temp from 50+ miles away would be misleading, so many beaches will simply not have this data.

## Application-Level Handling

### Recommended Approach

```javascript
// In your app/API
function getWaterTemp(beach) {
  if (beach.water_temp_f !== null) {
    return `${beach.water_temp_f}°F`;
  }

  // Fallback based on season and region
  const season = getCurrentSeason();
  const region = beach.county;

  return getSeasonalAverage(region, season) + " (estimated)";
}
```

### Seasonal Averages (California)

Use these as fallbacks when `water_temp_f` is null:

| Region | Winter | Spring | Summer | Fall |
|--------|--------|--------|--------|------|
| San Diego | 57°F | 60°F | 68°F | 65°F |
| Orange County | 56°F | 58°F | 66°F | 62°F |
| Los Angeles | 55°F | 57°F | 65°F | 61°F |
| Santa Barbara | 54°F | 56°F | 63°F | 59°F |
| Central Coast | 52°F | 53°F | 58°F | 56°F |
| San Francisco | 52°F | 52°F | 56°F | 55°F |
| North Coast | 50°F | 51°F | 55°F | 53°F |

## Data Source Limitations Summary

| Field | Source | Coverage | Null After Fill | Notes |
|-------|--------|----------|----------------|-------|
| Swell data | NOAA GFS | 100% | 0% | Always available |
| Wind speed/dir | NOAA GFS | 100% | 0% | Now using GFS data |
| Temperature | NWS | 90-95% | 0-1% | Fill from neighbors |
| Weather | NWS | 90-95% | 0-1% | Fill from neighbors |
| Pressure | NWS | 90-95% | 0-1% | Fill from neighbors |
| Tide level | CO-OPS | 100% | 0% | 14 stations cover all |
| Water temp | CO-OPS | 50-70% | 15-25% | **Limited sensors** |

## Troubleshooting

### "I still have nulls in primary_swell_height_ft"

**Problem:** Core GFS data missing
**Solution:**
1. Check if NOAA GFS ran successfully (check logs for errors)
2. Run `fill_neighbors.py` to fill from nearby beaches
3. If still null, check if beach coordinates are valid

### "All beaches at one timestamp have null temperature"

**Problem:** NWS API might have been down during that hour
**Solution:** Re-run `step2_enhance_data.py` to retry NWS data fetch for that time period

### "30%+ of records have null water_temp_f"

**Status:** **This is normal!** Water temperature has limited sensor coverage.
**Solution:** Use seasonal averages as fallback in your application.

### "fill_neighbors.py doesn't fill water_temp_f"

**Why:** It does fill, but if no nearby beaches (within reasonable distance) have water_temp data, it can't fill. Many California beaches are 100+ miles from the nearest water temp sensor.

**Solution:** Accept this limitation or implement seasonal fallback in your application.

## Summary

✅ **Wind speed is now fixed** - Uses GFS data (was previously null)
✅ **Temperature** - 95%+ coverage after neighbor fill
✅ **Weather** - 95%+ coverage after neighbor fill
⚠️ **Water temp** - 50-70% coverage (this is the maximum possible with current data sources)

**Bottom line:** After running your update scripts + `fill_neighbors.py`, the only field that should consistently have nulls is `water_temp_f`, and that's expected due to limited NOAA sensor coverage.
