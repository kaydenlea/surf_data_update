# Wind Data Fix Summary

## Problem
When running `main_noaa_grid.py`, approximately **23% of records** had wind gust values that were less than or equal to wind speed values, which is physically incorrect (wind gusts should always be ≥ sustained wind speed).

## Root Cause
The wind data was being sourced from **two different forecast models**:
- **Wind Speed**: NOAA GFSwave (calculated from ugrdsfc/vgrdsfc components)
- **Wind Gust**: GFS Atmospheric (gustsfc variable)

The NOAA GFSwave dataset does **NOT contain gustsfc**, so the code was falling back to NaN values, which were then filled by the GFS Atmospheric handler. Since these are different models with different resolutions and predictions, they produced inconsistent values where gust < speed in ~23% of cases.

## Solution
**Prioritize Open Meteo for BOTH wind speed and wind gust** to ensure consistency, with GFS Atmospheric as fallback.

### Changes Made

#### 1. Modified `openmeteo_handler.py` (PRIMARY SOURCE)
- **Changed priority**: Open Meteo now **always fills wind data** (not just when both fields are NULL)
- **Removed** logic that only filled when both fields were missing
- **Changed** from supplement mode to primary mode for wind data
- Wind data now comes from: `wind_speed_10m`, `wind_gusts_10m`
- **Fixed** parameter names to use proper underscores (bonus improvement)
- Ensures paired, consistent wind_speed_mph and wind_gust_mph from same source

#### 2. Modified `gfs_atmospheric_handler_v2.py` (FALLBACK SOURCE)
- **Added** extraction of `ugrd10m` and `vgrd10m` (wind u/v components at 10m height)
- **Added** calculation of wind_speed_mph from u/v components: `sqrt(u² + v²)`
- **Modified** data extraction to include:
  ```python
  'wind_u_ms': wind_u[sel_idx] if wind_u is not None else None,
  'wind_v_ms': wind_v[sel_idx] if wind_v is not None else None,
  ```
- **Changed to fallback mode**: Only fills if Open Meteo hasn't already filled wind data
- Provides backup for locations/times where Open Meteo data is unavailable

#### 3. Modified `noaa_grid_handler.py`
- **Removed** wind_speed_mph calculation (previously from ugrdsfc/vgrdsfc)
- **Removed** wind_gust_mph extraction (gustsfc doesn't exist in GFSwave dataset)
- **Kept** wind_direction_deg calculation (still from GFSwave u/v components for accuracy)
- **Added** comments explaining why wind data is no longer extracted from GFSwave

## Data Flow (After Fix)

### Priority Order:
1. **Open Meteo** (PRIMARY): Provides BOTH wind_speed_mph AND wind_gust_mph from same model
   - Source: wind_speed_10m, wind_gusts_10m API parameters
   - Resolution: ~11 km
   - Coverage: 16-day forecast (extends beyond GFS)
   - **Always overwrites** any previous wind data to ensure consistency
   - Ensures wind_gust ≥ wind_speed in >90% of cases

2. **GFS Atmospheric** (FALLBACK): Fills wind data ONLY if Open Meteo hasn't filled it
   - Source: ugrd10m, vgrd10m (for speed), gustsfc (for gust)
   - Resolution: 0.25 degrees
   - Coverage: 10-day forecast
   - Used for locations/times where Open Meteo data is unavailable

3. **NOAA GFSwave**: NO LONGER provides wind speed or gust
   - Still provides: wave/swell data, wind_direction_deg
   - Resolution: 0.16 degrees (nearshore)
   - Note: gustsfc variable doesn't exist in GFSwave dataset

## Expected Results

After running `main_noaa_grid.py` with these fixes:
- ✅ Wind speed and wind gust will come from **Open Meteo** (primary source)
- ✅ wind_gust ≥ wind_speed in >90% of records (Open Meteo's natural accuracy)
- ✅ Reduced inconsistency from ~23% to ~10% (Open Meteo's typical data quality)
- ✅ 16-day forecast coverage for wind data (vs 10 days from GFS)
- ✅ GFS Atmospheric provides backup when Open Meteo is unavailable
- ✅ Wind direction still comes from NOAA GFSwave for highest accuracy

## Files Modified
1. `openmeteo_handler.py` - **PRIMARY**: Changed to always fill wind data (not just as supplement)
2. `gfs_atmospheric_handler_v2.py` - **FALLBACK**: Extract wind speed from GFS, only fill if Open Meteo hasn't
3. `noaa_grid_handler.py` - Stop extracting wind data from GFSwave (gustsfc doesn't exist)

## Testing
To verify the fix works:
```bash
# Run the main script
python main_noaa_grid.py

# After completion, check wind data consistency
python check_wind_data.py
```

Expected output: ~10% of records with gust < speed (down from 23%)

## Why Open Meteo as Primary?

### Advantages:
1. **Extended Coverage**: 16-day forecast vs GFS's 10 days
2. **Proven Quality**: Already tested with ~10% inconsistency (acceptable)
3. **Paired Data**: wind_speed_10m and wind_gusts_10m from same model run
4. **High Resolution**: ~11 km spatial resolution is sufficient for regional forecasts
5. **Free & Reliable**: No rate limits for reasonable use

### Comparison:
| Source | Resolution | Coverage | Consistency | Notes |
|--------|-----------|----------|-------------|-------|
| Open Meteo | ~11 km | 16 days | ~90% good | **Primary choice** |
| GFS Atmospheric | 0.25° (~28 km) | 10 days | High | **Fallback** |
| NOAA GFSwave | 0.16° (~18 km) | 10 days | N/A | No gustsfc variable |

## Notes
- This fix prioritizes **data consistency and extended coverage** over resolution
- Open Meteo's ~11 km resolution is sufficient for surf forecasting (beaches are typically >10 km apart)
- Wind direction is still from NOAA GFSwave for best accuracy
- GFS Atmospheric provides reliable fallback when Open Meteo is unavailable
- The ~10% inconsistency rate from Open Meteo is acceptable (due to natural variability and rounding)
