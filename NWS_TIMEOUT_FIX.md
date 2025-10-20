# NWS Timeout Fix

## Problem
When running `main_noaa.py` with all 1336 beaches, the NWS handler was timing out:
```
ERROR - NWS: Failed to fetch hourly forecast: HTTPSConnectionPool(host='api.weather.gov', port=443): Read timed out. (read timeout=15)
```

## Root Cause
The NWS handler was making **2 API calls per beach**:
1. Grid point lookup (`/points/{lat},{lon}`)
2. Hourly forecast fetch (`/gridpoints/{office}/{gridX},{gridY}/forecast/hourly`)

With 1336 beaches:
- Total API calls: **2,672 calls**
- Time at 0.2s delay: **~9 minutes** minimum
- Timeouts: Individual requests timing out at 15 seconds

## Solutions Implemented

### 1. **Increased Timeout** (15s → 30s)
```python
def get_nws_gridpoint(lat: float, lon: float, timeout: int = 30)
def get_nws_hourly_forecast(forecast_url: str, timeout: int = 30)
```

Gives NWS API more time to respond during high load.

### 2. **Grid Point Caching**
```python
cache_key = f"{round(lat, 2)},{round(lon, 2)}"  # ~5.5km radius

if cache_key in gridpoint_cache:
    grid_info = gridpoint_cache[cache_key]
else:
    grid_info = get_nws_gridpoint(lat, lon)
    gridpoint_cache[cache_key] = grid_info
```

**Benefits:**
- Beaches within ~5.5km share the same grid point
- Reduces redundant API calls for clustered beaches
- Expected reduction: **50-80%** for California beaches

### 3. **Forecast Caching**
```python
if cache_key in forecast_cache:
    periods = forecast_cache[cache_key]
else:
    periods = get_nws_hourly_forecast(grid_info["forecastHourly"])
    forecast_cache[cache_key] = periods
```

**Benefits:**
- Same forecast data used for nearby beaches
- Eliminates redundant hourly forecast fetches
- Expected reduction: **50-80%** for California beaches

### 4. **Better Error Handling**
```python
except requests.exceptions.Timeout:
    logger.debug(f"   NWS: Timeout getting gridpoint for {lat},{lon}")
    return None
```

- Changed from `logger.error` → `logger.debug` for non-critical errors
- Timeouts no longer clutter logs
- Script continues gracefully

### 5. **Enhanced Logging**
```python
logger.info(f"   NWS stats: {success_count} beaches with data, {no_coverage_count} no coverage, {timeout_count} timeouts")
logger.info(f"   NWS cache: {len(gridpoint_cache)} unique gridpoints, {len(forecast_cache)} unique forecasts")
```

Provides visibility into caching effectiveness.

## Expected Performance Improvement

### Before Optimization:
- **API Calls**: 2,672 (1336 × 2)
- **Time**: ~9-15 minutes
- **Timeout Risk**: HIGH

### After Optimization:
- **API Calls**: ~400-800 (depending on beach clustering)
- **Time**: ~2-4 minutes
- **Timeout Risk**: LOW

**Speed up**: **3-5x faster** 🚀

## Why Caching Works

California beaches are clustered along the coast:
- **San Diego County**: ~150 beaches in tight clusters
- **Orange County**: ~50 beaches close together
- **Los Angeles County**: ~100 beaches in dense areas
- **Ventura County**: ~40 beaches along coastline
- **Santa Barbara County**: ~60 beaches clustered
- **Etc.**

With 0.01° rounding (~1.1km radius), most beach clusters share:
1. Same NWS grid point
2. Same hourly forecast data

## Testing

To verify the fix works:

```bash
cd "E:\Code\surf_data_update" && python main_noaa.py
```

Check the logs for:
```
INFO - NWS stats: XXX beaches with data, YYY no coverage, ZZZ timeouts
INFO - NWS cache: AAA unique gridpoints, BBB unique forecasts
```

**Expected values**:
- `AAA unique gridpoints`: 200-400 (vs 1336 without caching)
- `BBB unique forecasts`: 200-400 (vs 1336 without caching)
- `ZZZ timeouts`: 0-50 (vs 100+ without timeout increase)

## Fallback Behavior

If NWS still times out or has no coverage:
1. **Temperature/Weather/Wind**: Remain `null` in forecast records
2. **Neighbor Fill**: `fill_neighbors.py` will fill from nearby beaches
3. **No Data Loss**: Wave/swell data from NOAA GFSwave still present

## Alternative Solutions (Not Implemented)

### 1. Skip NWS Entirely
```python
# In main_noaa.py
nws_enhanced = noaa_records  # Skip NWS supplement
```

**Pros**: No timeout risk
**Cons**: Lose temperature/weather/wind data (need neighbor fill)

### 2. Parallel Requests
```python
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(get_nws_gridpoint, lat, lon) for beach in beaches]
```

**Pros**: Much faster (10x speedup)
**Cons**: Risk of rate limiting, harder to debug

### 3. Use Different Weather API
Switch to different source for temperature/weather:
- NOAA HRRR model (OpenDAP)
- NOAA NAM model
- Open-Meteo (fallback)

## Recommendation

✅ **Use the implemented caching solution** - It provides the best balance of:
1. Speed (3-5x faster)
2. Reliability (better timeout handling)
3. API respect (fewer requests)
4. Simplicity (no threading complexity)

If you still experience timeouts, consider:
1. Increase timeout to 60s
2. Add retry logic with exponential backoff
3. Skip NWS for some beaches and rely on neighbor fill

## Monitoring

After running the script, check:
```bash
# Count NWS timeout errors
grep "NWS: Timeout" surf_data_update.log | wc -l

# Check cache effectiveness
grep "NWS cache:" surf_data_update.log
```

Target metrics:
- Timeouts: < 5% of beaches
- Cache hit ratio: > 60%
- Total NWS time: < 5 minutes
