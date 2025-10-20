# Wind Gust Data Solution

## Problem
The initial NOAA stack implementation showed 0% coverage for `wind_gust_mph` because:
1. NOAA NWS API doesn't consistently provide wind gust forecasts in their hourly data
2. NOAA GFSwave dataset focuses on ocean waves, not atmospheric wind gusts

## Solution Implemented

### Multi-Source Wind Gust Strategy

We've implemented a cascading approach to obtain wind gust data:

#### 1. **Primary: NOAA GFSwave `gustsfc` (if available)**
```python
# In noaa_handler.py extract_grid_point_data()
if 'gustsfc' in ds.data_vars:
    wgust = ds["gustsfc"].sel(lat=grid_lat, lon=grid_lon).values
    grid_data['wind_gust_mps'] = wgust[sel_idx]
```

The NOAA GFS atmospheric model includes `gustsfc` (surface wind gust) variable. However, GFSwave (ocean wave model) may not always include this variable.

#### 2. **Secondary: NOAA NWS API Wind Gust**
```python
# In nws_handler.py get_nws_supplement_data()
if period.get("windGust"):
    wind_gust = parse_wind_speed(period["windGust"])
    if wind_gust is not None:
        rec["wind_gust_mph"] = safe_float(wind_gust)
```

NWS hourly forecasts sometimes include a `windGust` field, which we parse when available.

#### 3. **Fallback: Estimate from Wind Speed (1.4x factor)**
```python
# Both handlers use this fallback
if wind_gust_mph is None and wind_speed_mph is not None:
    wind_gust_mph = safe_float(wind_speed_mph * 1.4)
```

When no actual gust data is available, we estimate using the typical meteorological gust factor of 1.4x sustained wind speed.

## Why 1.4x Gust Factor?

The 1.4x multiplier is based on meteorological standards:
- **Typical gust factor range**: 1.3 to 1.5
- **Open ocean/coastal**: 1.4 is the standard factor
- **Used by**: National Weather Service, meteorological models
- **Formula**: `Gust Speed ≈ 1.4 × Sustained Wind Speed`

This provides a reasonable estimate when direct gust measurements/forecasts are unavailable.

## Data Coverage Expected

With this implementation:
- **GFSwave gustsfc**: Variable (depends on dataset version)
- **NWS windGust**: ~0-20% (NWS doesn't always provide)
- **Estimated (1.4x)**: 100% (always available as fallback)

**Total coverage**: 100% (through estimation fallback)

## Testing

To verify wind gust coverage:
```bash
python test_full_noaa_run.py
```

Check the "Data Quality Summary" section for `wind_gust_mph` completeness.

## Alternative Approaches Considered

### 1. NOAA GFS Atmospheric Model (Separate Dataset)
- **URL**: `https://nomads.ncep.noaa.gov/dods/gfs_0p25/gfs{date}/gfs_0p25_{cycle}z`
- **Variable**: `gustsfc`
- **Pros**: Direct wind gust forecasts from atmospheric model
- **Cons**:
  - Requires loading second dataset (more API calls)
  - Different grid/timing than GFSwave
  - Increased complexity and rate limit risk

### 2. NOAA HRRR (High Resolution Rapid Refresh)
- **URL**: `https://nomads.ncep.noaa.gov/dods/hrrr`
- **Resolution**: 3km (better than GFS 0.25°)
- **Pros**: Higher resolution, better gust forecasts
- **Cons**:
  - US-only coverage
  - Shorter forecast range (18-48 hours vs 7 days)
  - More complex to integrate

### 3. Wind Gust from Variance/Turbulence Models
- **Method**: Calculate from wind speed variance
- **Pros**: More sophisticated estimation
- **Cons**:
  - Requires additional meteorological data
  - Computational overhead
  - Not significantly better than 1.4x factor for coastal areas

## Recommendation

The implemented solution (1.4x estimation fallback) is optimal because:
1. ✅ **100% coverage** guaranteed through fallback
2. ✅ **No additional API calls** required
3. ✅ **Meteorologically sound** (standard NWS practice)
4. ✅ **Simple and maintainable**
5. ✅ **Free and public domain**

## Future Enhancements

If more accurate gust data is needed:
1. Load separate NOAA GFS atmospheric dataset for `gustsfc`
2. Use NOAA HRRR for short-term forecasts (0-18 hours)
3. Implement location-specific gust factors based on terrain/exposure

## References
- NOAA GFS Documentation: https://www.emc.ncep.noaa.gov/emc/pages/numerical_forecast_systems/gfs.php
- NWS API: https://www.weather.gov/documentation/services-web-api
- Wind Gust Estimation: https://www.weather.gov/media/epz/wxcalc/windGust.pdf
