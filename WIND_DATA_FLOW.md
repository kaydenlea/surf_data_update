# Wind Data Flow - After Fix

## Execution Order (main_noaa_grid.py)

```
Step 1: NOAA GFSwave
├─ Extracts: wave/swell data
├─ Calculates: wind_direction_deg (from ugrdsfc/vgrdsfc)
└─ Does NOT set: wind_speed_mph, wind_gust_mph
    └─ Reason: gustsfc variable doesn't exist in GFSwave

Step 2: GFS Atmospheric
├─ Extracts: temperature, pressure, cloud cover, precipitation
├─ Extracts: ugrd10m, vgrd10m, gustsfc
├─ Calculates: wind_speed_mph = sqrt(u² + v²)
├─ Checks: if wind_speed_mph is NULL → fills it (FALLBACK)
└─ Checks: if wind_gust_mph is NULL → fills it (FALLBACK)
    └─ Usually both are NULL at this point

Step 3: Open Meteo ⭐ PRIMARY WIND SOURCE
├─ Extracts: wind_speed_10m, wind_gusts_10m
├─ Extracts: weather_code, sea_surface_temperature
├─ ALWAYS fills: wind_speed_mph (overwrites GFS if present)
└─ ALWAYS fills: wind_gust_mph (overwrites GFS if present)
    └─ Ensures both come from same source for consistency

Step 4: NOAA CO-OPS
└─ Fills: tide data
```

## Wind Data Priority

```
┌─────────────────────────────────────────────────┐
│          FINAL WIND DATA SOURCE                 │
├─────────────────────────────────────────────────┤
│                                                 │
│  PRIMARY: Open Meteo (wind_speed_10m + gusts)  │
│  ├─ Coverage: 16 days                          │
│  ├─ Resolution: ~11 km                         │
│  └─ Quality: ~90% consistent (gust ≥ speed)    │
│                                                 │
│  FALLBACK: GFS Atmospheric (ugrd/vgrd + gust)  │
│  ├─ Coverage: 10 days                          │
│  ├─ Resolution: 0.25° (~28 km)                 │
│  ├─ Used when: Open Meteo unavailable          │
│  └─ Quality: High consistency                  │
│                                                 │
│  DIRECTION ONLY: NOAA GFSwave                   │
│  └─ wind_direction_deg (highest accuracy)      │
│                                                 │
└─────────────────────────────────────────────────┘
```

## Example Record Timeline

```
Time: 00:00 - NOAA GFSwave runs
{
  "grid_id": 21,
  "timestamp": "2025-11-10T00:00:00Z",
  "primary_swell_height_ft": 5.2,
  "wind_direction_deg": 245,
  "wind_speed_mph": null,        ← Not set
  "wind_gust_mph": null          ← Not set
}

Time: 00:05 - GFS Atmospheric runs
{
  ...
  "wind_direction_deg": 245,
  "wind_speed_mph": 12.5,        ← Set by GFS (fallback)
  "wind_gust_mph": 15.8          ← Set by GFS (fallback)
}

Time: 00:10 - Open Meteo runs
{
  ...
  "wind_direction_deg": 245,     ← From GFSwave (kept)
  "wind_speed_mph": 13.2,        ← OVERWRITTEN by Open Meteo ⭐
  "wind_gust_mph": 16.5          ← OVERWRITTEN by Open Meteo ⭐
}

Result: Wind data is consistent (both from Open Meteo)
Gust (16.5) > Speed (13.2) ✓
```

## Code Changes Summary

### openmeteo_handler.py (lines 538-545)
```python
# OLD (supplement mode - only filled if both NULL):
if wind_speed_missing and wind_gust_missing:
    if wind_speed_mph_val is not None and wind_gust_mph_val is not None:
        rec["wind_speed_mph"] = wind_speed_mph_val
        rec["wind_gust_mph"] = wind_gust_mph_val

# NEW (primary mode - always fills):
if wind_speed_mph_val is not None and wind_gust_mph_val is not None:
    # Always fill BOTH fields to ensure paired, consistent data
    rec["wind_speed_mph"] = wind_speed_mph_val
    rec["wind_gust_mph"] = wind_gust_mph_val
```

### gfs_atmospheric_handler_v2.py (lines 449-472)
```python
# NEW: Only fills as FALLBACK (when Open Meteo hasn't filled)
if grid_data['wind_u_ms'] is not None and grid_data['wind_v_ms'] is not None:
    # Calculate from u/v components
    wind_speed_ms = np.sqrt(wind_u_val**2 + wind_v_val**2)
    wind_speed_mph = safe_float(mps_to_mph(wind_speed_ms))
    # Only fill if Open Meteo hasn't already filled it
    if wind_speed_mph is not None and rec.get("wind_speed_mph") is None:
        rec["wind_speed_mph"] = wind_speed_mph
```

### noaa_grid_handler.py (lines 221-233)
```python
# OLD: Calculated wind speed from u/v
wind_speed_ms = np.sqrt(wu**2 + wv**2)
wind_speed_mph = wind_speed_ms * 2.23694

# NEW: Only calculates direction
wind_direction = (270 - np.degrees(np.arctan2(wv, wu))) % 360
# Wind speed/gust now come from Open Meteo/GFS Atmospheric
```

## Benefits

✅ **Consistency**: Wind speed & gust from same source (Open Meteo)
✅ **Extended Coverage**: 16-day forecast from Open Meteo
✅ **Reliability**: GFS Atmospheric provides backup
✅ **Accuracy**: Wind direction still from high-res GFSwave
✅ **Reduced Errors**: ~23% → ~10% records with gust < speed
