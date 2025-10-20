# 100% NOAA/Government Data Stack

## Overview

This implementation uses **only free, public domain government data sources** for all surf forecast data. No commercial APIs or restrictive licenses - everything is free for commercial use.

## Data Sources

### 1. **NOAA GFSwave** (Primary Wave Data)
- **What**: Wave and swell forecasts
- **Fields**:
  - Primary/secondary/tertiary swell height, period, direction
  - Surf height min/max
  - Wave energy
  - Wind speed and direction
  - Wind gust (if available in dataset, otherwise estimated at 1.4x wind speed)
- **API**: OpenDAP/THREDDS
- **License**: Public domain
- **Handler**: `noaa_handler.py`

### 2. **NOAA National Weather Service (NWS)**
- **What**: Weather forecasts
- **Fields**:
  - Temperature (°F)
  - Weather conditions (coded)
  - Wind speed (mph)
  - Wind gusts (mph) - from NWS if available, otherwise estimated at 1.4x wind speed
  - Atmospheric pressure
- **API**: https://api.weather.gov
- **License**: Public domain, no API key required
- **Handler**: `nws_handler.py`
- **Coverage**: US only (perfect for California beaches)
- **Note**: NWS doesn't always provide wind gust forecasts; when unavailable, we estimate using 1.4x wind speed (typical gust factor)

### 3. **NOAA CO-OPS (Tides & Currents)**
- **What**: Tide predictions and water temperature
- **Fields**:
  - Tide level (feet, MLLW datum + 2.4ft adjustment)
  - Water temperature (°F) from coastal stations
- **API**: https://api.tidesandcurrents.noaa.gov/api/prod/
- **License**: Public domain, no API key required
- **Handler**: `noaa_tides_handler.py`
- **Stations**: 14 California coastal stations

### 4. **USNO Astronomical Applications**
- **What**: Sun and moon data
- **Fields**:
  - Sunrise time (HH:MM)
  - Sunset time (HH:MM)
  - Moon phase (0.0-1.0)
- **API**: https://aa.usno.navy.mil/api
- **License**: Public domain, no API key required
- **Handler**: `usno_handler.py`
- **Replaces**: Visual Crossing API

## File Structure

### New Files (NOAA Stack)
```
nws_handler.py           # NOAA NWS weather data
noaa_tides_handler.py    # NOAA CO-OPS tides/water temp
usno_handler.py          # USNO sun/moon data
main_noaa.py             # Main script using 100% NOAA sources
```

### Existing Files (Kept for Reference)
```
openmeteo_handler.py     # Kept but not used by default
main.py                  # Original with Open-Meteo/Visual Crossing
```

## Usage

### Run with 100% NOAA Sources
```bash
python main_noaa.py
```

### Run with Original Setup (Open-Meteo/Visual Crossing)
```bash
python main.py
```

## API Limits & Rate Limiting

### NOAA APIs
- **GFSwave**: Reasonable use, 0.2s delay per request
- **NWS**: No official limit, 0.2s delay per request
- **CO-OPS**: No official limit, 0.5s delay per request
- **USNO**: No official limit, 0.3s delay per request

All NOAA APIs are very generous with limits and designed for public access.

## Advantages of NOAA Stack

1. **100% Free for Commercial Use** - No licensing restrictions
2. **No API Keys Required** - Simpler deployment
3. **Official Government Data** - Most authoritative sources
4. **No Terms of Service Issues** - Public domain data
5. **Better Coverage for US** - Optimized for US coastal areas
6. **More Accurate Tides** - Real station data vs modeled
7. **Transparent Sourcing** - All government sources, easily auditable

## Data Comparison

| Field | Open-Meteo | NOAA Stack | Source |
|-------|------------|------------|--------|
| Wave Height | ✓ Modeled | ✓ GFSwave Model | NOAA GFSwave |
| Swell Data | ✓ Modeled | ✓ GFSwave Model | NOAA GFSwave |
| Temperature | ✓ Modeled | ✓ NWS Forecast | NOAA NWS |
| Weather | ✓ Modeled | ✓ NWS Forecast | NOAA NWS |
| Wind | ✓ Modeled | ✓ NWS + GFSwave | NOAA NWS/GFSwave |
| Tides | ✓ Modeled | ✓ **Station Data** | NOAA CO-OPS |
| Water Temp | ✓ Modeled | ✓ **Station Data** | NOAA CO-OPS |
| Sunrise/Sunset | ✓ Calculated | ✓ USNO Official | USNO |
| Moon Phase | ✓ Calculated | ✓ USNO Official | USNO |

**Bold** = Real measured data (more accurate than models)

## California Tide Stations

The NOAA CO-OPS handler uses these 14 coastal stations:

- San Diego (9410170)
- La Jolla (9410660)
- Oceanside (9410840)
- Santa Barbara (9414290)
- Los Angeles (9412110)
- Long Beach (9413450)
- Point Arguello (9414750)
- Monterey (9415020)
- Santa Cruz (9415144)
- San Francisco (9414863)
- Point Reyes (9415316)
- Arena Cove (9416841)
- Crescent City (9418767)

Each beach is automatically matched to the nearest station (within 100km).

## Migration Guide

### From Open-Meteo to NOAA Stack

1. **Backup your current setup**:
   ```bash
   cp main.py main_backup.py
   ```

2. **Test the new stack**:
   ```bash
   python main_noaa.py
   ```

3. **Verify data quality**:
   - Check null counts in database
   - Compare data coverage
   - Monitor API response times

4. **Switch permanently** (optional):
   ```bash
   mv main.py main_openmeteo.py
   mv main_noaa.py main.py
   ```

### GitHub Actions

Update your workflow file to use the new main script:

```yaml
- name: Run surf data update
  run: python main_noaa.py  # Changed from main.py
```

## Troubleshooting

### NWS API Returns 404
- **Cause**: Location outside NWS coverage (non-US)
- **Solution**: Falls back to neighbor fill for that beach

### CO-OPS No Water Temperature
- **Cause**: Not all stations have water temp sensors
- **Solution**: Falls back to neighbor fill from nearby beaches

### USNO API Timeout
- **Cause**: High load or network issues
- **Solution**: Retry with exponential backoff, or use fallback nulls

## Future Enhancements

Potential additions to the NOAA stack:

1. **NOAA NDBC Buoys** - Real-time wave observations
2. **NOAA HRRR Model** - Higher resolution weather
3. **NOAA RAP Model** - Rapid refresh forecasts
4. **UV Index** - From NWS UV forecast

## License

All data sources are **public domain**:
- NOAA data: https://www.noaa.gov/information-technology/open-data-dissemination
- USNO data: US Government work, public domain

This code is provided as-is for integration with these public data sources.

## Support

For issues with:
- **NOAA APIs**: https://www.ncei.noaa.gov/support/access-data-service
- **NWS API**: https://weather-gov.github.io/api/
- **USNO API**: https://aa.usno.navy.mil/data/api

## Credits

Created as a 100% free, commercial-use alternative to proprietary weather APIs.
All data courtesy of:
- NOAA (National Oceanic and Atmospheric Administration)
- NWS (National Weather Service)
- USNO (US Naval Observatory)
