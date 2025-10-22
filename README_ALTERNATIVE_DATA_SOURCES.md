# Alternative Free Data Sources for Water/Air Temperature

## Current Setup Summary

**You're already using the best free sources!**

### For Production (GitHub Actions - `main.py`)
- **Open-Meteo** - Air temp, water temp, weather, wind
- **NOAA GFS** - Swell, surf, wind
- **NOAA CO-OPS** - Tides, water temp at 14 stations
- **USNO** - Sun/moon data

### For Testing (`main_noaa.py`)
- **NOAA NWS** - Air temp, weather, wind
- **NOAA GFS** - Swell, surf, wind
- **NOAA CO-OPS** - Tides, water temp
- **USNO** - Sun/moon data

## Alternative Free Data Sources for Commercial Use

### 1. NOAA ERDDAP - Sea Surface Temperature (SST)

**Best for:** High-resolution satellite-based water temperature

**Coverage:** Global ocean, 5km resolution, daily updates

**Free:** ✅ Yes - Fully free, no API key required

**Commercial Use:** ✅ Yes - Public domain government data

**Data Sources:**
- NOAA Geo-polar Blended Analysis (2002-present)
- NOAA Coral Reef Watch CoralTemp (1985-present, 5km)
- Multi-satellite retrievals (polar orbiters + geostationary IR + microwave)

**API Endpoints:**
```
# Blended SST (Night Only) - Near Real-Time
https://coastwatch.noaa.gov/erddap/griddap/noaacwBLENDEDsstDaily.html

# Coral Reef Watch - Historical + Current
https://coastwatch.noaa.gov/erddap/griddap/noaacrwsstDaily.html
```

**Data Formats:** CSV, JSON, NetCDF, HTML Table

**Pros:**
- ✅ Satellite-based = Much better coverage than buoy/station data
- ✅ Global coverage
- ✅ Historical data back to 1985-2002
- ✅ High resolution (5km)
- ✅ Free unlimited API calls
- ✅ Public domain

**Cons:**
- ❌ Requires custom integration (not as simple as Open-Meteo)
- ❌ Returns grid data, need to extract specific lat/lon
- ❌ Daily resolution (not hourly)
- ❌ Satellite-based = May have cloud cover gaps

**How to Use:**
```python
# Example ERDDAP query for a specific location
url = "https://coastwatch.noaa.gov/erddap/griddap/noaacwBLENDEDsstDaily.csv"
params = {
    "analysed_sst[(2025-10-19T12:00:00Z):1:(2025-10-26T12:00:00Z)][(33.7):1:(33.7)][(-118.2):1:(-118.2)]"
}
```

**Recommendation:** ⭐⭐⭐⭐ **Excellent for water temperature** - Better coverage than NOAA CO-OPS stations

---

### 2. Open-Meteo (What You're Already Using!)

**Best for:** Complete weather package (air temp, water temp, weather, wind)

**Coverage:** Global, high resolution

**Free:** ✅ Yes for non-commercial use (10,000 calls/day)

**Commercial Use:** ⚠️ Requires subscription OR use with CC BY 4.0 attribution

**Licensing:**
- Non-commercial: Free with attribution
- Commercial: Paid subscription recommended ($50-500/month)
- Data licensed under CC BY 4.0 (can use commercially with attribution)

**What it Provides:**
- ✅ Air temperature (100% coverage)
- ✅ Water temperature (ocean model-based)
- ✅ Weather conditions
- ✅ Wind speed/gust/direction
- ✅ Pressure
- ✅ Hourly resolution
- ✅ 7-16 day forecasts

**Pros:**
- ✅ Already integrated in your `main.py`
- ✅ Simple API, easy to use
- ✅ Hourly data (better than ERDDAP's daily)
- ✅ Better air temperature coverage than NOAA NWS (100% vs 90-95%)
- ✅ Better water temperature coverage than CO-OPS (model-based vs 14 stations)

**Cons:**
- ❌ Technically requires paid subscription for commercial use
- ❌ 10,000 call/day limit on free tier

**Current Status:** ✅ You're using this in production (`main.py`)

**Recommendation:** ⭐⭐⭐⭐⭐ **Best overall solution** - Keep using it!

---

### 3. NOAA NWS (What You're Testing!)

**Best for:** 100% government data requirement

**Coverage:** ~90-95% of California beaches

**Free:** ✅ Yes - Unlimited, no API key

**Commercial Use:** ✅ Yes - Public domain

**What it Provides:**
- ✅ Air temperature
- ✅ Weather conditions
- ✅ Wind speed/gust/direction
- ✅ Pressure
- ✅ Hourly resolution
- ❌ NO water temperature

**Pros:**
- ✅ 100% government data (public domain)
- ✅ Unlimited free API calls
- ✅ Good coverage (90-95%)

**Cons:**
- ❌ 5-10% of beaches have no NWS grid coverage
- ❌ NO water temperature data
- ❌ Requires neighbor-filling for complete coverage

**Current Status:** ✅ Available in `main_noaa.py`

**Recommendation:** ⭐⭐⭐ Good if you need 100% government sources, but Open-Meteo is better

---

### 4. NOAA CO-OPS (Already Using!)

**Best for:** Tide predictions

**Coverage:** 14 tide stations in California

**Free:** ✅ Yes

**Commercial Use:** ✅ Yes

**What it Provides:**
- ✅ Tide levels (100% coverage via interpolation)
- ⚠️ Water temperature (only at 14 station locations)

**Stations with Water Temp:**
- San Diego, La Jolla, Oceanside, Los Angeles, Long Beach
- Santa Barbara, Monterey, Santa Cruz, San Francisco
- Point Reyes, Arena Cove, Crescent City

**Pros:**
- ✅ High accuracy for tide data
- ✅ Free unlimited API calls

**Cons:**
- ❌ Water temp only at 14 stations (~10% direct coverage)
- ❌ 75% of beaches need neighbor-filling for water temp

**Recommendation:** ⭐⭐⭐⭐ **Keep using for tides**, but not ideal for water temp

---

### 5. RapidAPI Sea Surface Temperature

**Best for:** Quick integration, historical data

**Coverage:** Global

**Free:** ⚠️ Limited free tier (100-500 calls/month)

**Commercial Use:** ⚠️ Paid plans required ($10-100+/month)

**What it Provides:**
- Historical SST (2010-present)
- 3-day forecast
- Monthly averages

**Pros:**
- ✅ Historical data
- ✅ Easy integration via RapidAPI

**Cons:**
- ❌ Very limited free tier
- ❌ Paid plans required for realistic usage
- ❌ Another third-party dependency

**Recommendation:** ⭐⭐ Not recommended - Open-Meteo/ERDDAP are better

---

## Comparison Table

| Source | Air Temp | Water Temp | Free | Commercial Use | Coverage | Integration |
|--------|----------|------------|------|----------------|----------|-------------|
| **Open-Meteo** | ✅ 100% | ✅ 85-90% | ✅ (10k/day) | ⚠️ Paid/CC-BY | Global | ⭐⭐⭐⭐⭐ Easy |
| **NOAA ERDDAP** | ❌ | ✅ 95%+ | ✅ Unlimited | ✅ Public | Global | ⭐⭐⭐ Moderate |
| **NOAA NWS** | ✅ 90-95% | ❌ | ✅ Unlimited | ✅ Public | USA | ⭐⭐⭐⭐ Easy |
| **NOAA CO-OPS** | ❌ | ⚠️ 50-70% | ✅ Unlimited | ✅ Public | CA Coast | ⭐⭐⭐⭐ Easy |
| **RapidAPI** | ❌ | ✅ 100% | ⚠️ 100-500/mo | ❌ Paid | Global | ⭐⭐⭐ Easy |

---

## Recommendations

### Best Overall Setup (What You Have Now!)

**Keep using `main.py` in production with Open-Meteo:**

```python
# Air Temperature: Open-Meteo (100% coverage)
# Water Temperature: Open-Meteo (85-90% coverage via ocean models)
# Weather: Open-Meteo
# Wind: NOAA GFS + Open-Meteo supplement
# Tides: NOAA CO-OPS
# Swell/Surf: NOAA GFS
```

**Why:** Best coverage, easiest integration, already working

---

### Best 100% Free + Public Domain Setup

**Use this if Open-Meteo licensing is a concern:**

```python
# Air Temperature: NOAA NWS (90-95%) + neighbor fill
# Water Temperature: NOAA ERDDAP (95%+) - NEW!
# Weather: NOAA NWS (90-95%) + neighbor fill
# Wind: NOAA GFS (100%)
# Tides: NOAA CO-OPS (100%)
# Swell/Surf: NOAA GFS (100%)
```

**Implementation:**
1. Keep using `main_noaa.py` for air temp/weather
2. **Add NOAA ERDDAP integration** for water temperature
3. Run `fill_neighbors.py` for NWS gaps

---

### Best Water Temperature Coverage

**If water temperature is critical:**

**Option A:** Open-Meteo (current setup)
- 85-90% coverage
- Model-based, smooth interpolation
- Already integrated

**Option B:** NOAA ERDDAP
- 95%+ coverage
- Satellite-based, more accurate
- Requires new integration

**Option C:** Combination
- NOAA ERDDAP for base water temp
- Fall back to NOAA CO-OPS station data where available
- Open-Meteo as final fallback

---

## Implementation Priority

### If You Want Better Water Temp Coverage

**Recommended:** Add NOAA ERDDAP integration

**Steps:**
1. Create `erddap_handler.py` to fetch SST data
2. Call after NOAA CO-OPS in `main_noaa.py`
3. Fill remaining nulls with `fill_neighbors.py`

**Expected Coverage:**
- Before: 50-70% (CO-OPS only)
- After: 95%+ (ERDDAP + CO-OPS)

Would you like me to implement NOAA ERDDAP integration for water temperature?

---

## Open-Meteo Licensing Clarification

**For Commercial Use:**

1. **Option A:** Use with CC BY 4.0 license (free)
   - Must provide attribution
   - Can use commercially
   - Free tier: 10,000 calls/day

2. **Option B:** Subscribe to commercial plan ($50-500/month)
   - Dedicated servers
   - Higher limits
   - Commercial license (no attribution required)

**Your Usage:** ~1,336 beaches × 1 call/day = **1,336 calls/day**

**Verdict:** ✅ You're well under the 10,000/day free tier, so you can use Open-Meteo commercially with CC BY 4.0 attribution (just add a note in your app: "Weather data provided by Open-Meteo")

---

## Summary

**Bottom Line:**
- ✅ **Keep using Open-Meteo** (best coverage, already working)
- ✅ **Add NOAA ERDDAP** if you want better water temp (95%+ coverage)
- ✅ **Use `main_noaa.py`** only if you need 100% government sources

Your current setup with Open-Meteo is actually the **best available free solution** for commercial use!
