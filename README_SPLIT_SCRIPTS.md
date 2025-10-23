# Split Script Workflow

The surf data update process has been split into two independent scripts for flexibility and speed.

## Scripts Overview

### 1. `step1_wave_data.py` - Fast Wave Data Update
**Purpose**: Fetch core wave/swell/surf data from NOAA GFSwave
**Runtime**: ~1-3 minutes
**Rate Limits**: Fast (0.2s delays)

**Data Populated**:
- ✓ Primary/secondary/tertiary swell (height, period, direction)
- ✓ Surf height range (min/max)
- ✓ Wave energy
- ✓ Basic wind data (speed, direction, gust)

### 2. `step2_supplement_data.py` - Supplemental Data Enhancement
**Purpose**: Add weather, tides, and astronomical data
**Runtime**: ~12-20 minutes
**Rate Limits**: Conservative (5-8s delays to avoid NOAA blocking)

**Data Populated**:
- ✓ Temperature (°F)
- ✓ Weather conditions (WMO codes)
- ✓ Enhanced wind data (speed, direction, gust)
- ✓ Atmospheric pressure (inHg)
- ✓ Tide levels (ft)
- ✓ Water temperature (°F)
- ✓ Sunrise/sunset times
- ✓ Moonrise/moonset times
- ✓ Moon phase

---

## Usage

### Option 1: Run Both Scripts (Complete Update)

```bash
# Step 1: Get wave data (fast - 1-3 minutes)
python step1_wave_data.py

# Step 2: Add supplemental data (slow - 12-20 minutes)
python step2_supplement_data.py
```

**Total time**: ~15-25 minutes for complete database update

### Option 2: Wave Data Only (Quick Updates)

If you just need fresh wave forecasts:

```bash
# Only run step 1
python step1_wave_data.py
```

**Use case**: Quick updates during active surf sessions, when you only care about wave changes

### Option 3: Supplement Data Only (Update Weather/Tides)

If you already have wave data and just want to refresh weather/tides:

```bash
# Only run step 2 (requires existing wave data)
python step2_supplement_data.py
```

**Use case**: Weather changed but waves are the same

---

## How It Works

### Step 1: Wave Data Flow
```
step1_wave_data.py
    ↓
Fetch beaches from database
    ↓
NOAA GFSwave (ocean data)
    ↓
Upsert to forecast_data table
    ↓
DONE ✓
```

### Step 2: Supplement Data Flow
```
step2_supplement_data.py
    ↓
Fetch beaches & counties
    ↓
Fetch EXISTING forecast records from database
    ↓
Enhance with NOAA GFS Atmospheric (weather/temp/wind/pressure)
    ↓
Enhance with NOAA CO-OPS (tides/water temp)
    ↓
Upsert enhanced records to forecast_data table
    ↓
Add USNO data (sun/moon) to daily_county_conditions
    ↓
DONE ✓
```

**Important**: Step 2 reads existing records from the database, so Step 1 must run first (at least once).

---

## Rate Limiting

### Step 1 (Wave Data)
- **Request delay**: 0.2 seconds
- **Batch delay**: 0.2 seconds
- **Total requests**: ~80-100
- **Why fast**: GFSwave has relaxed rate limits

### Step 2 (Supplemental Data)
- **Request delay**: 5.0 seconds (atmospheric)
- **Batch delay**: 8.0 seconds (atmospheric)
- **Total requests**: ~80-100 locations
- **Why slow**: GFS Atmospheric is VERY strict about rate limits

**Note**: If you hit rate limit errors in Step 2, increase the delays in `config.py`:
```python
NOAA_ATMOSPHERIC_REQUEST_DELAY = 5.0  # Increase to 7.0 or 10.0
NOAA_ATMOSPHERIC_BATCH_DELAY = 8.0    # Increase to 10.0 or 15.0
```

---

## Scheduling Examples

### Daily Full Update (Recommended)
```bash
# Cron job: Every day at 6 AM
0 6 * * * cd /path/to/surf_data_update && python step1_wave_data.py && python step2_supplement_data.py
```

### Frequent Wave Updates + Periodic Weather Updates
```bash
# Wave data every 3 hours
0 */3 * * * cd /path/to/surf_data_update && python step1_wave_data.py

# Supplement data once daily at 6 AM
0 6 * * * cd /path/to/surf_data_update && python step2_supplement_data.py
```

### Quick Surf Check During Active Session
```bash
# Manual run - just wave data (1-3 minutes)
python step1_wave_data.py
```

---

## Troubleshooting

### "No existing forecast records found"
**Problem**: Step 2 can't find wave data
**Solution**: Run `step1_wave_data.py` first

### "Over Rate Limit" errors
**Problem**: NOAA blocked your IP
**Solution**:
1. Wait 1 hour
2. Increase delays in `config.py`
3. Run scripts less frequently

### Step 1 works but Step 2 fails
**Problem**: Atmospheric data has stricter rate limits
**Solution**: This is normal - Step 2 takes longer. Be patient and don't interrupt it.

---

## Original Combined Script

The original `main_noaa.py` still exists and runs both steps in sequence:

```bash
python main_noaa.py
```

**Equivalent to**:
```bash
python step1_wave_data.py && python step2_supplement_data.py
```

Use `main_noaa.py` if you want the old behavior.

---

## Benefits of Split Scripts

1. ✅ **Faster wave updates** - Get fresh surf forecasts in 1-3 minutes
2. ✅ **Independent scheduling** - Update waves frequently, weather occasionally
3. ✅ **Better debugging** - Isolate issues to specific data sources
4. ✅ **Flexibility** - Skip slow steps when you don't need them
5. ✅ **Avoid rate limits** - Run wave data multiple times per day without triggering blocks

---

## Questions?

- **Q**: Do I always need to run both scripts?
  **A**: No! Run Step 1 for quick wave updates. Run Step 2 when you need complete data.

- **Q**: How often can I run Step 1?
  **A**: Every 1-3 hours safely (GFSwave updates every 6 hours)

- **Q**: How often can I run Step 2?
  **A**: Once or twice per day max (due to strict rate limits)

- **Q**: Can I run them in parallel?
  **A**: No - Step 2 needs Step 1's data. Always run Step 1 first.
