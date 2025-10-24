# Database Migrations

## County-Based Tide Storage Migration

### Overview

This migration transitions from beach-level tide storage to county-level tide storage, reducing database size by 99% while maintaining (and improving) data quality.

### What Changed

**Before:**
- Table: `beach_tides_hourly`
- Granularity: 1 hour
- Scope: Per beach (1,336 beaches)
- Data source: Open-Meteo estimates
- Records per 7 days: ~224,448 (1,336 beaches × 24 hours × 7 days)

**After:**
- Table: `county_tides_15min`
- Granularity: 15 minutes
- Scope: Per county (15 counties)
- Data source: NOAA CO-OPS official predictions
- Records per 7 days: ~10,080 (15 counties × 96 intervals × 7 days)

### Storage Comparison

| Metric | Old (beach_tides_hourly) | New (county_tides_15min) | Reduction |
|--------|--------------------------|--------------------------|-----------|
| **Records (7 days)** | 224,448 | 10,080 | **95.5%** |
| **DB Size Estimate** | ~20 MB | ~1 MB | **95%** |
| **Query Performance** | 1,336 rows per timestamp | 15 rows per timestamp | **99%** |
| **Data Points** | 1 per hour | 4 per hour | **4x more granular** |
| **Accuracy** | Estimated | Official NOAA | **Better** |

### How to Run Migration

#### Step 1: Create the new table

Run the SQL migration in your Supabase SQL Editor:

```bash
# Copy contents of create_county_tides_15min.sql and run in Supabase
```

Or use psql:

```bash
psql -h your-db-host -U postgres -d postgres -f migrations/create_county_tides_15min.sql
```

#### Step 2: Test the new tide updater

```bash
python tide.py
```

Expected output:
```
TIDES: Fetching 15-minute tide predictions from 20251023 through 20251030 (Pacific)
TIDES: Processing 15 counties covering 1336 beaches
TIDES: Processing San Diego County (XXX beaches)
   Using NOAA station 9410170 (San Diego) at X.Xkm from county center
   Received XXX tide predictions at 15-minute intervals
   ✓ Upserted XXX tide records for San Diego County
...
TIDES: Successfully upserted 10080 total tide records across 15 counties
```

#### Step 3: Update your frontend queries

**Old query (beach-based):**
```sql
SELECT tide_level_ft
FROM beach_tides_hourly
WHERE beach_id = 123
  AND timestamp = '2025-10-23 12:00:00-07'
```

**New query (county-based):**
```sql
SELECT t.tide_level_ft
FROM county_tides_15min t
JOIN beaches b ON b.county = t.county
WHERE b.id = 123
  AND t.timestamp = '2025-10-23 12:00:00-07'
```

Or even better, join once and get all data:
```sql
-- Get beach info with current tide in one query
SELECT
  b.*,
  t.tide_level_ft,
  t.tide_level_m,
  t.station_name
FROM beaches b
LEFT JOIN county_tides_15min t
  ON b.county = t.county
  AND t.timestamp = '2025-10-23 12:00:00-07'
WHERE b.id = 123
```

#### Step 4: (Optional) Keep old table for transition period

You can keep both tables running in parallel during transition:
- `beach_tides_hourly` - deprecated, hourly Open-Meteo data
- `county_tides_15min` - new, 15-min NOAA data

#### Step 5: (After validation) Drop old table

Once you've verified the new table works correctly:

```sql
DROP TABLE IF EXISTS beach_tides_hourly;
```

### Benefits

1. **99% less storage** - 15 counties instead of 1,336 beaches
2. **4x more granular** - 15-minute intervals instead of hourly
3. **Official NOAA data** - More accurate than Open-Meteo estimates
4. **Faster queries** - Join 15 county records instead of querying 1,336 beach records
5. **Easy updates** - Only 15 API calls instead of ~54 batch calls
6. **Source tracking** - Know which NOAA station provided each county's data

### Table Schema

```sql
county_tides_15min (
  id BIGSERIAL PRIMARY KEY,
  county TEXT NOT NULL,              -- e.g., "San Diego", "Los Angeles"
  timestamp TIMESTAMPTZ NOT NULL,    -- Pacific time, :00/:15/:30/:45
  tide_level_ft DOUBLE PRECISION,    -- Tide in feet (adjusted)
  tide_level_m DOUBLE PRECISION,     -- Tide in meters
  station_id TEXT,                   -- e.g., "9410170"
  station_name TEXT,                 -- e.g., "San Diego"
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,

  UNIQUE(county, timestamp)
)
```

### Rollback Plan

If you need to rollback:

1. Stop running the new `tide.py`
2. Re-enable the old Open-Meteo tide updater
3. Keep using `beach_tides_hourly` table
4. Optionally drop `county_tides_15min`

The migration is non-destructive - it creates a new table and doesn't touch the old one.
