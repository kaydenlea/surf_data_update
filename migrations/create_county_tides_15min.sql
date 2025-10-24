-- Migration: Create county_tides_15min table
-- Purpose: Store tide predictions at 15-minute intervals by county instead of per beach
-- Benefit: 99% less storage (15 counties vs 1,336 beaches) while maintaining same data quality

-- Create the county_tides_15min table
CREATE TABLE IF NOT EXISTS county_tides_15min (
    id BIGSERIAL PRIMARY KEY,
    county TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    tide_level_ft DOUBLE PRECISION,
    tide_level_m DOUBLE PRECISION,
    station_id TEXT,
    station_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Unique constraint on county + timestamp (one tide reading per county per 15-min interval)
    CONSTRAINT county_tides_15min_unique UNIQUE (county, timestamp)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_county_tides_15min_county ON county_tides_15min(county);
CREATE INDEX IF NOT EXISTS idx_county_tides_15min_timestamp ON county_tides_15min(timestamp);
CREATE INDEX IF NOT EXISTS idx_county_tides_15min_county_timestamp ON county_tides_15min(county, timestamp);

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_county_tides_15min_updated_at
    BEFORE UPDATE ON county_tides_15min
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE county_tides_15min IS 'NOAA CO-OPS tide predictions at 15-minute intervals, stored by county';
COMMENT ON COLUMN county_tides_15min.county IS 'County name (e.g., San Diego, Los Angeles)';
COMMENT ON COLUMN county_tides_15min.timestamp IS 'Timestamp in Pacific timezone at 15-minute intervals (:00, :15, :30, :45)';
COMMENT ON COLUMN county_tides_15min.tide_level_ft IS 'Tide level in feet (adjusted by TIDE_ADJUSTMENT_FT)';
COMMENT ON COLUMN county_tides_15min.tide_level_m IS 'Tide level in meters';
COMMENT ON COLUMN county_tides_15min.station_id IS 'NOAA CO-OPS station ID used for this county';
COMMENT ON COLUMN county_tides_15min.station_name IS 'NOAA CO-OPS station name';
