-- Create grid_forecast_data table
-- This table stores forecast data per grid point instead of per beach
-- Structure mirrors forecast_data table but uses grid_id instead of beach_id

CREATE TABLE IF NOT EXISTS grid_forecast_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    grid_id INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,

    -- Swell data (from NOAA GFSwave)
    primary_swell_height_ft REAL,
    primary_swell_period_s REAL,
    primary_swell_direction REAL,
    secondary_swell_height_ft REAL,
    secondary_swell_period_s REAL,
    secondary_swell_direction REAL,
    tertiary_swell_height_ft REAL,
    tertiary_swell_period_s REAL,
    tertiary_swell_direction REAL,

    -- Surf height (calculated from swells)
    surf_height_min_ft REAL,
    surf_height_max_ft REAL,

    -- Wave energy (calculated)
    wave_energy_kj REAL,

    -- Wind data (from NOAA GFSwave)
    wind_speed_mph REAL,
    wind_direction_deg REAL,
    wind_gust_mph REAL,

    -- Atmospheric data (from GFS Atmospheric)
    temperature REAL,
    weather INTEGER,  -- WMO weather code
    pressure_inhg REAL,

    -- Ocean data (from NOAA CO-OPS)
    tide_level_ft REAL,
    water_temp_f REAL,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    UNIQUE(grid_id, timestamp),

    -- Foreign key to grid_points
    CONSTRAINT fk_grid_forecast_grid_id
        FOREIGN KEY (grid_id)
        REFERENCES grid_points(id)
        ON DELETE CASCADE
);

-- Create indices for performance
CREATE INDEX IF NOT EXISTS idx_grid_forecast_grid_id ON grid_forecast_data(grid_id);
CREATE INDEX IF NOT EXISTS idx_grid_forecast_timestamp ON grid_forecast_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_grid_forecast_grid_timestamp ON grid_forecast_data(grid_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_grid_forecast_created_at ON grid_forecast_data(created_at);

-- Add comments for documentation
COMMENT ON TABLE grid_forecast_data IS 'Forecast data per grid point (vs per beach). Stores wave, wind, weather, and ocean conditions from NOAA sources.';
COMMENT ON COLUMN grid_forecast_data.grid_id IS 'Foreign key to grid_points.id - the nearshore grid point for this forecast';
COMMENT ON COLUMN grid_forecast_data.timestamp IS 'Forecast timestamp in Pacific time (3-hour intervals: 00:00, 03:00, 06:00, etc.)';
COMMENT ON COLUMN grid_forecast_data.primary_swell_height_ft IS 'Height of primary (dominant) swell in feet';
COMMENT ON COLUMN grid_forecast_data.primary_swell_period_s IS 'Period of primary swell in seconds';
COMMENT ON COLUMN grid_forecast_data.primary_swell_direction IS 'Direction primary swell is coming FROM in degrees (0=N, 90=E, 180=S, 270=W)';
COMMENT ON COLUMN grid_forecast_data.surf_height_min_ft IS 'Minimum surf height in feet';
COMMENT ON COLUMN grid_forecast_data.surf_height_max_ft IS 'Maximum surf height in feet';
COMMENT ON COLUMN grid_forecast_data.wave_energy_kj IS 'Wave energy in kilojoules per square meter';
COMMENT ON COLUMN grid_forecast_data.wind_speed_mph IS 'Wind speed in miles per hour';
COMMENT ON COLUMN grid_forecast_data.wind_direction_deg IS 'Direction wind is coming FROM in degrees';
COMMENT ON COLUMN grid_forecast_data.wind_gust_mph IS 'Wind gust speed in miles per hour';
COMMENT ON COLUMN grid_forecast_data.temperature IS 'Air temperature in degrees Fahrenheit';
COMMENT ON COLUMN grid_forecast_data.weather IS 'WMO weather code (0=clear, 1=mainly clear, 2=partly cloudy, 3=overcast, etc.)';
COMMENT ON COLUMN grid_forecast_data.pressure_inhg IS 'Atmospheric pressure in inches of mercury';
COMMENT ON COLUMN grid_forecast_data.tide_level_ft IS 'Tide level in feet (MLLW datum)';
COMMENT ON COLUMN grid_forecast_data.water_temp_f IS 'Water temperature in degrees Fahrenheit';

-- Enable Row Level Security (optional - adjust policies as needed)
-- ALTER TABLE grid_forecast_data ENABLE ROW LEVEL SECURITY;

-- Create policy for public read access (optional)
-- CREATE POLICY "Public read access" ON grid_forecast_data
--     FOR SELECT USING (true);
