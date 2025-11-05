-- Create table for California nearshore grid points
-- Stores the 94 grid points within ~25 miles of the California coast
-- These points come from the NOAA GFSwave 0.1667° resolution grid

CREATE TABLE IF NOT EXISTS grid_points (
    id SERIAL PRIMARY KEY,
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    latitude_index INTEGER NOT NULL,
    longitude_index INTEGER NOT NULL,
    region VARCHAR(50),
    distance_from_coast_miles DECIMAL(6, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure unique coordinates
    UNIQUE(latitude, longitude),

    -- Ensure unique grid indices
    UNIQUE(latitude_index, longitude_index)
);

-- Create indices for common queries
CREATE INDEX IF NOT EXISTS idx_grid_points_lat_lon ON grid_points(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_grid_points_region ON grid_points(region);

-- Add comments for documentation
COMMENT ON TABLE grid_points IS 'NOAA GFSwave grid points for California nearshore region (within ~25 miles of coast). Grid resolution: 0.1667° (~11.5 miles)';
COMMENT ON COLUMN grid_points.latitude IS 'Latitude in decimal degrees (North positive)';
COMMENT ON COLUMN grid_points.longitude IS 'Longitude in decimal degrees (East positive, 0-360 range)';
COMMENT ON COLUMN grid_points.latitude_index IS 'Index position in GFSwave latitude array';
COMMENT ON COLUMN grid_points.longitude_index IS 'Index position in GFSwave longitude array';
COMMENT ON COLUMN grid_points.region IS 'California coastal region: Northern, Central, or Southern';
COMMENT ON COLUMN grid_points.distance_from_coast_miles IS 'Approximate distance from California coastline in miles';
