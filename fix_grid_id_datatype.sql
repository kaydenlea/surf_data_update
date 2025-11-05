-- Fix grid_id column datatype in beaches table
-- The grid_points table uses INTEGER id (SERIAL), so grid_id should also be INTEGER

-- Drop the existing grid_id column if it has wrong type (UUID)
ALTER TABLE beaches DROP COLUMN IF EXISTS grid_id;

-- Add grid_id column as INTEGER to match grid_points.id type
ALTER TABLE beaches ADD COLUMN grid_id INTEGER;

-- Add foreign key constraint
ALTER TABLE beaches
  ADD CONSTRAINT fk_beaches_grid_id
  FOREIGN KEY (grid_id)
  REFERENCES grid_points(id)
  ON DELETE SET NULL;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_beaches_grid_id ON beaches(grid_id);

-- Add comment
COMMENT ON COLUMN beaches.grid_id IS 'Foreign key to grid_points.id - the nearest nearshore grid point for this beach';
