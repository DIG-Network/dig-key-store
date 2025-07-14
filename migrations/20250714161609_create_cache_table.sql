-- Create the cache table if it doesn't exist
CREATE TABLE IF NOT EXISTS cache (
  cache_key TEXT PRIMARY KEY,
  cache_value BLOB NOT NULL,
  expires INTEGER,  -- Unix timestamp, NULL if no expiry
  last_accessed INTEGER NOT NULL
);

-- Create indexes for efficient querying if they don't exist
CREATE INDEX IF NOT EXISTS idx_expires ON cache(expires);
CREATE INDEX IF NOT EXISTS idx_last_accessed ON cache(last_accessed);
