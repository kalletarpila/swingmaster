CREATE TABLE IF NOT EXISTS rc_macro_source_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_key TEXT NOT NULL,
  vendor TEXT NOT NULL,
  external_series_id TEXT NOT NULL,
  observation_date TEXT NOT NULL,
  raw_value REAL,
  raw_value_text TEXT,
  source_url TEXT NOT NULL,
  loaded_at_utc TEXT NOT NULL,
  run_id TEXT NOT NULL,
  UNIQUE (source_key, observation_date)
);

CREATE INDEX IF NOT EXISTS idx_rc_macro_source_raw_obs_date
ON rc_macro_source_raw(observation_date);

CREATE INDEX IF NOT EXISTS idx_rc_macro_source_raw_vendor_series_date
ON rc_macro_source_raw(vendor, external_series_id, observation_date);
