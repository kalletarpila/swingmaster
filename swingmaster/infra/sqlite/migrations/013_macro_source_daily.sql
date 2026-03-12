CREATE TABLE IF NOT EXISTS macro_source_daily (
  as_of_date TEXT NOT NULL,
  source_code TEXT NOT NULL,
  source_value REAL NOT NULL,
  source_value_raw_text TEXT,
  source_frequency TEXT NOT NULL,
  published_at_utc TEXT NOT NULL,
  retrieved_at_utc TEXT NOT NULL,
  revision_tag TEXT,
  run_id TEXT NOT NULL,
  PRIMARY KEY (as_of_date, source_code)
);

CREATE INDEX IF NOT EXISTS idx_macro_source_daily_source_date
ON macro_source_daily(source_code, as_of_date);
