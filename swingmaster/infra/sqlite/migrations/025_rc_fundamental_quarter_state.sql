CREATE TABLE IF NOT EXISTS rc_fundamental_quarter_state (
    ticker TEXT NOT NULL,
    market TEXT NOT NULL,
    primary_source TEXT NOT NULL,
    latest_db_period_end_date TEXT,
    detected_source_period_end_date TEXT,
    new_quarter_available INTEGER NOT NULL DEFAULT 0,
    last_checked_at_utc TEXT,
    last_updated_at_utc TEXT NOT NULL,
    last_detection_run_id TEXT,
    last_ingest_run_id TEXT,
    PRIMARY KEY (ticker)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarter_state_market
ON rc_fundamental_quarter_state(market);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarter_state_new_quarter
ON rc_fundamental_quarter_state(new_quarter_available);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarter_state_market_new_quarter
ON rc_fundamental_quarter_state(market, new_quarter_available);
