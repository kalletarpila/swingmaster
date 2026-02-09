-- Add rc_signal_daily table for provider-level signal persistence

CREATE TABLE IF NOT EXISTS rc_signal_daily (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    signal_keys_json TEXT NOT NULL,
    run_id TEXT,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (run_id) REFERENCES rc_run(run_id)
);

CREATE INDEX IF NOT EXISTS idx_rc_signal_daily_date ON rc_signal_daily(date);
-- Purpose: Persist daily provider-level signal keys per ticker/date.
