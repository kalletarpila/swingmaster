CREATE TABLE IF NOT EXISTS rc_earnings_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    announcement_at TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    announcement_session TEXT NOT NULL,
    is_reported INTEGER NOT NULL,
    reported_eps REAL,
    estimated_eps REAL,
    surprise_pct REAL,
    source TEXT NOT NULL,
    source_observed_at_utc TEXT NOT NULL,
    source_timezone TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, announcement_at, source)
);

CREATE INDEX IF NOT EXISTS idx_rc_earnings_event_ticker_date
ON rc_earnings_event(ticker, announcement_date);

CREATE INDEX IF NOT EXISTS idx_rc_earnings_event_announcement_date
ON rc_earnings_event(announcement_date);

CREATE INDEX IF NOT EXISTS idx_rc_earnings_event_reported
ON rc_earnings_event(is_reported, announcement_date);

CREATE INDEX IF NOT EXISTS idx_rc_earnings_event_source
ON rc_earnings_event(source);
