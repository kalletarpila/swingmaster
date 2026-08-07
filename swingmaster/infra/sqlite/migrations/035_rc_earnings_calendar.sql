CREATE TABLE IF NOT EXISTS rc_earnings_calendar (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    estimated_announcement_at TEXT,
    estimated_announcement_date TEXT,
    estimated_session TEXT NOT NULL DEFAULT 'UNKNOWN',
    calendar_status TEXT NOT NULL,
    source TEXT NOT NULL,
    source_observed_at_utc TEXT NOT NULL,
    first_observed_at_utc TEXT NOT NULL,
    last_observed_at_utc TEXT NOT NULL,
    previous_estimated_announcement_at TEXT,
    date_change_count INTEGER NOT NULL DEFAULT 0,
    completed_earnings_event_id INTEGER,
    calendar_last_checked_at_utc TEXT,
    calendar_check_status TEXT,
    calendar_last_failed_at_utc TEXT,
    calendar_failure_count INTEGER NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, source)
);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_market_status
ON rc_earnings_calendar(market, calendar_status);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_ticker
ON rc_earnings_calendar(ticker);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_estimated_date
ON rc_earnings_calendar(estimated_announcement_date);

CREATE INDEX IF NOT EXISTS idx_earnings_calendar_completed_event
ON rc_earnings_calendar(completed_earnings_event_id);
