CREATE TABLE IF NOT EXISTS rc_fundamental_quarter_earnings_match (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    earnings_event_id INTEGER NOT NULL,
    announcement_at TEXT NOT NULL,
    announcement_date TEXT NOT NULL,
    announcement_session TEXT NOT NULL,
    effective_trading_date TEXT,
    effective_date_status TEXT NOT NULL,
    reporting_delay_days INTEGER NOT NULL,
    matching_status TEXT NOT NULL,
    matching_confidence TEXT NOT NULL,
    matching_method TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    availability_policy TEXT NOT NULL,
    matcher_version TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, period_end_date),
    UNIQUE (market, ticker, earnings_event_id),
    FOREIGN KEY (earnings_event_id) REFERENCES rc_earnings_event(id)
);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_qem_ticker_period
ON rc_fundamental_quarter_earnings_match(ticker, period_end_date);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_qem_effective_date
ON rc_fundamental_quarter_earnings_match(effective_trading_date);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_qem_earnings_event
ON rc_fundamental_quarter_earnings_match(earnings_event_id);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_qem_status_confidence
ON rc_fundamental_quarter_earnings_match(matching_status, matching_confidence);

