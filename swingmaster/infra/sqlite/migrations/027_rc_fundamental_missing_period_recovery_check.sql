CREATE TABLE IF NOT EXISTS rc_fundamental_missing_period_recovery_check (
    ticker TEXT NOT NULL,
    market TEXT NOT NULL,
    classification_run_id TEXT NOT NULL,
    classification_as_of_date TEXT NOT NULL,
    missing_period_end_date TEXT NOT NULL,
    recovery_status TEXT NOT NULL,
    has_core_fields INTEGER NOT NULL,
    found_period_end_dates TEXT NOT NULL,
    reason TEXT NOT NULL,
    checked_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY (
        ticker,
        market,
        classification_run_id,
        missing_period_end_date,
        run_id
    )
);

CREATE INDEX IF NOT EXISTS idx_missing_period_recovery_run_id
ON rc_fundamental_missing_period_recovery_check (run_id);

CREATE INDEX IF NOT EXISTS idx_missing_period_recovery_market_status
ON rc_fundamental_missing_period_recovery_check (market, recovery_status);

CREATE INDEX IF NOT EXISTS idx_missing_period_recovery_ticker_market
ON rc_fundamental_missing_period_recovery_check (ticker, market);

CREATE INDEX IF NOT EXISTS idx_missing_period_recovery_classification_run
ON rc_fundamental_missing_period_recovery_check (classification_run_id);
