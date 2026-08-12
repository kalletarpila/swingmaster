CREATE TABLE IF NOT EXISTS rc_fundamental_historical_backfill_result (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    target_period_end_date TEXT NOT NULL,
    result_status TEXT NOT NULL,
    result_reason TEXT NOT NULL,
    actionable INTEGER NOT NULL DEFAULT 0,
    exhausted INTEGER NOT NULL DEFAULT 0,
    retry_after_utc TEXT,
    sec_evidence_state TEXT NOT NULL,
    yahoo_evidence_state TEXT NOT NULL,
    last_attempted_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, target_period_end_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_hbr_market_ticker_period
ON rc_fundamental_historical_backfill_result(market, ticker, target_period_end_date);

CREATE INDEX IF NOT EXISTS idx_fundamental_hbr_result_status
ON rc_fundamental_historical_backfill_result(result_status);

CREATE INDEX IF NOT EXISTS idx_fundamental_hbr_actionable
ON rc_fundamental_historical_backfill_result(actionable, exhausted);
