CREATE TABLE IF NOT EXISTS rc_fundamental_valuation (
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    valuation_ev_ebit REAL,
    valuation_bucket TEXT NOT NULL,
    valuation_status TEXT NOT NULL,
    market_cap REAL,
    enterprise_value REAL,
    close_price REAL,
    shares_outstanding REAL,
    cash REAL,
    total_debt REAL,
    ebit_ttm REAL,
    fundamental_score_lifecycle REAL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_valuation_ticker
ON rc_fundamental_valuation(ticker);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_valuation_as_of_date
ON rc_fundamental_valuation(as_of_date);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_valuation_bucket
ON rc_fundamental_valuation(valuation_bucket);

CREATE INDEX IF NOT EXISTS idx_rc_fundamental_valuation_run_id
ON rc_fundamental_valuation(run_id);
