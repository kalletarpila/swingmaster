CREATE TABLE IF NOT EXISTS rc_fundamental_yahoo_quarterly (
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    revenue REAL,
    gross_profit REAL,
    operating_income REAL,
    net_income REAL,
    operating_cashflow REAL,
    capex REAL,
    free_cashflow REAL,
    cash REAL,
    total_debt REAL,
    shares_outstanding REAL,
    shares_source TEXT,
    shares_quality TEXT,
    source_run_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY (market, symbol, period_end_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_yahoo_quarterly_symbol
ON rc_fundamental_yahoo_quarterly(symbol);

CREATE INDEX IF NOT EXISTS idx_fundamental_yahoo_quarterly_run_id
ON rc_fundamental_yahoo_quarterly(run_id);
