PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS rc_fundamental_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rc_fundamental_run (
    run_id TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    tickers_total INTEGER,
    tickers_processed INTEGER,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS rc_fundamental_statement_raw (
    ticker TEXT NOT NULL,
    statement_type TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    period_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    field_value REAL,
    currency TEXT,
    source TEXT NOT NULL,
    retrieved_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (ticker, statement_type, period_end_date, field_name)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_raw_ticker
ON rc_fundamental_statement_raw(ticker);

CREATE INDEX IF NOT EXISTS idx_fundamental_raw_period
ON rc_fundamental_statement_raw(period_end_date);

CREATE TABLE IF NOT EXISTS rc_fundamental_quarterly (
    ticker TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    revenue REAL,
    gross_profit REAL,
    operating_income REAL,
    ebit REAL,
    ebitda REAL,
    net_income REAL,
    operating_cashflow REAL,
    capex REAL,
    free_cashflow REAL,
    cash REAL,
    total_debt REAL,
    shares_outstanding REAL,
    currency TEXT,
    run_id TEXT NOT NULL,
    PRIMARY KEY (ticker, period_end_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_ticker
ON rc_fundamental_quarterly(ticker);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_period
ON rc_fundamental_quarterly(period_end_date);

CREATE TABLE IF NOT EXISTS rc_fundamental_ttm (
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    latest_period_end_date TEXT NOT NULL,
    revenue_ttm REAL,
    revenue_growth_ttm_yoy REAL,
    ebit_ttm REAL,
    ebit_growth_ttm_yoy REAL,
    ebit_margin_ttm REAL,
    ebit_margin_trend_4q REAL,
    gross_margin_trend_4q REAL,
    fcf_ttm REAL,
    fcf_margin_ttm REAL,
    fcf_margin_trend_4q REAL,
    net_debt REAL,
    net_debt_to_ebitda REAL,
    share_dilution_yoy REAL,
    lifecycle_class TEXT,
    fundamental_score REAL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_ticker
ON rc_fundamental_ttm(ticker);

CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_as_of_date
ON rc_fundamental_ttm(as_of_date);

INSERT INTO rc_fundamental_schema_version (version, applied_at_utc)
SELECT 1, CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM rc_fundamental_schema_version
    WHERE version = 1
);
