CREATE TABLE IF NOT EXISTS rc_fundamental_quarterly_vintage (
    ticker TEXT NOT NULL,
    market TEXT,
    period_end_date TEXT NOT NULL,
    statement_vintage_id TEXT NOT NULL,

    source_provider TEXT NOT NULL,
    source_document_id TEXT,
    source_hash TEXT,
    revision_number INTEGER NOT NULL DEFAULT 1,
    is_restated INTEGER NOT NULL DEFAULT 0,
    supersedes_vintage_id TEXT,
    availability_quality TEXT NOT NULL DEFAULT 'ESTIMATED',

    filed_at_utc TEXT,
    available_at_utc TEXT NOT NULL,
    ingested_at_utc TEXT NOT NULL,
    provider_observed_at_utc TEXT,

    run_id TEXT,
    provider_run_id TEXT,
    normalization_run_id TEXT,
    enrichment_run_id TEXT,

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

    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT,

    PRIMARY KEY (ticker, period_end_date, statement_vintage_id)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_vintage_ticker_period
ON rc_fundamental_quarterly_vintage(ticker, period_end_date);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_vintage_ticker_available
ON rc_fundamental_quarterly_vintage(ticker, available_at_utc);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_vintage_ticker_period_available
ON rc_fundamental_quarterly_vintage(ticker, period_end_date, available_at_utc);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_vintage_market_ticker_period
ON rc_fundamental_quarterly_vintage(market, ticker, period_end_date);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_vintage_source_hash
ON rc_fundamental_quarterly_vintage(source_provider, source_hash);

CREATE TABLE IF NOT EXISTS rc_fundamental_quarterly_field_provenance (
    ticker TEXT NOT NULL,
    market TEXT,
    period_end_date TEXT NOT NULL,
    statement_vintage_id TEXT NOT NULL,
    field_name TEXT NOT NULL,

    field_value REAL,
    source_provider TEXT NOT NULL,
    source_table TEXT,
    source_row_ref TEXT,
    source_document_id TEXT,
    source_hash TEXT,

    provenance_role TEXT NOT NULL,
    merge_action TEXT NOT NULL,
    old_value REAL,
    new_value REAL,

    available_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    run_id TEXT,
    enrichment_run_id TEXT,

    PRIMARY KEY (
        ticker,
        period_end_date,
        statement_vintage_id,
        field_name,
        source_provider,
        provenance_role
    )
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_field_prov_vintage
ON rc_fundamental_quarterly_field_provenance(ticker, period_end_date, statement_vintage_id);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_field_prov_source_hash
ON rc_fundamental_quarterly_field_provenance(source_provider, source_hash);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_field_prov_run_id
ON rc_fundamental_quarterly_field_provenance(run_id);
