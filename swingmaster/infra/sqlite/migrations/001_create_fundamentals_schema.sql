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

CREATE TABLE IF NOT EXISTS rc_fundamental_quarter_ingestion_status (
    id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    earnings_event_id INTEGER,
    announcement_date TEXT,
    effective_trading_date TEXT,
    ingestion_status TEXT NOT NULL,
    basic_status TEXT NOT NULL,
    quarter_basic_complete INTEGER NOT NULL DEFAULT 0,
    ttm_input_complete INTEGER NOT NULL DEFAULT 0,
    score_history_complete INTEGER NOT NULL DEFAULT 0,
    valuation_input_ready INTEGER NOT NULL DEFAULT 0,
    historical_research_ready INTEGER NOT NULL DEFAULT 0,
    available_basic_field_count INTEGER NOT NULL DEFAULT 0,
    missing_basic_fields TEXT NOT NULL DEFAULT '[]',
    missing_core_fields_json TEXT NOT NULL DEFAULT '[]',
    missing_ttm_fields_json TEXT NOT NULL DEFAULT '[]',
    missing_score_fields_json TEXT NOT NULL DEFAULT '[]',
    data_quality_warnings_json TEXT NOT NULL DEFAULT '[]',
    supported_source_field_count INTEGER,
    source_non_null_field_count INTEGER,
    persisted_matching_field_count INTEGER,
    retry_recommendation TEXT NOT NULL,
    last_fetch_status TEXT,
    last_fetch_source TEXT,
    last_source_observed_at_utc TEXT,
    last_checked_at_utc TEXT NOT NULL,
    assessment_policy_version TEXT NOT NULL,
    ingestion_evidence_type TEXT NOT NULL,
    run_id TEXT NOT NULL,
    assessed_at_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker, period_end_date)
);

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
    net_debt_to_ebit REAL,
    share_dilution_yoy REAL,
    lifecycle_class TEXT,
    growth_component REAL,
    margin_component REAL,
    margin_trend_component REAL,
    fcf_component REAL,
    leverage_component REAL,
    dilution_component REAL,
    lifecycle_component REAL,
    consistency_component REAL,
    score_rule TEXT,
    fundamental_score_lifecycle REAL,
    score_rule_lifecycle TEXT,
    growth_component_lifecycle REAL,
    margin_component_lifecycle REAL,
    margin_trend_component_lifecycle REAL,
    fcf_component_lifecycle REAL,
    leverage_component_lifecycle REAL,
    dilution_component_lifecycle REAL,
    lifecycle_component_lifecycle REAL,
    consistency_component_lifecycle REAL,
    fundamental_score REAL,
    run_id TEXT NOT NULL,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_ticker
ON rc_fundamental_ttm(ticker);

CREATE INDEX IF NOT EXISTS idx_fundamental_ttm_as_of_date
ON rc_fundamental_ttm(as_of_date);

CREATE TABLE IF NOT EXISTS rc_fundamental_score_percentile (
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    target_date TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    rule_id TEXT NOT NULL,
    run_id TEXT NOT NULL,

    universe_size INTEGER NOT NULL,
    sector_size INTEGER,
    industry_size INTEGER,

    growth_pct_global REAL,
    growth_pct_sector REAL,
    growth_pct_industry REAL,

    margin_pct_global REAL,
    margin_pct_sector REAL,
    margin_pct_industry REAL,

    margin_trend_pct_global REAL,
    margin_trend_pct_sector REAL,
    margin_trend_pct_industry REAL,

    fcf_pct_global REAL,
    fcf_pct_sector REAL,
    fcf_pct_industry REAL,

    leverage_pct_global REAL,
    leverage_pct_sector REAL,
    leverage_pct_industry REAL,

    dilution_pct_global REAL,
    dilution_pct_sector REAL,
    dilution_pct_industry REAL,

    consistency_pct_global REAL,
    consistency_pct_sector REAL,
    consistency_pct_industry REAL,

    fundamental_score_percentile_global REAL,
    fundamental_score_percentile_sector REAL,
    fundamental_score_percentile_industry REAL,
    fundamental_score_percentile_blended REAL,
    sector_rank_blended INTEGER,
    industry_rank_blended INTEGER,
    fundamental_score_percentile_global_lifecycle_weighted REAL,
    fundamental_score_percentile_sector_lifecycle_weighted REAL,
    fundamental_score_percentile_industry_lifecycle_weighted REAL,
    fundamental_score_percentile_blended_lifecycle_weighted REAL,
    sector_rank_blended_lifecycle_weighted INTEGER,
    industry_rank_blended_lifecycle_weighted INTEGER,
    percentile_lifecycle_weight_rule TEXT,

    created_at_utc TEXT NOT NULL,

    PRIMARY KEY (ticker, target_date, rule_id)
);

CREATE INDEX IF NOT EXISTS idx_rc_fund_score_pct_target
ON rc_fundamental_score_percentile(target_date);

CREATE INDEX IF NOT EXISTS idx_rc_fund_score_pct_rule_run
ON rc_fundamental_score_percentile(rule_id, run_id);

CREATE INDEX IF NOT EXISTS idx_rc_fund_score_pct_sector
ON rc_fundamental_score_percentile(target_date, sector);

CREATE INDEX IF NOT EXISTS idx_rc_fund_score_pct_industry
ON rc_fundamental_score_percentile(target_date, industry);

CREATE VIEW IF NOT EXISTS rc_fundamental_latest AS
SELECT t.*
FROM rc_fundamental_ttm t
JOIN (
    SELECT ticker, MAX(as_of_date) AS as_of_date
    FROM rc_fundamental_ttm
    GROUP BY ticker
) latest
  ON t.ticker = latest.ticker
 AND t.as_of_date = latest.as_of_date;

INSERT INTO rc_fundamental_schema_version (version, applied_at_utc)
SELECT 1, CURRENT_TIMESTAMP
WHERE NOT EXISTS (
    SELECT 1
    FROM rc_fundamental_schema_version
    WHERE version = 1
);
