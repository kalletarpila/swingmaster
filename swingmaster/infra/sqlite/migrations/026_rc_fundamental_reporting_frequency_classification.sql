CREATE TABLE IF NOT EXISTS rc_fundamental_reporting_frequency_classification (
    ticker TEXT NOT NULL,
    market TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    lookback_months INTEGER NOT NULL,
    reporting_frequency_class TEXT NOT NULL,
    inferred_reporting_frequency TEXT NOT NULL,
    has_valid_ttm_coverage INTEGER NOT NULL,
    reason TEXT NOT NULL,
    period_count_in_lookback INTEGER NOT NULL,
    observed_period_end_dates TEXT NOT NULL,
    expected_period_end_dates TEXT,
    missing_period_end_dates TEXT,
    missing_period_count INTEGER NOT NULL DEFAULT 0,
    source_data_max_period_end_date TEXT,
    classifier_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    PRIMARY KEY (ticker, market, as_of_date, classifier_version, run_id)
);

CREATE INDEX IF NOT EXISTS idx_reporting_frequency_classification_market_asof
ON rc_fundamental_reporting_frequency_classification(market, as_of_date);

CREATE INDEX IF NOT EXISTS idx_reporting_frequency_classification_ticker_market
ON rc_fundamental_reporting_frequency_classification(ticker, market);

CREATE INDEX IF NOT EXISTS idx_reporting_frequency_classification_run_id
ON rc_fundamental_reporting_frequency_classification(run_id);
