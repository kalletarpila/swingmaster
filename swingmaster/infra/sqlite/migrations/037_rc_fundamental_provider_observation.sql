CREATE TABLE IF NOT EXISTS rc_fundamental_provider_observation_content (
    id INTEGER PRIMARY KEY,
    content_key TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_key TEXT NOT NULL,
    canonical_fiscal_year INTEGER,
    canonical_fiscal_quarter TEXT,
    period_end_date TEXT,
    observation_kind TEXT NOT NULL,
    source_endpoint TEXT NOT NULL,
    provider_reported_at_utc TEXT,
    timestamp_quality TEXT NOT NULL,
    field_presence_fingerprint TEXT,
    payload_hash TEXT,
    source_reference TEXT,
    outcome TEXT NOT NULL,
    first_observed_at_utc TEXT NOT NULL,
    last_observed_at_utc TEXT NOT NULL,
    first_run_id TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    poll_count INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_fund_provider_obs_content_company_quarter
ON rc_fundamental_provider_observation_content(market, ticker, canonical_fiscal_year, canonical_fiscal_quarter);

CREATE INDEX IF NOT EXISTS idx_fund_provider_obs_content_provider_kind
ON rc_fundamental_provider_observation_content(provider, observation_kind, outcome);

CREATE TABLE IF NOT EXISTS rc_fundamental_provider_observation_seen (
    id INTEGER PRIMARY KEY,
    content_id INTEGER NOT NULL,
    observed_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (content_id) REFERENCES rc_fundamental_provider_observation_content(id)
);

CREATE INDEX IF NOT EXISTS idx_fund_provider_obs_seen_content
ON rc_fundamental_provider_observation_seen(content_id, observed_at_utc);

CREATE INDEX IF NOT EXISTS idx_fund_provider_obs_seen_run
ON rc_fundamental_provider_observation_seen(run_id);
