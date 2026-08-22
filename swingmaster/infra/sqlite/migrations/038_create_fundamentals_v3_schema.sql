PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS v3_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS v3_run (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    plan_version TEXT,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    notes TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS v3_company (
    company_id INTEGER PRIMARY KEY,
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    profile TEXT NOT NULL DEFAULT 'ORDINARY' CHECK (profile IN ('ORDINARY')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    admission_source TEXT NOT NULL,
    admission_evidence TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (market, ticker)
);

CREATE INDEX IF NOT EXISTS idx_v3_company_active
ON v3_company(market, active);

CREATE TABLE IF NOT EXISTS v3_provider_symbol_alias (
    alias_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    provider TEXT NOT NULL CHECK (provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')),
    provider_symbol TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, provider, provider_symbol)
);

CREATE TABLE IF NOT EXISTS v3_quarter (
    quarter_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter TEXT NOT NULL CHECK (fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),
    period_end_date TEXT,
    publish_date TEXT,
    market_availability_date TEXT,
    q_lifecycle TEXT NOT NULL CHECK (q_lifecycle IN ('RESULT_DETECTED', 'ENRICHING', 'OPERATIONALLY_SETTLED')),
    sec_confirmation_state TEXT NOT NULL DEFAULT 'NOT_DERIVABLE' CHECK (
        sec_confirmation_state IN (
            'NOT_APPLICABLE', 'NOT_YET_EXPECTED', 'PENDING', 'CHECKED_NO_EVIDENCE',
            'PARTIAL_EVIDENCE', 'CONFIRMED', 'UNSUPPORTED', 'ERROR_RETRY', 'NOT_DERIVABLE'
        )
    ),
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, fiscal_year, fiscal_quarter)
);

CREATE INDEX IF NOT EXISTS idx_v3_quarter_company_period
ON v3_quarter(company_id, fiscal_year, fiscal_quarter);

CREATE TABLE IF NOT EXISTS v3_quarter_fundamentals (
    quarter_id INTEGER PRIMARY KEY REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    revenue REAL,
    ebitda REAL,
    free_cashflow REAL,
    cash REAL,
    total_debt REAL,
    shares_outstanding REAL,
    ebit REAL,
    operating_income REAL,
    operating_cashflow REAL,
    capex REAL,
    gross_profit REAL,
    net_income REAL,
    currency TEXT,
    accepted_source_provider TEXT CHECK (
        accepted_source_provider IS NULL
        OR accepted_source_provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')
    ),
    accepted_at_utc TEXT,
    update_run_id TEXT,
    derivation_method TEXT,
    resolution_issue_id INTEGER,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS v3_provider_q_acquisition (
    acquisition_id INTEGER PRIMARY KEY,
    quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    provider TEXT NOT NULL CHECK (provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')),
    acquisition_result TEXT NOT NULL CHECK (
        acquisition_result IN ('NOT_CHECKED', 'ACQUIRED', 'PARTIAL', 'NO_DATA', 'FAILED', 'UNSUPPORTED')
    ),
    last_checked_at_utc TEXT,
    last_success_at_utc TEXT,
    next_retry_at_utc TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    usable_field_count INTEGER NOT NULL DEFAULT 0,
    provider_cache_ref TEXT,
    last_error_code TEXT,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (quarter_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_v3_provider_q_acquisition_due
ON v3_provider_q_acquisition(provider, acquisition_result, next_retry_at_utc);

CREATE TABLE IF NOT EXISTS v3_result_calendar (
    calendar_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    provider TEXT NOT NULL CHECK (provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')),
    provider_event_key TEXT,
    fiscal_year INTEGER,
    fiscal_quarter TEXT CHECK (fiscal_quarter IS NULL OR fiscal_quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),
    expected_result_date TEXT,
    calendar_status TEXT NOT NULL,
    source_observed_at_utc TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, provider, provider_event_key)
);

CREATE INDEX IF NOT EXISTS idx_v3_result_calendar_expected
ON v3_result_calendar(provider, expected_result_date, calendar_status);

CREATE TABLE IF NOT EXISTS v3_operational_action (
    action_id INTEGER PRIMARY KEY,
    action_type TEXT NOT NULL CHECK (
        action_type IN (
            'CHECK_RESULT', 'FETCH_INITIAL', 'ENRICH_Q', 'RETRY_PROVIDER',
            'CHECK_SEC', 'BACKFILL_HISTORICAL', 'MANUAL_REVIEW'
        )
    ),
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    provider TEXT CHECK (provider IS NULL OR provider IN ('YAHOO', 'LEGACY', 'V2', 'SEC', 'SIMFIN')),
    due_at_utc TEXT,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DEFERRED', 'BLOCKED', 'RESOLVED', 'CANCELLED')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    details_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_v3_operational_action_open_semantic
ON v3_operational_action(action_type, company_id, COALESCE(quarter_id, -1), COALESCE(provider, ''))
WHERE status IN ('ACTIVE', 'DEFERRED', 'BLOCKED');

CREATE INDEX IF NOT EXISTS idx_v3_operational_action_due
ON v3_operational_action(status, due_at_utc);

CREATE TABLE IF NOT EXISTS v3_event (
    event_id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    company_id INTEGER REFERENCES v3_company(company_id) ON DELETE SET NULL,
    quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL,
    run_id TEXT,
    occurred_at_utc TEXT NOT NULL,
    details_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_v3_event_quarter
ON v3_event(quarter_id, occurred_at_utc);

CREATE TABLE IF NOT EXISTS v3_ttm (
    ttm_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    as_of_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    model_version TEXT NOT NULL,
    ttm_ready INTEGER NOT NULL CHECK (ttm_ready IN (0, 1)),
    revenue_ttm REAL,
    ebitda_ttm REAL,
    fcf_ttm REAL,
    net_debt REAL,
    output_json TEXT,
    run_id TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, as_of_quarter_id, model_version)
);

CREATE TABLE IF NOT EXISTS v3_score (
    score_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    as_of_quarter_id INTEGER NOT NULL REFERENCES v3_quarter(quarter_id) ON DELETE CASCADE,
    score_model_version TEXT NOT NULL,
    score_ready INTEGER NOT NULL CHECK (score_ready IN (0, 1)),
    fundamental_score REAL,
    output_json TEXT,
    run_id TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, as_of_quarter_id, score_model_version)
);

CREATE TABLE IF NOT EXISTS v3_valuation (
    valuation_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    valuation_date TEXT NOT NULL,
    model_version TEXT NOT NULL,
    valuation_ready INTEGER NOT NULL CHECK (valuation_ready IN (0, 1)),
    output_json TEXT,
    run_id TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, valuation_date, model_version)
);

CREATE TABLE IF NOT EXISTS v3_migration_audit (
    audit_id INTEGER PRIMARY KEY,
    migration_run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    company_id INTEGER REFERENCES v3_company(company_id) ON DELETE SET NULL,
    quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL,
    audit_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    evidence_json TEXT,
    created_at_utc TEXT NOT NULL,
    UNIQUE (migration_run_id, source, source_key, audit_type)
);

CREATE INDEX IF NOT EXISTS idx_v3_migration_audit_source
ON v3_migration_audit(source, decision);

CREATE TABLE IF NOT EXISTS v3_resolution_issue (
    issue_id INTEGER PRIMARY KEY,
    quarter_id INTEGER REFERENCES v3_quarter(quarter_id) ON DELETE SET NULL,
    unresolved_market TEXT,
    unresolved_ticker TEXT,
    unresolved_fiscal_year INTEGER,
    unresolved_fiscal_quarter TEXT,
    issue_type TEXT NOT NULL,
    field_name TEXT,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DEFERRED', 'BLOCKED', 'RESOLVED', 'CANCELLED')),
    source_details_json TEXT,
    resolution TEXT,
    created_at_utc TEXT NOT NULL,
    resolved_at_utc TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_v3_resolution_issue_status
ON v3_resolution_issue(status, issue_type);

INSERT OR IGNORE INTO v3_schema_version (version, applied_at_utc)
VALUES (1, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
