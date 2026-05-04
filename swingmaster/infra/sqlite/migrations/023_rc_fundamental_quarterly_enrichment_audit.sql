CREATE TABLE IF NOT EXISTS rc_fundamental_quarterly_enrichment_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    period_end_date TEXT NOT NULL,
    field_name TEXT NOT NULL,
    old_value REAL,
    new_value REAL,
    primary_source TEXT NOT NULL,
    fallback_source TEXT NOT NULL,
    enrichment_status TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_enrichment_audit_ticker_period
ON rc_fundamental_quarterly_enrichment_audit(ticker, period_end_date);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_enrichment_audit_field_name
ON rc_fundamental_quarterly_enrichment_audit(field_name);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_enrichment_audit_run_id
ON rc_fundamental_quarterly_enrichment_audit(run_id);

CREATE INDEX IF NOT EXISTS idx_fundamental_quarterly_enrichment_audit_status
ON rc_fundamental_quarterly_enrichment_audit(enrichment_status);
