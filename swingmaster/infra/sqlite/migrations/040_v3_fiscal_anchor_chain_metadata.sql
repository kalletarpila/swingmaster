PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS v3_company_fiscal_anchor_chain (
    company_id INTEGER PRIMARY KEY REFERENCES v3_company(company_id) ON DELETE CASCADE,
    chain_status TEXT NOT NULL,
    break_reason TEXT NOT NULL CHECK (break_reason IN (
        'SOURCE_HISTORY_EXHAUSTED',
        'UNRESOLVED_BOUNDARY',
        'CALENDAR_TRANSITION',
        'NO_FISCAL_YEAR',
        'COMPLETE_TO_FY1999'
    )),
    earliest_verified_fiscal_year INTEGER,
    latest_verified_fiscal_year INTEGER,
    populated_anchor_count INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);
