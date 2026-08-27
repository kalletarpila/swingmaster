PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS v3_company_fiscal_calendar_profile (
    profile_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    calendar_type TEXT NOT NULL CHECK (calendar_type IN ('CALENDAR_YEAR','FIXED_DATE_FISCAL_YEAR','WEEK_BASED_52_53','OTHER_VERIFIED','UNKNOWN')),
    start_basis TEXT NOT NULL CHECK (start_basis IN ('FIXED_DATE','WEEKDAY_NEAR_DATE','OTHER')),
    reference_month INTEGER,
    reference_day INTEGER,
    anchor_weekday TEXT,
    relative_position_rule TEXT,
    supports_52_53_week INTEGER NOT NULL CHECK (supports_52_53_week IN (0,1)),
    fiscal_year_label_convention TEXT NOT NULL,
    typical_start_description_raw TEXT NOT NULL,
    profile_parse_status TEXT NOT NULL CHECK (profile_parse_status IN ('PARSED','UNPARSED')),
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    confidence TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id)
);

CREATE TABLE IF NOT EXISTS v3_company_fiscal_year_calendar (
    anchor_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL REFERENCES v3_company(company_id) ON DELETE CASCADE,
    fiscal_year INTEGER NOT NULL,
    fiscal_year_start_date TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    confidence TEXT NOT NULL,
    verification_status TEXT NOT NULL,
    import_state TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    UNIQUE (company_id, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_v3_company_fiscal_year_calendar_start
ON v3_company_fiscal_year_calendar(fiscal_year_start_date);
