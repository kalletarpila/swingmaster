CREATE TABLE IF NOT EXISTS rc_fundamental_yahoo_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market TEXT NOT NULL,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    info_json TEXT NOT NULL,
    fast_info_json TEXT NOT NULL,
    quarterly_income_stmt_json TEXT NOT NULL,
    quarterly_balance_sheet_json TEXT NOT NULL,
    quarterly_cashflow_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    loaded_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fundamental_yahoo_raw_market_symbol
ON rc_fundamental_yahoo_raw(market, symbol);

CREATE INDEX IF NOT EXISTS idx_fundamental_yahoo_raw_run_id
ON rc_fundamental_yahoo_raw(run_id);

CREATE INDEX IF NOT EXISTS idx_fundamental_yahoo_raw_status
ON rc_fundamental_yahoo_raw(status);
