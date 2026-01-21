-- swingmaster initial schema

CREATE TABLE IF NOT EXISTS rc_run (
    run_id TEXT PRIMARY KEY,
    created_at TEXT,
    engine_version TEXT,
    policy_id TEXT,
    policy_version TEXT
);

CREATE TABLE IF NOT EXISTS rc_state_daily (
    ticker TEXT,
    date TEXT,
    state TEXT,
    reasons_json TEXT,
    confidence INTEGER,
    age INTEGER,
    run_id TEXT,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (run_id) REFERENCES rc_run(run_id)
);

CREATE TABLE IF NOT EXISTS rc_transition (
    ticker TEXT,
    date TEXT,
    from_state TEXT,
    to_state TEXT,
    reasons_json TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_rc_state_daily_date ON rc_state_daily(date);
CREATE INDEX IF NOT EXISTS idx_rc_transition_ticker_date ON rc_transition(ticker, date);
