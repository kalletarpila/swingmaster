CREATE TABLE IF NOT EXISTS rc_fundamental_finnhub_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market TEXT NOT NULL,
    provider TEXT NOT NULL,
    symbol TEXT NOT NULL,
    description TEXT,
    currency TEXT,
    figi TEXT,
    mic TEXT,
    type TEXT,
    freq TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    loaded_at_utc TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fundamental_finnhub_raw_market_symbol
ON rc_fundamental_finnhub_raw(market, symbol);

CREATE INDEX IF NOT EXISTS idx_fundamental_finnhub_raw_run_id
ON rc_fundamental_finnhub_raw(run_id);

CREATE INDEX IF NOT EXISTS idx_fundamental_finnhub_raw_status
ON rc_fundamental_finnhub_raw(status);
