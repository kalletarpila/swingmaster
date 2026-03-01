-- Add rc_transactions_simu table for simulated transaction persistence.

CREATE TABLE IF NOT EXISTS rc_transactions_simu (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticker TEXT NOT NULL,
  market TEXT NOT NULL,
  buy_date TEXT NOT NULL,
  buy_price REAL NOT NULL,
  buy_qty INTEGER NOT NULL CHECK (buy_qty > 0),
  buy_rule_hit TEXT,
  sell_date TEXT,
  sell_price REAL,
  sell_qty INTEGER CHECK (sell_qty IS NULL OR sell_qty > 0),
  sell_reason TEXT,
  holding_trading_days INTEGER CHECK (holding_trading_days IS NULL OR holding_trading_days >= 0),
  run_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (ticker, buy_date, run_id)
);

-- Manual verification:
-- sqlite3 /path/to/db ".schema rc_transactions_simu"
-- sqlite3 /path/to/db "PRAGMA table_info(rc_transactions_simu);"
