CREATE TABLE IF NOT EXISTS rc_market_regime_daily (
  trade_date TEXT NOT NULL,
  market TEXT NOT NULL,
  regime_version TEXT NOT NULL,
  sp500_close REAL NOT NULL,
  sp500_ma50 REAL NOT NULL,
  sp500_ma200 REAL NOT NULL,
  sp500_state TEXT NOT NULL CHECK (sp500_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ndx_close REAL NOT NULL,
  ndx_ma50 REAL NOT NULL,
  ndx_ma200 REAL NOT NULL,
  ndx_state TEXT NOT NULL CHECK (ndx_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  regime_combined TEXT NOT NULL CHECK (regime_combined IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  crash_confirm_days INTEGER NOT NULL,
  computed_at TEXT NOT NULL,
  PRIMARY KEY (trade_date, market, regime_version)
);

CREATE INDEX IF NOT EXISTS idx_rc_market_regime_daily_market_date
ON rc_market_regime_daily(market, trade_date);

CREATE INDEX IF NOT EXISTS idx_rc_market_regime_daily_market_version_regime_date
ON rc_market_regime_daily(market, regime_version, regime_combined, trade_date);

CREATE TABLE IF NOT EXISTS rc_episode_regime (
  episode_id TEXT NOT NULL,
  market TEXT NOT NULL,
  regime_version TEXT NOT NULL,
  ew_entry_date TEXT,
  ew_entry_regime_combined TEXT CHECK (ew_entry_regime_combined IS NULL OR ew_entry_regime_combined IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ew_entry_sp500_state TEXT CHECK (ew_entry_sp500_state IS NULL OR ew_entry_sp500_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ew_entry_ndx_state TEXT CHECK (ew_entry_ndx_state IS NULL OR ew_entry_ndx_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ew_exit_date TEXT,
  ew_exit_regime_combined TEXT CHECK (ew_exit_regime_combined IS NULL OR ew_exit_regime_combined IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ew_exit_sp500_state TEXT CHECK (ew_exit_sp500_state IS NULL OR ew_exit_sp500_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  ew_exit_ndx_state TEXT CHECK (ew_exit_ndx_state IS NULL OR ew_exit_ndx_state IN ('BULL', 'CRASH_ALERT', 'BEAR', 'SIDEWAYS')),
  computed_at TEXT NOT NULL,
  PRIMARY KEY (episode_id, regime_version)
);

CREATE INDEX IF NOT EXISTS idx_rc_episode_regime_market_version_entry_regime
ON rc_episode_regime(market, regime_version, ew_entry_regime_combined);

CREATE INDEX IF NOT EXISTS idx_rc_episode_regime_market_version_exit_regime
ON rc_episode_regime(market, regime_version, ew_exit_regime_combined);

CREATE INDEX IF NOT EXISTS idx_rc_episode_regime_market_version_entry_date
ON rc_episode_regime(market, regime_version, ew_entry_date);

CREATE INDEX IF NOT EXISTS idx_rc_episode_regime_market_version_exit_date
ON rc_episode_regime(market, regime_version, ew_exit_date);
