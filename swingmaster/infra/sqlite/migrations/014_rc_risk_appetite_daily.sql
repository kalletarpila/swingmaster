CREATE TABLE IF NOT EXISTS rc_risk_appetite_daily (
  as_of_date TEXT PRIMARY KEY,
  btc_ref_5d REAL,
  btc_ma90 REAL,
  btc_mom REAL,
  bitcoin_score REAL,
  hy_spread_5d REAL,
  credit_score REAL,
  pcr_10d REAL,
  pcr_score REAL,
  walcl_latest REAL,
  walcl_13w_ago REAL,
  liquidity_change_13w REAL,
  liquidity_score REAL,
  dxy_ref_5d REAL,
  dxy_ma200 REAL,
  dxy_diff REAL,
  dxy_score REAL,
  risk_score_raw REAL,
  risk_score_final REAL,
  regime_label TEXT,
  regime_label_confirmed TEXT,
  data_quality_status TEXT NOT NULL,
  component_count INTEGER NOT NULL,
  run_id TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  CHECK (regime_label IN ('RISK_OFF', 'DEFENSIVE', 'NEUTRAL', 'RISK_ON', 'EUPHORIC') OR regime_label IS NULL),
  CHECK (
    regime_label_confirmed IN ('RISK_OFF', 'DEFENSIVE', 'NEUTRAL', 'RISK_ON', 'EUPHORIC')
    OR regime_label_confirmed IS NULL
  ),
  CHECK (
    data_quality_status IN ('OK', 'PARTIAL_FORWARD_FILL', 'MISSING_COMPONENT', 'INVALID_SOURCE_VALUE')
  )
);

CREATE INDEX IF NOT EXISTS idx_rc_risk_appetite_daily_created_at
ON rc_risk_appetite_daily(created_at_utc);
