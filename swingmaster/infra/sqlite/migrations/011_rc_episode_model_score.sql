CREATE TABLE IF NOT EXISTS rc_episode_model_score (
  episode_id TEXT NOT NULL,
  model_id TEXT NOT NULL,
  ticker TEXT NOT NULL,
  entry_window_date TEXT,
  entry_window_exit_date TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  regime_used TEXT NOT NULL,
  model_family TEXT NOT NULL,
  target_name TEXT NOT NULL,
  feature_version TEXT NOT NULL,
  regime_version TEXT,
  artifact_path TEXT,
  predicted_probability REAL NOT NULL,
  scored_at TEXT NOT NULL,
  PRIMARY KEY (episode_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_rc_episode_model_score_model_exit_date
ON rc_episode_model_score(model_id, entry_window_exit_date);

CREATE INDEX IF NOT EXISTS idx_rc_episode_model_score_ticker_asof
ON rc_episode_model_score(ticker, as_of_date);
