-- Add deterministic episodic dual-inference source table for EW-score dual fields.

CREATE TABLE IF NOT EXISTS rc_episode_model_dual_inference_current (
  episode_id TEXT PRIMARY KEY,
  score_up20_meta_v1 REAL NOT NULL,
  score_fail10_60d_close_hgb REAL NOT NULL,
  model_version TEXT NOT NULL,
  computed_at TEXT NOT NULL
);
