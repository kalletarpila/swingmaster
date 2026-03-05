-- Canonical dual-score source tables stored inside RC DB for production flow.

CREATE TABLE IF NOT EXISTS rc_episode_model_inference_rank_meta_v1 (
  episode_id TEXT PRIMARY KEY,
  score_meta_v1_up20_60d_close REAL NOT NULL,
  model_version TEXT NOT NULL,
  computed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rc_episode_model_full_inference_no_dow_scores_hgb_fail10 (
  episode_id TEXT PRIMARY KEY,
  score_pred REAL NOT NULL,
  model_version TEXT NOT NULL,
  computed_at TEXT NOT NULL
);

