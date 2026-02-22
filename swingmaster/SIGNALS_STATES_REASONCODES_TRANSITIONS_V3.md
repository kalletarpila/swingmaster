# Swingmaster V3: Signals, Thresholds, States, ReasonCodes, and Transition Contract

This is the unified V3 reference document for:
1. signal generation rules and thresholds,
2. state machine and transition rules,
3. reason code semantics,
4. V3 metadata contract in `state_attrs_json`,
5. EW scoring/runtime integration that now runs alongside V3 pipeline outputs.

## Document update provenance
- Previous update baseline: commit `4808bc32f7dd96df9be1f16b604d52df52acb01f` (2026-02-19 18:46:56 +0200)
- Current sync point: commit `b5159d19c5890e40335369e379f25cfd1fc1f7f8` (2026-02-22 09:02:42 +0200)
- This revision reflects all program changes in `4808bc3..b5159d1` that affect V3 behavior, runtime integration, scoring outputs, and reporting contracts.

## Sources (code)
- `swingmaster/core/signals/enums.py`
- `swingmaster/app_api/providers/osakedata_signal_provider_v3.py`
- `swingmaster/app_api/providers/signals_v3/*.py`
- `swingmaster/app_api/providers/signals_v2/*.py` (legacy modules reused by v3)
- `swingmaster/core/domain/enums.py`
- `swingmaster/core/domain/models.py`
- `swingmaster/core/domain/transition_graph.py`
- `swingmaster/core/policy/guardrails.py`
- `swingmaster/core/policy/rule_v1/policy.py`
- `swingmaster/core/policy/rule_v2/policy.py`
- `swingmaster/core/policy/rule_v3/policy.py`
- `swingmaster/cli/run_range_universe.py`
- `swingmaster/ew_score/model_config.py`
- `swingmaster/ew_score/repo.py`
- `swingmaster/ew_score/compute.py`
- `swingmaster/ew_score/daily_list.py`
- `swingmaster/cli/run_ew_score.py`
- `swingmaster/cli/run_daily_production_list.py`
- `swingmaster/cli/run_performance_report.py`

## 1. Signal Layer (V3)

### 1.1 Active SignalKey set
- `SLOW_DECLINE_STARTED`
- `SLOW_DRIFT_DETECTED`
- `SHARP_SELL_OFF_DETECTED`
- `STRUCTURAL_DOWNTREND_DETECTED`
- `VOLATILITY_COMPRESSION_DETECTED`
- `MA20_RECLAIMED`
- `HIGHER_LOW_CONFIRMED`
- `STRUCTURE_BREAKOUT_UP_CONFIRMED`
- `TREND_STARTED`
- `TREND_MATURED`
- `SELLING_PRESSURE_EASED`
- `STABILIZATION_CONFIRMED`
- `ENTRY_SETUP_VALID`
- `EDGE_GONE`
- `INVALIDATED`
- all `DOW_*` keys
- `DATA_INSUFFICIENT`
- `NO_SIGNAL`

### 1.2 Provider defaults (`OsakeDataSignalProviderV3`)
- `sma_window=20`
- `momentum_lookback=1`
- `matured_below_sma_days=5`
- `atr_window=14`
- `stabilization_days=5`
- `atr_pct_threshold=0.03`
- `range_pct_threshold=0.05`
- `entry_sma_window=5`
- `invalidation_lookback=10`
- `require_row_on_date=False`
- `dow_window=3`
- `dow_use_high_low=True`
- `dow_sensitive_down_reset=False`
- `debug=False`
- `debug_dow_markers=False`
- `SAFETY_MARGIN_ROWS=2`

### 1.3 Required history rows
`required_rows = max(
  sma_window + momentum_lookback,
  sma_window + 5,
  atr_window + 1,
  max(stabilization_days + 1, entry_sma_window),
  invalidation_lookback + 1,
  (2 * dow_window) + 1,
  SMA_LEN + REGIME_WINDOW - 1,
  SMA_LEN + SLOPE_LOOKBACK,
  BREAK_LOW_WINDOW + 1
) + SAFETY_MARGIN_ROWS`

If insufficient rows, emit `DATA_INSUFFICIENT`.
If `require_row_on_date=True` and latest row is not the requested date, emit `DATA_INSUFFICIENT`.

### 1.4 ATR helper (`_compute_atr`)
- Requires at least `period + 1` OHLC rows.
- `TR = max(high-low, abs(high-prev_close), abs(low-prev_close))`
- `ATR = mean(first period TR values)`

## 2. Signal Rules and Thresholds

### 2.1 `SLOW_DRIFT_DETECTED`
File: `signals_v3/slow_drift_detected.py`
- `LOOKBACK_LONG_DAYS=10`, `MA_SHORT=5`, `MA_LONG=10`, `MIN_DECLINE=-0.03`
- Conditions:
1. `len(closes) >= 11`
2. `c_t10 > c_t5 > c_t2 > c_t0`
3. `(c_t0 / c_t10) - 1 <= -0.03`
4. `ma5 < ma10` and `c_t0 < ma10`
- Provider also emits legacy key `SLOW_DECLINE_STARTED` when this is true.

### 2.2 `SHARP_SELL_OFF_DETECTED`
File: `signals_v3/sharp_sell_off_detected.py`
- `ATR_LEN=14`, `ONE_DAY_MULT=2.5`, `THREE_DAY_MULT=3.5`
- Conditions:
1. `len(closes) >= 4`
2. ATR14 available
3. `atr_pct = atr14 / c_t0`
4. Trigger if either:
- `(c_t0 / c_t1) - 1 <= -(2.5 * atr_pct)`
- `(c_t0 / c_t3) - 1 <= -(3.5 * atr_pct)`

### 2.3 `VOLATILITY_COMPRESSION_DETECTED`
File: `signals_v3/volatility_compression_detected.py`
- `ATR_LEN=14`, `ROLLING_WINDOW=20`, `OFFSET_T5=5`, `OFFSET_T10=10`, `DEFAULT_COMPRESSION_RATIO=0.75`
- Min data: `(ROLLING_WINDOW - 1) + ATR_LEN + 1`
- Compute `atr_pct[offset] = ATR14(ohlc[offset:]) / close[offset]` for `offset 0..19`
- Trigger when all hold:
- `atr_t0 < atr_t5`
- `atr_t0 < atr_t10`
- `atr_t0 <= 0.75 * max(atr_pct_values)`

### 2.4 `MA20_RECLAIMED`
File: `signals_v3/ma20_reclaimed.py`
- `window=20`
- Conditions:
1. `len(closes) >= 21`
2. all required closes are positive
3. `sma_t0 = mean(closes[0:20])`
4. `sma_t1 = mean(closes[1:21])`
5. `closes[0] > sma_t0` and `closes[1] <= sma_t1`

### 2.5 `STRUCTURAL_DOWNTREND_DETECTED`
File: `signals_v3/structural_downtrend_detected.py`
- `LOOKBACK_WINDOW=30`
- Primary trigger from Dow facts:
- `DOW_TREND_DOWN` or `DOW_NEW_LL`
- Fallback trigger:
- use last 30 closes
- detect local highs/lows (1-step pivots)
- require at least 2 highs and 2 lows
- last two highs descending and last two lows descending

### 2.6 Legacy signals still used in v3 provider
- `TREND_STARTED`
- `TREND_MATURED`
- `STABILIZATION_CONFIRMED`
- `ENTRY_SETUP_VALID`
- `INVALIDATED`
- `compute_dow_signal_facts(...)`

Provider details:
- If `INVALIDATED=True`, same-day `STABILIZATION_CONFIRMED` and `ENTRY_SETUP_VALID` are removed.
- `TREND_STARTED` is forced if `DOW_TREND_CHANGE_UP_TO_NEUTRAL` and `DOW_LAST_LOW_LL`; otherwise base `eval_trend_started(...)` is used.
- If `DOW_LAST_LOW_HL` exists: emit `HIGHER_LOW_CONFIRMED`.
- If `DOW_BOS_BREAK_UP` exists: emit `STRUCTURE_BREAKOUT_UP_CONFIRMED`.
- If no primary signals and no `INVALIDATED`: emit `NO_SIGNAL`.

## 3. State Machine

### 3.1 States
- `NO_TRADE`
- `DOWNTREND_EARLY`
- `DOWNTREND_LATE`
- `STABILIZING`
- `ENTRY_WINDOW`
- `PASS`

Typical lifecycle:
`NO_TRADE -> DOWNTREND_EARLY -> DOWNTREND_LATE -> STABILIZING -> ENTRY_WINDOW -> PASS -> NO_TRADE`

### 3.2 Allowed transitions
- `NO_TRADE` -> `NO_TRADE`, `DOWNTREND_EARLY`
- `DOWNTREND_EARLY` -> `DOWNTREND_EARLY`, `DOWNTREND_LATE`, `STABILIZING`, `NO_TRADE`
- `DOWNTREND_LATE` -> `DOWNTREND_LATE`, `STABILIZING`, `NO_TRADE`
- `STABILIZING` -> `STABILIZING`, `ENTRY_WINDOW`, `NO_TRADE`
- `ENTRY_WINDOW` -> `ENTRY_WINDOW`, `PASS`, `NO_TRADE`
- `PASS` -> `PASS`, `NO_TRADE`

### 3.3 Guardrails
`apply_guardrails` blocks transition if:
- transition not in allowed graph -> `DISALLOWED_TRANSITION`
- state age below minimum -> `MIN_STATE_AGE_LOCK`

`MIN_STATE_AGE`:
- `NO_TRADE=0`
- `DOWNTREND_EARLY=2`
- `DOWNTREND_LATE=3`
- `STABILIZING=2`
- `ENTRY_WINDOW=1`
- `PASS=1`

### 3.4 Rule contract (v1/v2 base)
Decision flow:
1. hard exclusions (`DATA_INSUFFICIENT`, `INVALIDATED`)
2. helpers (`EDGE_GONE`, `CHURN_GUARD`, `ENTRY_CONDITIONS_MET`, `RESET_TO_NEUTRAL`)
3. per-state rules
4. fallback (stay + fallback reason)

Per-state rules:
- `NO_TRADE`: `TREND_STARTED` -> `DOWNTREND_EARLY`
- `DOWNTREND_EARLY`: `TREND_MATURED` -> `DOWNTREND_LATE`; `STABILIZATION_CONFIRMED`/`SELLING_PRESSURE_EASED` -> `STABILIZING`
- `DOWNTREND_LATE`: `STABILIZATION_CONFIRMED`/`SELLING_PRESSURE_EASED` -> `STABILIZING`
- `STABILIZING`: `STABILIZATION_CONFIRMED` -> stay
- `ENTRY_WINDOW`: `ENTRY_SETUP_VALID` -> stay, else `PASS`
- `PASS`: `NO_TRADE`

V2 additions:
- inject `INVALIDATED` when previous state is `STABILIZING`/`ENTRY_WINDOW` and `DOW_NEW_LL` exists
- allow `NO_TRADE -> DOWNTREND_EARLY` via `SLOW_DECLINE_STARTED` when decision was `NO_SIGNAL` and `DOW_TREND_UP` is not present

V1/V3 consistency fix (current behavior):
- if `prev_state == STABILIZING` and today has `TREND_STARTED`, `ReasonCode.TREND_STARTED` is added to decision reasons even if state does not transition.

### 3.5 Helper semantics used by policy decisions

`ENTRY_CONDITIONS_MET` helper (`_entry_conditions_decision`, `rule_v1`):
- Applies only when `prev_state == STABILIZING`.
- Hard blockers:
  - `DATA_INSUFFICIENT`
  - `INVALIDATED`
  - `EDGE_GONE`
  - `NO_SIGNAL`
  - `TREND_STARTED`
  - `TREND_MATURED`
- Requires current day `ENTRY_SETUP_VALID`.
- Requires recent stabilization context:
  - immediate pass if same-day `STABILIZATION_CONFIRMED`, or
  - in history-aware mode, `STABILIZATION_CONFIRMED` seen within `STAB_RECENCY_DAYS=10`.
- Requires setup freshness:
  - in history-aware mode, `ENTRY_SETUP_VALID` seen within `SETUP_FRESH_DAYS=5`,
  - fallback proxy without signal history: recent `ENTRY_WINDOW` state in same lookback.
- Output: transition to `ENTRY_WINDOW` with reason `ENTRY_CONDITIONS_MET`.

`EDGE_GONE` helper (`_edge_gone_decision`, `rule_v1`):
- Applies only when `prev_state` is `ENTRY_WINDOW` or `STABILIZING`.
- Hard blockers:
  - `DATA_INSUFFICIENT`
  - `INVALIDATED`
- Constants (current):
  - `EDGE_GONE_ENTRY_WINDOW_MAX_AGE = 9`
  - `EDGE_GONE_STABILIZING_MAX_AGE = 20`
  - `EDGE_GONE_RECENT_SETUP_LOOKBACK = 10`
- Behavior:
  - in `ENTRY_WINDOW`: if consecutive days in state `>= 9`, force `PASS` with reason `EDGE_GONE`.
  - in `STABILIZING`: if consecutive days in state `>= 20`, force `NO_TRADE` with reason `EDGE_GONE`, unless recent setup activity is detected in lookback window.

## 4. ReasonCodes

V3 uses same reason codes as v2:
- `SLOW_DECLINE_STARTED`
- `TREND_STARTED`
- `TREND_MATURED`
- `SELLING_PRESSURE_EASED`
- `STABILIZATION_CONFIRMED`
- `ENTRY_CONDITIONS_MET`
- `EDGE_GONE`
- `INVALIDATED`
- `INVALIDATION_BLOCKED_BY_LOCK`
- `DISALLOWED_TRANSITION`
- `PASS_COMPLETED`
- `ENTRY_WINDOW_COMPLETED`
- `RESET_TO_NEUTRAL`
- `CHURN_GUARD`
- `MIN_STATE_AGE_LOCK`
- `DATA_INSUFFICIENT`
- `NO_SIGNAL`

Persisted as `POLICY:<ReasonCode>`.

## 5. V3 State Attr Metadata Contract

`RuleBasedTransitionPolicyV3Impl` delegates to v2, then updates attrs metadata.

### 5.1 `downtrend_origin`
On `NO_TRADE -> DOWNTREND_EARLY`:
- if `TREND_STARTED` -> `TREND`
- else if `SLOW_DECLINE_STARTED` -> `SLOW`
- else keep previous

### 5.2 `downtrend_entry_type`
One-time classification on first `NO_TRADE -> DOWNTREND_EARLY` when not already set:
- detect origin for entry type:
- `SLOW_DECLINE_STARTED` -> `SLOW`
- else `TREND_STARTED` -> `TREND`
- else `UNKNOWN`
- structural confirmation true if any:
- `STRUCTURAL_DOWNTREND_DETECTED`
- `DOW_TREND_DOWN`
- `DOW_NEW_LL`
- `DOW_BOS_BREAK_DOWN`
- mapping:
- `SLOW` + structural -> `SLOW_STRUCTURAL`
- `SLOW` + not structural -> `SLOW_SOFT`
- `TREND` + structural -> `TREND_STRUCTURAL`
- `TREND` + not structural -> `TREND_SOFT`
- otherwise `UNKNOWN`
- once set, not overwritten on later days

### 5.3 `decline_profile`
Classification:
- `SLOW_DRIFT_DETECTED` -> `SLOW_DRIFT`
- `SHARP_SELL_OFF_DETECTED` -> `SHARP_SELL_OFF`
- `STRUCTURAL_DOWNTREND_DETECTED` or `TREND_MATURED` or `DOW_TREND_DOWN` -> `STRUCTURAL_DOWNTREND`
- else `UNKNOWN`

One-way behavior:
- specific profile does not downgrade
- `UNKNOWN` can upgrade to specific within downtrend phase

### 5.4 `stabilization_phase`
- if next state is `STABILIZING`:
- `ENTRY_SETUP_VALID` and not `INVALIDATED` -> `EARLY_REVERSAL`
- `STABILIZATION_CONFIRMED` and `VOLATILITY_COMPRESSION_DETECTED` and not `INVALIDATED` -> `BASE_BUILDING`
- else `EARLY_STABILIZATION`
- if next state is `ENTRY_WINDOW` -> `EARLY_REVERSAL`
- else keep previous

Special v3 invariant fix (current code path):
- when previous state is `STABILIZING`, and signals contain both `ENTRY_SETUP_VALID` and `INVALIDATED`, and v3 resolves final state to `NO_TRADE`, `stabilization_phase` is forced to `EARLY_STABILIZATION` (not `None`) for that day.

### 5.5 `entry_gate` and `entry_quality`
Gate override from `STABILIZING`:
- Gate A (`MA20_RECLAIMED` + `HIGHER_LOW_CONFIRMED` + not `INVALIDATED`):
- `entry_gate="EARLY_STAB_MA20_HL"`, `entry_quality="A"`
- Gate B (`MA20_RECLAIMED` + not `HIGHER_LOW_CONFIRMED` + not `INVALIDATED`):
- `entry_gate="EARLY_STAB_MA20"`, `entry_quality="B"`
- Legacy entry window:
- `entry_gate="LEGACY_ENTRY_SETUP_VALID"`, `entry_quality="LEGACY"`

### 5.6 `status` JSON merge
`StateAttrs.status` JSON keys:
- `downtrend_origin`
- `downtrend_entry_type`
- `decline_profile`
- `stabilization_phase`
- `entry_gate`
- `entry_quality`
- `entry_continuation_confirmed`

Deterministic merge:
- string value present -> write key
- value absent -> remove key
- empty payload -> `status=None`

### 5.7 Entry continuation confirmation metadata
This metadata is populated during range runs in `run_range_universe.py` from market data (not from delegated policy decision):
- Rule: first 5 trading days from `ENTRY_WINDOW` start (`fwd_idx=1..5`, EW day inclusive)
- Compute rolling `SMA5(close)` on trading-day series
- `above_5 = count(close > SMA5)` across those 5 days where SMA5 is non-null
- `entry_continuation_confirmed = (above_5 >= 3)`

Write timing and storage:
- The value becomes decidable on `fwd_idx=5` (decision day).
- Canonical write target: `rc_state_daily.state_attrs_json` for `(ticker, decision_date)`:
  - `$.status.entry_continuation_confirmed = true/false`
- `rc_transition.state_attrs_json` is also enriched for `to_state='ENTRY_WINDOW'` rows with:
  - `entry_continuation_rule`
  - `entry_continuation_above_5`
  - `entry_continuation_confirmed`
  (kept for transition-audit compatibility)

## 6. Runtime Notes (Range CLI + V3 guard)
- `run_range_universe.py` defaults are now `--policy-version v3` and `--signal-version v3`.
- Version compatibility guard executes immediately after arg parsing:
  - allowed: both `signal_version` and `policy_version` are `v3`
  - allowed: both are non-`v3`
  - rejected: mixed pair -> `RuntimeError("Incompatible versions: signal-version and policy-version must both be v3, or both non-v3.")`
- Guard runs before opening SQLite connections and before any schema ensure calls.
- SQLite temp directory is forced to `/tmp` in CLI startup (`SQLITE_TMPDIR` env + `PRAGMA temp_store_directory='/tmp'`) to keep range runs stable in this environment.
- Optional EW scoring integration from range CLI:
  - flags: `--ew-score`, `--ew-score-rule`, `--osakedata-db`
  - execution point: after state data exists for processed dates
  - summary line: dates processed + total rows written.

## 7. EW Scoring Layer (current production contract)

### 7.1 Table and schema migration behavior
Table: `rc_ew_score_daily`

Legacy columns (kept untouched):
- `ew_score_day3`, `ew_level_day3`, `ew_rule`, `inputs_json`

Fastpass mode columns:
- `ew_score_fastpass`, `ew_level_fastpass`
- `ew_rule_fastpass`, `inputs_json_fastpass`

Rolling mode columns:
- `ew_score_rolling`, `ew_level_rolling`
- `ew_rule_rolling`, `inputs_json_rolling`

Migration helper:
- `ensure_rc_ew_score_daily_dual_mode_columns(conn)` checks `PRAGMA table_info(rc_ew_score_daily)` and adds missing dual-mode columns with `ALTER TABLE`.
- Idempotent on repeated runs.
- Raises clear error if table `rc_ew_score_daily` does not exist.

### 7.2 Router by market (explicit maps)
In `ew_score/compute.py`:
- `ROLLING_ENABLED_BY_MARKET = {"omxh": True, "omxs": True, "usa": False}`
- `FASTPASS_ENABLED_BY_MARKET = {"omxh": True, "omxs": True, "usa": True}`
- Rolling rules:
  - `omxh -> EW_SCORE_ROLLING_V2_FIN`
  - `omxs -> EW_SCORE_ROLLING_V2_SE`
- Fastpass rules:
  - `omxh -> EW_SCORE_FASTPASS_V1_FIN`
  - `omxs -> EW_SCORE_FASTPASS_V1_SE`
  - `usa  -> EW_SCORE_FASTPASS_V1_USA_SMALL`

### 7.3 Level contract (uniform 0/1/2/3)
Both rolling and fastpass use same level mapping from `rows_total` and score threshold:
- if `rows_total < 4`:
  - `level=1` when `score >= threshold`
  - else `level=0`
- if `rows_total >= 4`:
  - `level=3` when `score >= threshold`
  - else `level=2`

### 7.4 UPSERT isolation (dual-mode safety)
Conflict target remains `ON CONFLICT(ticker, date)`.

Legacy path:
- `upsert_row(...)` is preserved unchanged and updates only legacy columns:
  - `ew_score_day3`, `ew_level_day3`, `ew_rule`, `inputs_json`

Mode-specific writes:
- `upsert_fastpass_row(...)` updates only:
  - `ew_score_fastpass`, `ew_level_fastpass`, `ew_rule_fastpass`, `inputs_json_fastpass`
- `upsert_rolling_row(...)` updates only:
  - `ew_score_rolling`, `ew_level_rolling`, `ew_rule_rolling`, `inputs_json_rolling`

No cross-mode overwrites:
- fastpass writes do not modify rolling columns
- rolling writes do not modify fastpass columns
- legacy columns remain unchanged by these two upserts.
- `created_at` is not changed in conflict updates for dual-mode upserts.

### 7.5 Active thresholds and market lock state
Fastpass thresholds:
- `EW_SCORE_FASTPASS_V1_USA_SMALL`: threshold `0.60`
- `EW_SCORE_FASTPASS_V1_FIN`: threshold `0.60`
- `EW_SCORE_FASTPASS_V1_SE`: threshold `0.65`

Rolling thresholds:
- `EW_SCORE_ROLLING_V2_FIN`: threshold `0.45`
- `EW_SCORE_ROLLING_V2_SE`: threshold `0.47`
- `USA rolling`: `OFF` (locked)

### 7.6 Rule immutability (LOCKED)
- Locked rule IDs are immutable: coefficients and thresholds must not change under the same `rule_id`.
- Parameter changes require a new rule id (for example `..._V2_...`).
- Level contract `0/1/2/3` is locked and shared by fastpass + rolling.

### 7.7 Required audit JSON keys
Fastpass (`inputs_json_fastpass`) required keys:
- `rule_id`, `beta0`, `threshold`
- `entry_date`, `last_stab_date`
- `close_entry`, `close_last_stab`, `r_stab_to_entry_pct`
- all categorical values used by the active market model
- `rows_total`
- `score_raw_z`

Rolling (`inputs_json_rolling`) required keys:
- `rule_id`, `beta0`, `beta1`, `threshold`
- `entry_date`, `as_of_date`
- `close_day0`, `close_today`
- `r_prefix_pct`, `rows_total`
- `score_raw_z`

## 8. Operational workflow (production)
1. Run range pipeline to produce states/transitions/episodes.
2. Run EW scoring (range or daily) to fill `rc_ew_score_daily` dual-mode columns.
3. Run reporting to aggregate episode-level results by:
   - max rolling level
   - max fastpass level
   - rule IDs used per mode.

## 9. Regression protections (LOCKED)
- CLI compatibility guard:
  - mismatched `signal-version` vs `policy-version` must raise `RuntimeError("Incompatible versions...")` before DB/schema operations.
- USA rolling lock invariant:
  - for `market=usa`, rolling path stays disabled and rolling upsert is not called.
- Policy invariants preserved during freeze:
  - v1: `TREND_STARTED` reason is retained in `STABILIZING` even without transition.
  - v3: `stabilization_phase="EARLY_STABILIZATION"` is forced in the specific invalidated legacy setup branch.

## 10. Reporting updates tied to EW modes
- `run_performance_report.py` now reports EW rules from all rule columns:
  - `legacy=... | rolling=... | fastpass=...`
- EW max-level extraction prefers `ew_level_rolling` when present, falls back to `ew_level_day3`.
- Added episode-level fastpass sections:
  - `EARLY_PHASE_BY_MAX_FASTPASS_LEVEL`
  - `MATURE_PHASE_BY_MAX_FASTPASS_LEVEL`
- Report supports `--format csv` with:
  - `;` as delimiter
  - decimal comma formatting via report formatter.

## 11. Audit Notes
- V3 may override delegated `next_state` (`STABILIZING -> ENTRY_WINDOW` via Gate A/B).
- Reason codes are inherited from delegated decision; v3 gate override does not introduce a new reason code.
- For analysis, use together:
- `state` / `prev_state`
- `reason_codes`
- `state_attrs_json` fields (`downtrend_origin`, `downtrend_entry_type`, `decline_profile`, `stabilization_phase`, `entry_gate`, `entry_quality`, `entry_continuation_confirmed`)

---

This document is the single V3 reference. Update it whenever V3 signal modules, policy modules, or metadata contracts change.
