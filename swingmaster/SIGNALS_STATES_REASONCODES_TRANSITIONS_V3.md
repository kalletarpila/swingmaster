# Swingmaster V3: Signals, Thresholds, States, ReasonCodes, and Transition Contract

This is the unified V3 reference document for:
1. signal generation rules and thresholds,
2. state machine and transition rules,
3. reason code semantics,
4. V3 metadata contract in `state_attrs_json`,
5. EW scoring/runtime integration that now runs alongside V3 pipeline outputs.

## Document update provenance
- Previous update baseline: commit `4808bc32f7dd96df9be1f16b604d52df52acb01f` (2026-02-19 18:46:56 +0200)
- Last content update commit for this file: `2f2bb8fabece30ac59859190c516403c03390ee9` (2026-03-05 20:01:57 +0200)
- Filesystem modified timestamp for this file: `2026-03-05 19:58:28 +0200`
- Current repository HEAD at time of this sync: `7634345329503e6006fe20465d38cca9623b2055` (2026-03-08)
- This revision extends the V3 doc with the current BUY-rule contract reality and a precise implementation delta for USA probabilistic PASS rules.

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
- `swingmaster/cli/daily_report.py`
- `swingmaster/cli/run_daily_report.py`
- `swingmaster/cli/run_weekly_report.py`
- `swingmaster/cli/run_transactions_simu_fast.py`
- `swingmaster/cli/run_transactions_simu_sell_fast.py`
- `swingmaster/reporting/sell_rules_engine.py`
- `daily_reports/buy_rules/*.json`
- `daily_reports/buy_rules/schema_v1.md`
- `daily_reports/sell_rules/*.json`
- `swingmaster/ew_score/model_config.py`
- `swingmaster/ew_score/repo.py`
- `swingmaster/ew_score/compute.py`
- `swingmaster/ew_score/daily_list.py`
- `swingmaster/cli/run_ew_score.py`
- `swingmaster/cli/run_daily_production_list.py`
- `swingmaster/cli/run_performance_report.py`

## 1. Signal Layer (V3)

### 1.1 Active SignalKey set
SignalKey enum superset (current identifiers in code):
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

Current `OsakeDataSignalProviderV3` emitted set:
- `SLOW_DRIFT_DETECTED`
- `SLOW_DECLINE_STARTED`
- `SHARP_SELL_OFF_DETECTED`
- `VOLATILITY_COMPRESSION_DETECTED`
- `MA20_RECLAIMED`
- `TREND_MATURED`
- `STABILIZATION_CONFIRMED`
- `ENTRY_SETUP_VALID`
- `INVALIDATED`
- `STRUCTURAL_DOWNTREND_DETECTED`
- `TREND_STARTED`
- all computed `DOW_*` facts
- `HIGHER_LOW_CONFIRMED` (derived from `DOW_LAST_LOW_HL`)
- `STRUCTURE_BREAKOUT_UP_CONFIRMED` (derived from `DOW_BOS_BREAK_UP`)
- `DATA_INSUFFICIENT`
- `NO_SIGNAL`

Enum keys currently consumed by policy but not emitted by the current V3 provider:
- `SELLING_PRESSURE_EASED`
- `EDGE_GONE`

Important interpretation:
- `DOW_*` keys are structural facts, not a mutually-exclusive single label.
- A single day may emit multiple `DOW_*` facts at the same time, for example:
  - trend state (`DOW_TREND_*`)
  - last confirmed low classification (`DOW_LAST_LOW_*`)
  - last confirmed high classification (`DOW_LAST_HIGH_*`)
  - new-break event (`DOW_NEW_LL`, `DOW_NEW_HH`, `DOW_BOS_BREAK_*`)
- This is expected current behavior, not a provider conflict.

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
- All computed Dow facts are written through to the final signal set after primary/non-primary signals are resolved.
- If no primary signals and no `INVALIDATED`: emit `NO_SIGNAL`.
- `SELLING_PRESSURE_EASED` remains part of the policy contract, but no dedicated V3 provider module emits it in the current codebase.
- `EDGE_GONE` is a policy/helper concept in current runtime, not a primary V3 provider output.

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
- Current range-run behavior forces `effective_require_row_on_date = True` internally.
- Universe filtering may apply `require_row_on_date` against `date_from`, and daily processing also filters tickers to those that actually have an as-of row on each processed trading day.
- Practical effect:
  - daily `run_daily(...)` is executed only for tickers present on that trading day
  - tickers missing an as-of row are skipped for that day instead of being processed against stale last row data
- Optional EW scoring integration from range CLI:
  - flags: `--ew-score`, `--ew-score-rule`, `--osakedata-db`
  - execution point: after state data exists for processed dates and after episode post phases have populated `rc_pipeline_episode`
  - summary line: dates processed + total rows written.
- Post-run episode enrichment phases execute after the day loop, in this order:
  - `populate_rc_pipeline_episode`
  - `populate_rc_pipeline_episode_sma_extremes`
  - `populate_rc_pipeline_episode_peak_timing_fields`
  - `populate_rc_pipeline_episode_entry_confirmation`
- Current CLI prints explicit progress markers for these phases:
  - `POST_PHASE_START phase=...`
  - `POST_PHASE_END phase=... ms=...`
  - `POST_PHASE_SUMMARY ...`
- Operational implication:
  - `rc_state_daily` / `rc_transition` daily data is committed inside each `run_daily(...)`
  - if a long range run is interrupted during post phases, the main day-level state machine output is already persisted; risk is limited to incomplete episode/post-processing fields until the range post phases are re-run.

### 6.1 Daily report base sections
Current `daily_report.py` builds report output in this order:
- `NEW_EW`
- `NEW_PASS`
- `BUYS`
- `EW_SNAPSHOT`
- `ALERTS`

Base rows come from the SQL template `daily_reports/fin_daily_report.sql`, which is parameterized by market config.

Operational meaning of the buy-rule triggers:
- `NEW_EW`: ticker transitioned to `ENTRY_WINDOW` on `as_of_date`
- `NEW_PASS`: ticker transitioned to `PASS` on `as_of_date`
- `EW_SNAPSHOT`: ticker is in `ENTRY_WINDOW` on `as_of_date`

`buy_badges` is not part of the base `report_raw` SQL temp table.
Current reporting path enriches it afterwards from `rc_transactions_simu`:
- only `BUYS` rows are eligible
- match key is `(ticker, buy_date)`
- when multiple persisted BUY rows exist, reporting prefers latest by `created_at DESC, id DESC`
- empty `[]` stays hidden in rendered output

Current market routing in `daily_report.py`:
- `FIN` / `OMXH` -> db `swingmaster_rc.db`
- `SE` / `OMXS` -> db `swingmaster_rc_se.db`
- `USA` -> db `swingmaster_rc_usa_2024_2025.db`
- `USA500` -> db `swingmaster_rc_usa_500.db`

Current hardcoded report identities:
- `FIN`: section `BUYS_FIN`, rule label `FIN_PASS_FP60`, threshold `0.60`
- `SE`: section `BUYS_1_SE`, rule label `SE_BUY_1_FP80`, threshold `0.80`
- `USA`: section `BUYS_USA`, rule label `USA_PASS_FP80`, threshold `0.80`
- `USA500`: section `BUYS_USA500`, rule label `USA500_PASS_FP80`, threshold `0.80`

Important distinction:
- these hardcoded section/rule placeholders belong to the parameterized SQL template path
- current authoritative BUY recommendation rows are then rebuilt in Python from market JSON buy rules
- therefore a hardcoded SQL label may differ from final JSON-driven `rule_hit` values
- `USA500` currently reuses `rules_market="USA"` for JSON buy-rule loading, while keeping its own hardcoded section label for the SQL template path

### 6.2 Buy-rule JSON contract and current live rules
Buy rules are metadata/reporting layer only:
- they do not alter signal generation
- they do not alter policy logic
- they do not alter state transitions
- they filter already-produced report candidate rows

Validation contract (`version = 1`):
- top-level keys exactly: `market`, `version`, `rules`
- allowed triggers:
  - `NEW_EW`
  - `NEW_PASS`
  - `NEW_NOTRADE`
  - `EW_SNAPSHOT`
- allowed condition keys:
  - `fastpass_score_gte`
  - `fastpass_level_eq`
  - `rolling_end_level_eq`
  - `dual_buy_badge_eq`
  - `days_in_current_episode_gte`
  - `days_in_current_episode_lte`
  - `days_in_stabilizing_before_ew_gte`
  - `days_in_stabilizing_before_ew_eq`
- conditions are ANDed
- no OR logic
- no nested logic

Field mapping used by the engine:
- `fastpass_score_gte` -> `ew_score_fastpass`
- `fastpass_level_eq` -> `ew_level_fastpass`
- `rolling_end_level_eq` -> `ew_level_rolling`
- `dual_buy_badge_eq` -> `dual_buy_badge`
- `days_in_current_episode_*` -> `days_in_current_episode`
- `days_in_stabilizing_before_ew_*` -> `days_in_stabilizing_before_ew`

Current committed market configs:
- `FIN`
  - `FIN_PASS_FP60`
  - trigger `NEW_PASS`
  - condition `fastpass_score_gte = 0.60`
- `SE`
  - `SE_PASS_FP80`
  - trigger `NEW_PASS`
  - condition `fastpass_score_gte = 0.80`
  - `SE_ENTRY_FP80`
  - trigger `NEW_EW`
  - condition `fastpass_score_gte = 0.80`
  - `SE_PASS_ROLL_END_LVL1`
  - trigger `NEW_PASS`
  - condition `rolling_end_level_eq = 1`
- `USA`
  - `USA_PASS_FP80`
  - trigger `NEW_PASS`
  - condition `fastpass_score_gte = 0.80`
  - `USA_NOTRADE_FP80`
  - trigger `NEW_NOTRADE`
  - condition `fastpass_score_gte = 0.80`
  - `USA_PASS_DUAL_PREMIUM_U80F20`
  - trigger `NEW_PASS`
  - condition `dual_buy_badge_eq = BUY_PREMIUM`
  - `USA_PASS_DUAL_ELITE_U72F27`
  - trigger `NEW_PASS`
  - condition `dual_buy_badge_eq = BUY_ELITE`
  - `USA_PASS_DUAL_STRONG_U66F32`
  - trigger `NEW_PASS`
  - condition `dual_buy_badge_eq = BUY_STRONG`
  - `USA_PASS_DUAL_QUALIFIED_U60F35`
  - trigger `NEW_PASS`
  - condition `dual_buy_badge_eq = BUY_QUALIFIED`
  - `USA_NOTRADE_DUAL_PREMIUM_U80F20`
  - trigger `NEW_NOTRADE`
  - condition `dual_buy_badge_eq = BUY_PREMIUM`
  - `USA_NOTRADE_DUAL_ELITE_U72F27`
  - trigger `NEW_NOTRADE`
  - condition `dual_buy_badge_eq = BUY_ELITE`
  - `USA_NOTRADE_DUAL_STRONG_U66F32`
  - trigger `NEW_NOTRADE`
  - condition `dual_buy_badge_eq = BUY_STRONG`
  - `USA_NOTRADE_DUAL_QUALIFIED_U60F35`
  - trigger `NEW_NOTRADE`
  - condition `dual_buy_badge_eq = BUY_QUALIFIED`
  - `BUY_BULL_FAIL10_UP20_V1`
  - trigger `NEW_PASS`
  - intended conditions:
    - `regime_eq = BULL`
    - `fail10_prob_gte = 0.10`
    - `fail10_prob_lte = 0.30`
    - `up20_prob_gte = 0.30`
  - `BUY_BULL_PASS_FAIL10_UP20_V2`
  - trigger `NEW_PASS`
  - intended conditions:
    - `regime_eq = BULL`
    - `entry_window_exit_state_eq = PASS`
    - `fail10_prob_gte = 0.10`
    - `fail10_prob_lte = 0.35`
    - `up20_prob_gte = 0.25`

Current buy-rule files:
- `daily_reports/buy_rules/fin.json`
- `daily_reports/buy_rules/se.json`
- `daily_reports/buy_rules/usa.json`

Rule application flow:
- load JSON for market
- filter base rows by trigger
- evaluate all conditions against already-populated fields
- create `BUYS` row for each match
- group by `(ticker, event_date)`
- join multiple rule hits with `;`

### 6.2.1 Current mismatch and exact implementation delta (USA probabilistic PASS rules)
Current runtime mismatch (as-of HEAD `7634345`):
- `daily_reports/buy_rules/usa.json` contains new probabilistic keys:
  - `regime_eq`
  - `entry_window_exit_state_eq`
  - `fail10_prob_gte`
  - `fail10_prob_lte`
  - `up20_prob_gte`
- `swingmaster/cli/daily_report.py` validator and field mapping do not support these keys yet.
- `report_raw` payload does not expose corresponding fields (`regime`, `entry_window_exit_state`, `fail10_prob`, `up20_prob`) for rule evaluation.
- Result: JSON validation fails on unknown keys before BUY rows are built.

Required implementation changes (normative):
1. Extend allowed buy-rule condition keys in `swingmaster/cli/daily_report.py`:
   - add `regime_eq`
   - add `entry_window_exit_state_eq`
   - add `fail10_prob_gte`
   - add `fail10_prob_lte`
   - add `up20_prob_gte`
2. Extend `CONDITION_FIELD_MAP` in `daily_report.py`:
   - `regime_eq` -> `regime`
   - `entry_window_exit_state_eq` -> `entry_window_exit_state`
   - `fail10_prob_gte` -> `fail10_prob`
   - `fail10_prob_lte` -> `fail10_prob`
   - `up20_prob_gte` -> `up20_prob`
3. Ensure `report_raw` contains these columns for BUY-rule evaluation:
   - `regime`
   - `entry_window_exit_state`
   - `fail10_prob`
   - `up20_prob`
4. SQL template update (`daily_reports/fin_daily_report.sql`):
   - add the four columns above to every `report_raw` row shape used by `NEW_PASS` / `NEW_NOTRADE` / `EW_SNAPSHOT` / `NEW_EW`.
   - resolve values via deterministic joins:
     - `entry_window_exit_state` from `rc_pipeline_episode.entry_window_exit_state`
     - `regime` from `rc_episode_regime.ew_exit_regime_combined`
     - `fail10_prob` from `rc_episode_model_score` where `model_id='FAIL10_BULL_HGB_V1'`
     - `up20_prob` from `rc_episode_model_score` where `model_id='UP20_BULL_HGB_V1'`
   - join key for episode/model rows must be `episode_id`; if base row is ticker/date keyed, map to episode via `(ticker, entry_window_exit_date=event_date)` first, then join by `episode_id`.
5. Python row contract update:
   - include new columns in `OUTPUT_COLUMNS` and `REPORT_RAW_COLUMNS` in `daily_report.py`
   - include placeholders for the new fields in empty BUY-section fallback row (`build_report_rows_json_mode`)
6. Validation/docs sync:
   - update `daily_reports/buy_rules/schema_v1.md`
   - update `daily_reports/buy_rules/README.md`
   - keep this V3 document synchronized with the same allowed keys and mappings.
7. Test coverage (must pass before rollout):
   - add/extend tests in `swingmaster/tests/test_daily_report_buy_rules.py`:
     - validator accepts new keys
     - rules evaluate correctly with numeric `_gte/_lte` and string `_eq`
     - grouped BUY output contains new rule hits
     - missing field behavior remains fail-closed (no match, no crash)

Runtime invariants for implementation:
- Buy rules remain report-layer filters only; no impact on state machine/policy.
- Condition semantics remain AND-only; no OR/nesting introduced.
- Unknown condition keys must continue to raise validation errors.

### 6.3 BUY transaction simulation contract (`rc_transactions_simu`)
Canonical persisted BUY simulator in current codebase:
- `run_transactions_simu_fast.py`

Purpose:
- convert `NEW_EW` and `NEW_PASS` candidate rows into persisted simulated BUY rows
- apply deterministic buy rules before persistence

Current `rc_transactions_simu` schema used in runtime:
- `id`
- `ticker`
- `market`
- `buy_date`
- `buy_price`
- `buy_qty`
- `buy_rule_hit`
- `sell_date`
- `sell_price`
- `sell_qty`
- `sell_reason`
- `holding_trading_days`
- `run_id`
- `created_at`
- `buy_badges`

Constraints:
- unique key: `(ticker, buy_date, run_id)`
- `buy_badges TEXT NOT NULL DEFAULT '[]'`

Fast BUY simulation flow:
1. fetch candidate rows from `rc_transition` / `rc_pipeline_episode`:
   - `NEW_EW` buys use `close_at_ew_start` as `buy_price`
   - `NEW_PASS` buys use `close_at_ew_exit` as `buy_price`
2. enrich missing `days_in_current_episode` / `days_in_stabilizing_before_ew` from trading-day map if needed
3. apply buy rules
4. group multi-rule hits per `(ticker, event_date)`
5. build transaction tuples with `buy_qty = 1`
6. `INSERT OR IGNORE` into `rc_transactions_simu`

Current `run_transactions_simu_fast.py` CLI/runtime contract:
- required:
  - `--rc-db`
  - `--market` (`FIN|SE|USA`)
  - `--start-date`
  - `--end-date`
- optional:
  - `--osakedata-db`
  - `--analysis-db`
  - `--mode append|replace-run`
  - `--dry-run`
  - `--run-id`
  - `--created-at`

Current fast BUY engine summary fields include:
- `new_ew_candidates`
- `new_pass_candidates`
- `candidate_rows_total`
- `buy_rows_total_raw`
- `buy_rows_total`
- `multi_rule_buys`
- `missing_field_count`
- `buys_inserted`
- `buys_ignored`

### 6.4 BUY badges contract
`buy_badges` is persisted as deterministic JSON array text in `rc_transactions_simu`.

Storage contract:
- DB value is always JSON array text
- empty array is stored as `[]`
- display layer may simplify values, but storage remains canonical

Current badge resolution order in `run_transactions_simu_fast.py`:
1. `downtrend_entry_type=<VALUE>` from `rc_state_daily.state_attrs_json` on `buy_date`
2. `LOW_VOLUME`
3. `PENNY_STOCK`
4. `BULL_DIV_IN_LAST_20_DAYS`

Current metadata-derived badge keys:
- only `downtrend_entry_type`

Current market thresholds for `LOW_VOLUME`:
- `FIN = 35000`
- `SE = 1000000`
- `USA = 1500000`

Definition:
- use last 20 trading days including `buy_date`
- compute average of `close * volume`
- require full 20 trading-day window
- add badge if average is strictly below market threshold

Current market thresholds for `PENNY_STOCK`:
- `FIN = 1.0`
- `SE = 10.0`
- `USA = 1.0`

Definition:
- use last 20 trading days including `buy_date`
- compute `SMA20(close)`
- require full 20 trading-day window
- add badge if average close is strictly below market threshold

Current definition for `BULL_DIV_IN_LAST_20_DAYS`:
- use same 20-trading-day window as above
- query `analysis_findings`
- case-insensitive pattern match against:
  - `Bullish Divergence`
  - `BullDiv & Hammer`
  - `BullDiv & Piercing Pattern`
  - `BullDiv & Bullish Engulfing`
  - `BullDiv & Dragonfly Doji`
- add badge if any matching pattern exists on any date inside the 20-trading-day window

Report presentation contract:
- daily/weekly output adds column `buy_badges`
- display strips metadata prefix `downtrend_entry_type=` from rendered badge values
- example:
  - stored: `["downtrend_entry_type=SLOW_SOFT","LOW_VOLUME"]`
  - rendered: `["SLOW_SOFT","LOW_VOLUME"]`

### 6.5 SELL-rule JSON contract and SELL fast simulation
SELL rules are also metadata/reporting layer only:
- they do not alter signals
- they do not alter policy
- they do not alter state transitions
- they operate on simulated open positions

Current engine files:
- `swingmaster/reporting/sell_rules_engine.py`
- `swingmaster/cli/run_transactions_simu_sell_fast.py`

Validation contract (`version = 1`):
- top-level keys exactly: `market`, `version`, `rules`
- allowed trigger:
  - `OPEN_POSITION`
- allowed condition keys:
  - `holding_trading_days_gte`
  - `holding_trading_days_eq`
  - `last_close_return_gte`
  - `last_close_return_lte`
  - `close_to_buy_return_gte`
  - `close_to_buy_return_lte`

Value resolution rules:
- `holding_trading_days_*` reads `holding_trading_days`
- `last_close_return_*` prefers explicit `last_close_return`
- if not present, engine derives return from:
  - `last_close` or `close`
  - and `buy_price`
- `close_to_buy_return_*` uses same derived return logic

Current committed sell configs for all three markets:
- `FIN_TIME_40TD` / `SE_TIME_40TD` / `USA_TIME_40TD`
  - `holding_trading_days >= 40`
- `FIN_STOP_LOSS_5P` / `SE_STOP_LOSS_5P` / `USA_STOP_LOSS_5P`
  - `close_to_buy_return <= -0.05`
- `FIN_TAKE_PROFIT_10P` / `SE_TAKE_PROFIT_10P` / `USA_TAKE_PROFIT_10P`
  - `close_to_buy_return >= 0.10`

Current SELL fast simulation flow:
1. read open positions from `rc_transactions_simu` where `sell_date IS NULL`
2. load market closes from `osakedata`
3. evaluate each open position over trading dates from `max(start_date, buy_date)` through `end_date`
4. first matching sell rule wins for that position
5. update existing `rc_transactions_simu` row in place:
   - `sell_date`
   - `sell_price`
   - `sell_qty`
   - `sell_reason`
   - `holding_trading_days`

Important runtime details:
- `run_transactions_simu_sell_fast.py` default `--dry-run = true`
- `replace-run` reverts prior sells from same run by matching `sell_reason` prefix
- sell reason format:
  - `SIMU_SELL_FAST_V1_RUNID=<run_id>|<rule_hit>`
- safety guard:
  - `MAX_EVAL_ROWS = 2_000_000`

### 6.6 Weekly report aggregation
`run_weekly_report.py`:
- reads recent 7 trading dates from each market DB
- rebuilds daily buy rows through the same `build_daily_report_rows(...)` path
- keeps only `BUYS` rows
- combines `FIN`, `SE`, `USA`
- writes deterministic combined weekly text/csv output
- preserves `buy_badges` column and current display formatting

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

### 7.2 Rule resolver + router by market
`run_ew_score.py` resolves `--rule` through `resolve_ew_score_rule(...)` before scoring.

Accepted input formats:
- canonical versioned:
  - `EW_SCORE_ROLLING_FIN_V2`
  - `EW_SCORE_ROLLING_SE_V2`
- alias form:
  - `EW_SCORE_ROLLING_FIN`
  - `EW_SCORE_ROLLING_SE`
  - resolves to latest available canonical version for that market
- legacy form:
  - `EW_SCORE_ROLLING_V2_FIN`
  - if exact file exists, use it
  - otherwise resolve by market to latest canonical version

After resolution, `compute.py` still applies explicit market routing when market-specific fastpass/rolling routes exist.

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

Current routing semantics:
- if market routing succeeds, routed rolling/fastpass writes are used and generic fallback scoring is skipped for that ticker/day
- fallback to the explicit `rule_id` happens only when routing does not handle the ticker/day

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

## 12. USA Episode Dual-Score Build (Reproducible: `UP20` + `FAIL10`)
This section documents the exact research pipeline used to produce two episode-level scores for USA episodes:
- positive score: probability-style ranking for `+20%` within next 60 trading days,
- negative risk score: probability-style ranking for `FAIL10` (does not reach `+10%` within next 60 trading days).

### 12.1 Runtime environment and DBs
- working DB: `/tmp/swingmaster_usa_episode_rank_test.db`
- source RC DB (state/transition attrs): `/home/kalle/projects/swingmaster/swingmaster_rc_usa_2024_2025.db`
- source OHLCV DB (already integrated into label tables in working DB): `/home/kalle/projects/rawcandle/data/osakedata.db`

### 12.2 Label definitions and horizon
All labels are anchored at episode decision timestamp `entry_window_exit_date` and use close-only forward window.

Definitions:
- `growth_60d_close_pct_from_exit = 100 * (max_close_next_60d - close_at_ew_exit) / close_at_ew_exit`
- `UP20` label:
  - `label_up20_60d_close = 1` if `growth_60d_close_pct_from_exit >= 20`
  - else `0`
- `FAIL10` label:
  - `label_fail10_60d_close = 1` if `growth_60d_close_pct_from_exit < 10`
  - else `0`
- only rows with full forward horizon were used (`has_full_forward_60d = 1`)

### 12.3 Temporal split (no random split)
Primary historical split used in model selection research:
- `train`: years `2020-2023`
- `validation`: year `2024`
- `test`: year `2025`

Rationale: avoid leakage from random sampling across regimes; preserve chronological evaluation.

Production frozen training window (active dual model):
- `train`: years `2020-2021` (single frozen fit)
- operational evaluation window used before activation: `2022-2025`
- no rolling retrain in daily production flow; model is replaced only by explicit version change

### 12.4 Feature sets
Training script: `swingmaster/research/train_episode_up20_baseline.py`

Feature families:
- `baseline`: EW/exit/badge features (`ew_score_fastpass`, `ew_level_fastpass`, `exit_state_*`, buy flags, badges)
- `episode`: baseline + episode geometry (`close_at_*`, day counts, ranges, phase returns)
- `full`: episode + stock/index context
- `full_no_dow`: `full` without DOW index blocks (`dji_*`, `djt_*`)

Final selected feature set for risk score:
- `full_no_dow`

### 12.5 Algorithms compared
Using the same script, three algorithms were compared:
- `logreg`:
  - `SimpleImputer(constant=0) + StandardScaler + LogisticRegression(max_iter=2000, solver=lbfgs)`
- `hgb`:
  - `HistGradientBoostingClassifier(learning_rate=0.05, max_depth=3, max_iter=300, min_samples_leaf=20, random_state=42)`
- `catboost`:
  - `CatBoostClassifier(iterations=500, depth=5, learning_rate=0.05, loss_function=Logloss, eval_metric=PRAUC, random_seed=42, verbose=False)`

### 12.6 Positive score (`UP20`) final construction
Base score tables (already produced):
- `rc_episode_model_full_up20_60d_close_scores_catboost` (`score_full_catboost`)
- `rc_episode_model_full_no_dow_up20_60d_close_pass_only_scores_catboost` (`score_pass_only_catboost`)

Meta rule (`meta_v1`):
- if episode exited `ENTRY_WINDOW -> PASS`, use `score_pass_only_catboost`
- otherwise use `score_full_catboost`

Stored evaluation table:
- `rc_episode_model_test_meta_rank_v1`
  - includes `score_full_catboost`, `score_pass_only_catboost`, `score_meta_v1`

### 12.7 Negative score (`FAIL10`) final construction
Label table:
- `rc_episode_label_fail10_60d_close`

Modeling table:
- `rc_episode_model_full_no_dow_fail10_60d_close`
  - uses `full_no_dow` feature columns
  - label is mapped to script-expected column name `label_up20_60d_close` for training compatibility

Compared model outputs:
- `rc_episode_model_full_no_dow_fail10_60d_close_scores_logreg`
- `rc_episode_model_full_no_dow_fail10_60d_close_scores_hgb`
- `rc_episode_model_full_no_dow_fail10_60d_close_scores_catboost`

Selection result on 2025 test:
- selected model: `HGB`
- reason: best combined quality (`AP` and `Brier`) for risk-probability behavior

Named score table used downstream:
- `rc_episode_model_full_no_dow_fail10_60d_close_scores_hgb_named`
  - `score_fail10_60d_close_hgb`

### 12.8 Final dual-score table and locked thresholds
Historical dual score table (2025 test):
- `rc_episode_model_test_dual_score_2025_hgb_fail10`
  - `score_up20_meta_v1`
  - `score_fail10_60d_close_hgb`

Locked shortlist thresholds:
- `score_up20_meta_v1 >= 0.60`
- `score_fail10_60d_close_hgb <= 0.35`

Materialized shortlist table (2025 test):
- `rc_episode_model_test_buys_u060_f035`

Materialized yearly shortlist table (2020-2024):
- `rc_episode_model_dual_2020_2024_hgb_fail10`
- `rc_episode_model_buys_u060_f035_2020_2024`

Production dual inference table (current):
- `rc_episode_model_dual_inference_current`
  - `score_up20_meta_v1`
  - `score_fail10_60d_close_hgb`
  - `model_version` default for production CLI:
    - `DUAL_META_V1_HGB_FAIL10_FROZEN_TRAIN_2020_2021`
- frozen thresholds used by EW dual level and buy-badge logic:
  - `score_up20_meta_v1 >= 0.60`
  - `score_fail10_60d_close_hgb <= 0.35`

Production pipeline (internal-only, no external source DB):
1. `run_episode_dual_score_production.py` computes frozen-model scores directly from `rc_pipeline_episode`
   - enriches features with:
     - `rc_ew_score_daily` fastpass fields
     - `rc_transactions_simu` buy/badge flags
     - osakedata-based stock/index context (`full_no_dow`: stock + `^GSPC` + `^NDX`)
   - writes source tables:
     - `rc_episode_model_inference_rank_meta_v1`
     - `rc_episode_model_full_inference_no_dow_scores_hgb_fail10`
   - writes current dual table:
     - `rc_episode_model_dual_inference_current`
2. `run_ew_score.py` upserts dual fields from `rc_episode_model_dual_inference_current` to `rc_ew_score_daily`.
3. reporting / fast-simu read dual fields from `rc_ew_score_daily`.

### 12.9 Reproduction command template
Train (example for `FAIL10` HGB):
```bash
python3 swingmaster/research/train_episode_up20_baseline.py \
  --db /tmp/swingmaster_usa_episode_rank_test.db \
  --table rc_episode_model_full_no_dow_fail10_60d_close \
  --feature-set full_no_dow \
  --model-type hgb \
  --scores-table rc_episode_model_full_no_dow_fail10_60d_close_scores_hgb \
  --top-k 50 100 200
```

Train (example for `UP20` full catboost):
```bash
python3 swingmaster/research/train_episode_up20_baseline.py \
  --db /tmp/swingmaster_usa_episode_rank_test.db \
  --table rc_episode_model_full_up20_60d_close \
  --feature-set full \
  --model-type catboost \
  --scores-table rc_episode_model_full_up20_60d_close_scores_catboost \
  --top-k 50 100 200
```

Populate production dual inference table (default frozen model version):
```bash
python3 swingmaster/cli/run_episode_dual_inference.py \
  --rc-db /path/to/swingmaster_rc_usa_2024_2025.db \
  --mode upsert
```

Internal production score generation (recommended):
```bash
python3 swingmaster/cli/run_episode_dual_score_production.py \
  --rc-db /path/to/swingmaster_rc_usa_2024_2025.db \
  --osakedata-db /path/to/osakedata.db \
  --mode upsert
```

### 12.10 Operational interpretation
- higher `score_up20_meta_v1` means stronger model belief of reaching `+20%` within 60 trading days.
- higher `score_fail10_60d_close_hgb` means stronger model belief that episode fails to reach `+10%` within 60 trading days.
- decision logic uses opposite directions:
  - maximize `score_up20_meta_v1`
  - minimize `score_fail10_60d_close_hgb`

---

## 13. Delta Since Previous Doc Update (Code-Verified)

Scope of this delta section:
- base commit: `2f2bb8fabece30ac59859190c516403c03390ee9`
- head commit at analysis time: `7634345329503e6006fe20465d38cca9623b2055`
- covered range: `2f2bb8f..7634345`
- total diff footprint: `103 files changed, 21252 insertions(+), 500 deletions(-)`

### 13.1 Commit timeline in scope
- `777480a`: Add market regime daily+episode tables and sync CLI
- `3024098`: Add UP20 BULL training CLI and CatBoost/HGB comparison flow
- `4105144`: Add episode exit feature storage, CLI, and migration
- `efd8bf1`: Add UP20 BULL practical evaluation CLI and metrics report
- `de9a6b2`: Add UP20 BULL HGB production scoring flow and score table
- `2fe1449`: Add FAIL10 BULL training/evaluation/scoring flows
- `93c2a31`: Add UP20 BEAR training/eval/scoring flow and wrapper CLI tests
- `1ec7289`: Add UP20 BEAR real-run model artifacts
- `cc4513a`: Add FAIL10 BEAR training/evaluation/scoring flows
- `15f55cd`: Add FAIL10 BEAR real-run model artifacts
- `0b63329`: Add BEAR score wrapper and UP20 SIDEWAYS train/eval/score flow
- `221e365`: Add FAIL10 SIDEWAYS training/eval/scoring flows
- `61bb0e1`: Add UP20 SIDEWAYS real model artifacts
- `5f0eb3e`: Add FAIL10 SIDEWAYS real model artifacts
- `28d99c7`: Add SIDEWAYS model score wrapper CLI
- `95ce2d5`: Add USA BUY rule for BULL fail10/up20 thresholds
- `7634345`: Add USA PASS buy rule `BUY_BULL_PASS_FAIL10_UP20_V2`

### 13.2 New database schema contracts
New migration `009_rc_market_regime_tables.sql`:
- creates `rc_market_regime_daily`:
  - composite PK `(trade_date, market, regime_version)`
  - includes SP500/NDX close, MA50, MA200, per-index state and combined regime
  - state domain: `BULL|CRASH_ALERT|BEAR|SIDEWAYS` (via CHECK constraints)
- creates `rc_episode_regime`:
  - composite PK `(episode_id, regime_version)`
  - stores regime snapshots at EW entry and EW exit dates
  - regime/state fields nullable but constrained to valid state domain when present

New migration `010_rc_episode_exit_features.sql`:
- creates `rc_episode_exit_features` keyed by `episode_id`
- stores deterministic as-of EW-exit feature vector (`EPISODE_EXIT_FEATURES_V1`)
- indexed by `as_of_date` and `(ticker, as_of_date)`

New migration `011_rc_episode_model_score.sql`:
- creates `rc_episode_model_score` keyed by `(episode_id, model_id)`
- columns include:
  - identity/context: `ticker`, `entry_window_date`, `entry_window_exit_date`, `as_of_date`
  - model metadata: `regime_used`, `model_family`, `target_name`, `feature_version`, `regime_version`, `artifact_path`
  - output metadata: `predicted_probability`, `scored_at`

### 13.3 New production modules and CLI contracts
Market regime production:
- module: `swingmaster/regime/production.py`
- CLI: `swingmaster/cli/run_market_regime_sync.py`
- default regime version: `REGIME_USA_MA50_MA200_CLOSE_CRASH2_V1`
- crash confirmation logic parameter: `--crash-confirm-days` (default 2)
- write modes: `upsert|replace-all|insert-missing`

Episode-exit feature production:
- module: `swingmaster/episode_exit_features/production.py`
- CLI: `swingmaster/cli/run_episode_exit_features.py`
- computes feature row as-of each `entry_window_exit_date`
- auto-skips episodes lacking exact as-of price row
- write modes: `upsert|replace-all|insert-missing`

Model scoring wrappers:
- BULL wrapper: `swingmaster/cli/run_bull_model_scores.py`
- BEAR wrapper: `swingmaster/cli/run_bear_model_scores.py`
- SIDEWAYS wrapper: `swingmaster/cli/run_sideways_model_scores.py`
- all wrappers:
  - run both UP20 and FAIL10 scorers
  - apply migrations before scoring
  - report coverage and optional threshold pass counts

### 13.4 New model families and IDs (code-level)
BULL:
- `UP20_BULL_CATBOOST_V1`, `UP20_BULL_HGB_V1`
- `FAIL10_BULL_CATBOOST_V1`, `FAIL10_BULL_HGB_V1`

BEAR:
- `UP20_BEAR_CATBOOST_V1`, `UP20_BEAR_HGB_V1`
- `FAIL10_BEAR_CATBOOST_V1`, `FAIL10_BEAR_HGB_V1`

SIDEWAYS:
- `UP20_SIDEWAYS_CATBOOST_V1`, `UP20_SIDEWAYS_HGB_V1`
- `FAIL10_SIDEWAYS_CATBOOST_V1`, `FAIL10_SIDEWAYS_HGB_V1`

Scoring persistence behavior (all regimes):
- scorers write to `rc_episode_model_score`
- upsert on `(episode_id, model_id)`
- regime filter source: `rc_episode_regime.ew_exit_regime_combined`
- feature source: `rc_episode_exit_features` aligned on `episode_id` and `as_of_date=entry_window_exit_date`

### 13.5 Artifacts added to repository
Added real-run artifact bundles (2026-03-07):
- `models/up20_bear_real_20260307/*`
- `models/fail10_bear_real_20260307/*`
- `models/up20_sideways_real_20260307/*`
- `models/fail10_sideways_real_20260307/*`

Observed evaluation recommendations inside committed eval JSONs:
- `FAIL10_BEAR_MODEL_EVAL_V1.json` -> `FAIL10_BEAR_HGB_V1`
- `FAIL10_SIDEWAYS_MODEL_EVAL_V1.json` -> `FAIL10_SIDEWAYS_CATBOOST_V1`
- `UP20_BEAR_MODEL_EVAL_V1.json` -> `UP20_BEAR_HGB_V1`
- `UP20_SIDEWAYS_MODEL_EVAL_V1.json` -> `UP20_SIDEWAYS_HGB_V1`

### 13.6 Tests added in this range
New tests were added for:
- regime migration/production
- episode-exit feature computation
- UP20/FAIL10 train/eval/score flows for BULL/BEAR/SIDEWAYS
- wrapper CLIs for BULL/BEAR/SIDEWAYS combined scoring

Important boundary:
- no code changes in this range under `swingmaster/core/*` or `swingmaster/app_api/providers/*`.
- therefore state-machine transition graph and signal provider logic were not directly modified by this commit range.

### 13.7 Verified gaps / inconsistencies (must-fix)
Gap A: USA buy-rule JSON vs runtime validator mismatch (critical)
- current `daily_reports/buy_rules/usa.json` includes keys:
  - `regime_eq`
  - `entry_window_exit_state_eq`
  - `fail10_prob_gte`
  - `fail10_prob_lte`
  - `up20_prob_gte`
- current `swingmaster/cli/daily_report.py` validator (`ALLOWED_CONDITION_KEYS`) does not include these keys.
- direct runtime check result (code-verified):
  - `load_buy_rules_config("USA")` -> `ValueError: Invalid buy-rules config: unknown condition key regime_eq`

Gap B: buy-rule docs out-of-sync with current usa.json intent
- `daily_reports/buy_rules/schema_v1.md` and `daily_reports/buy_rules/README.md` still describe older key set and do not define probabilistic model-score keys.

Gap C: report-row field contract missing probabilistic inputs for rule engine
- current buy-rule field map in `daily_report.py` has no mapping for:
  - `regime`
  - `entry_window_exit_state`
  - `fail10_prob`
  - `up20_prob`
- even after validator extension, rule evaluation will require these fields in BUY candidate rows.

### 13.8 Required follow-up implementation order
1. Extend `daily_report.py` validator + condition field map for the five new USA probabilistic keys.
2. Extend report SQL/output payload so the rule engine can read:
   - `regime`
   - `entry_window_exit_state`
   - `fail10_prob`
   - `up20_prob`
3. Update buy-rule schema docs (`schema_v1.md`, `README.md`) to match runtime contract.
4. Add tests in `test_daily_report_buy_rules.py` for new keys and field mapping.
5. Re-run daily-report generation for USA to verify `BUY_BULL_FAIL10_UP20_V1` and `BUY_BULL_PASS_FAIL10_UP20_V2` are executable.

This document is the single V3 reference. Update it whenever V3 signal modules, policy modules, metadata contracts, or report-layer rule contracts change.
