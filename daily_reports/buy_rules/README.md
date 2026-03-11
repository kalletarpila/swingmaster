Purpose

This directory contains buy-rule configuration files for the daily report runner.

V1 design

- The runner will first execute the existing base SQL.
- The base SQL will produce base sections such as `NEW_EW`, `NEW_PASS`, `NEW_NOTRADE`, and `EW_SNAPSHOT`.
- The runner will then load the JSON rules from this directory.
- The runner will filter the already-computed base rows in Python.
- V1 does not generate SQL from these rules.

Triggers

- `NEW_EW`: ticker entered `ENTRY_WINDOW` on `as_of_date`
- `NEW_PASS`: ticker transitioned to `PASS` on `as_of_date`
- `NEW_NOTRADE`: ticker exited `ENTRY_WINDOW` to `NO_TRADE` on `as_of_date`
- `EW_SNAPSHOT`: ticker is in `ENTRY_WINDOW` on `as_of_date`

Allowed condition keys in v1

- `fastpass_score_gte`
- `fastpass_level_eq`
- `rolling_end_level_eq`
- `dual_buy_badge_eq`
- `dual_badge_present_eq`
- `regime_eq`
- `entry_window_exit_state_eq`
- `fail10_prob_gte`
- `fail10_prob_lte`
- `up20_prob_gte`
- `days_in_current_episode_gte`
- `days_in_current_episode_lte`
- `days_in_stabilizing_before_ew_gte`
- `days_in_stabilizing_before_ew_eq`

Rules semantics

- Conditions are ANDed together.
- No OR logic is supported in v1.
- No nested logic is supported in v1.
- Composite rules are intentionally excluded in v1.
- Each rule may include optional `enabled: true|false`.
- Missing `enabled` is treated as active (`true`).
- Disabled rules (`enabled: false`) are ignored by both daily report and fast simulator via shared loader.

Dual badge bands (optional)

- Config may include top-level `dual_badge_bands`.
- Each band item must define `badge`, `up20_prob_gte`, `fail10_prob_lte`.
- Bands are evaluated in order; first match wins.
- This derives row fields:
  - `dual_buy_badge` (string or null)
  - `dual_badge_present` (boolean)

Example

```json
{
  "market": "USA",
  "version": 1,
  "dual_badge_bands": [
    {
      "badge": "DUAL_PREMIUM",
      "up20_prob_gte": 0.80,
      "fail10_prob_lte": 0.20
    }
  ],
  "rules": [
    {
      "rule_hit": "USA_PASS_DUAL",
      "trigger": "NEW_PASS",
      "enabled": true,
      "conditions": {
        "dual_badge_present_eq": true
      }
    }
  ]
}
```
