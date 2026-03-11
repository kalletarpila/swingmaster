Schema V1

Each market rule file is a JSON document with this structure:

```json
{
  "market": "<FIN|SE|USA>",
  "version": 1,
  "dual_badge_bands": [
    {
      "badge": "<DUAL_PREMIUM|DUAL_ELITE|DUAL_STRONG|DUAL_QUALIFIED>",
      "up20_prob_gte": <number>,
      "fail10_prob_lte": <number>
    }
  ],
  "rules": [
    {
      "rule_hit": "<STRING>",
      "trigger": "<NEW_EW|NEW_PASS|NEW_NOTRADE|EW_SNAPSHOT>",
      "enabled": "<BOOLEAN, OPTIONAL, DEFAULT=true>",
      "conditions": {
        "<condition_key>": <value>
      }
    }
  ]
}
```

Field descriptions

- `market`
  - Must be one of `FIN`, `SE`, or `USA`.
- `version`
  - Must be integer `1`.
- `rules`
  - Array of rule objects.
- `dual_badge_bands`
  - Optional array for dual badge classification bands.
  - Evaluated in list order; first match wins.
  - Used to derive `dual_buy_badge` and `dual_badge_present`.
- `rule_hit`
  - Stable semantic identifier for the rule.
- `trigger`
  - Base-section selector used by the runner.
- `conditions`
  - Flat key-value filter map over already-computed report columns.
- `enabled`
  - Optional boolean flag.
  - `true` means rule is active.
  - `false` means rule is ignored by loaders/runners.
  - Missing `enabled` is treated as `true`.

Allowed triggers

- `NEW_EW`
- `NEW_PASS`
- `NEW_NOTRADE`
- `EW_SNAPSHOT`

Allowed condition keys

- `fastpass_score_gte` (number)
- `fastpass_level_eq` (integer)
- `rolling_end_level_eq` (integer)
- `dual_buy_badge_eq` (string)
- `dual_badge_present_eq` (boolean)
- `regime_eq` (string)
- `entry_window_exit_state_eq` (string)
- `fail10_prob_gte` (number)
- `fail10_prob_lte` (number)
- `up20_prob_gte` (number)
- `days_in_current_episode_gte` (integer)
- `days_in_current_episode_lte` (integer)
- `days_in_stabilizing_before_ew_gte` (integer)
- `days_in_stabilizing_before_ew_eq` (integer)

Validation requirements for the future runner

- Unknown triggers must be rejected.
- Unknown condition keys must be rejected.
- Unknown top-level fields inside a rule must be rejected.
- `enabled`, when present, must be boolean.
- `dual_badge_bands`, when present, must contain objects with exactly:
  - `badge`, `up20_prob_gte`, `fail10_prob_lte`.
- Conditions are ANDed together.
- No OR, no composite rules, and no nested logic are allowed in v1.

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
