Schema V1

Each market rule file is a JSON document with this structure:

```json
{
  "market": "<FIN|SE|USA>",
  "version": 1,
  "rules": [
    {
      "rule_hit": "<STRING>",
      "trigger": "<NEW_EW|NEW_PASS|NEW_NOTRADE|EW_SNAPSHOT>",
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
- `rule_hit`
  - Stable semantic identifier for the rule.
- `trigger`
  - Base-section selector used by the runner.
- `conditions`
  - Flat key-value filter map over already-computed report columns.

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
- Conditions are ANDed together.
- No OR, no composite rules, and no nested logic are allowed in v1.

Example

```json
{
  "market": "SE",
  "version": 1,
  "rules": [
    {
      "rule_hit": "SE_ENTRY_FP80",
      "trigger": "NEW_EW",
      "conditions": {
        "fastpass_score_gte": 0.80
      }
    }
  ]
}
```
