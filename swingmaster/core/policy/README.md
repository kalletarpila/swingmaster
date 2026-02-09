## Purpose
Policy layer that decides next state from provider signals and history ports.

## Key Files
- `rule_v1/policy.py`: rule-based transition policy v1.
- `rule_v2/policy.py`: rule-based transition policy v2.
- `guardrails.py`: guardrail reasons applied during evaluation.
- `ports/state_history_port.py`: history access port interfaces.

## Signal Semantics
`require_row_on_date` (wired through signal providers) means signals are only computed when an OHLC row exists exactly on the as-of date. When enabled and the row is missing, the provider emits `DATA_INSUFFICIENT`, which takes precedence in policy and results in `NO_TRADE`.
