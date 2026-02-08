## Purpose
Modular provider-level signals (v2), one signal per file.

## Invariants
- Signals are deterministic and must not depend on policy state.
- Each module emits a single SignalKey by name.

## Key Files
- `entry_setup_valid.py`, `stabilization_confirmed.py`, `trend_started.py`, `trend_matured.py`, `invalidated.py`.
