## Purpose
SQLite repositories and data access for state history and transitions.

## Key Files
- `repos/rc_state_repo.py`: writes rc_state_daily and rc_transition records.
- `migrations/029_rc_earnings_event.sql`: creates the canonical read/write target table for normalized earnings announcement events; source retrieval and diagnostics are documented in `docs/yahoo_earnings_event_source_and_schema.md`.
