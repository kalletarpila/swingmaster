---
swingmaster – LOCKED DESIGN PRINCIPLES (v1)

This project is a deterministic, exclusion-first market state engine for swing investing.

LOCKED CONSTRAINTS:
- Core-first architecture
- UI must be a thin view layer (Flet now, Qt later)
- No UI dependencies in core
- No trading logic (no positions, no orders)
- No ML or learning models in v1
- Deterministic, auditable behavior only
- State machine transitions must respect a fixed allowed graph
- No churn / no rapid oscillation between states
- Reason codes are mandatory for every non-neutral decision

These principles must not be violated without explicit redesign.

---
