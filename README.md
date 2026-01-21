# swingmaster

swingmaster is a market state engine for swing investing, providing a structured foundation for core logic, infrastructure, and interfaces.

## Architecture
- **core**: domain concepts, signals, policies, and engine primitives
- **infra**: persistence adapters (e.g., SQLite) and supporting services
- **app_api**: application-facing entrypoints and orchestration surfaces
- **ui**: thin presentation layers (Flet first, Qt later)

Logic and behavior will be added incrementally via Codex; this scaffold holds the initial layout only.
