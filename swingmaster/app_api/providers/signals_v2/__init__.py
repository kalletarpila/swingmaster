"""Signal evaluation helpers for osakedata v2.

Responsibilities:
  - Provide modular, deterministic signal evaluation functions.
  - Must not depend on policy state or persistence.

Key definitions:
  - SignalContextV2 and individual signal modules in this package.

Inputs/Outputs:
  - Inputs: per-ticker OHLCV slices and derived windows.
  - Outputs: boolean signal evaluations used by policy.

Role in architecture:
  - Signals → policy decisions → state machine persistence.
"""
