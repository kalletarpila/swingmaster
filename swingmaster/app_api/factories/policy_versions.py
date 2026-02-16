"""Policy version constants used by app configuration."""

POLICY_V1 = "v1"
POLICY_V2 = "v2"
POLICY_V3 = "v3"
# v1 is disabled; keep constant for legacy parsing only.
ALLOWED_POLICY_VERSIONS = {POLICY_V2, POLICY_V3}
