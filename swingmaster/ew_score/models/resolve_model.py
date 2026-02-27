from __future__ import annotations

import re
from pathlib import Path

CANONICAL_MODELS_DIR = Path("/home/kalle/projects/swingmaster/swingmaster/ew_score/models/")

_CANONICAL_RE = re.compile(r"^EW_SCORE_ROLLING_([A-Za-z0-9]+)_V([0-9]+)$")
_ALIAS_RE = re.compile(r"^EW_SCORE_ROLLING_([A-Za-z0-9]+)$")
_LEGACY_RE = re.compile(r"^EW_SCORE_ROLLING_V[^_]+_([A-Za-z0-9]+)$")


class EwScoreRuleResolutionError(ValueError):
    pass


def _find_latest_for_market(models_dir: Path, market: str) -> tuple[str, Path]:
    pattern = re.compile(rf"^EW_SCORE_ROLLING_{re.escape(market)}_V([0-9]+)\.json$")
    max_version = -1
    resolved_name: str | None = None
    for path in models_dir.iterdir():
        if not path.is_file():
            continue
        match = pattern.match(path.name)
        if match is None:
            continue
        try:
            version = int(match.group(1))
        except ValueError:
            continue
        if version > max_version:
            max_version = version
            resolved_name = path.stem
    if resolved_name is None:
        raise EwScoreRuleResolutionError("No matching versioned model files")
    return resolved_name, models_dir / f"{resolved_name}.json"


def resolve_ew_score_rule(
    rule_input: str,
    models_dir: Path = CANONICAL_MODELS_DIR,
) -> tuple[str, Path]:
    if _CANONICAL_RE.match(rule_input):
        path = models_dir / f"{rule_input}.json"
        if path.exists():
            return rule_input, path
        raise EwScoreRuleResolutionError("Canonical versioned model not found")

    alias_match = _ALIAS_RE.match(rule_input)
    if alias_match is not None:
        market = alias_match.group(1)
        return _find_latest_for_market(models_dir=models_dir, market=market)

    legacy_match = _LEGACY_RE.match(rule_input)
    if legacy_match is not None:
        exact_path = models_dir / f"{rule_input}.json"
        if exact_path.exists():
            return rule_input, exact_path
        market = legacy_match.group(1)
        return _find_latest_for_market(models_dir=models_dir, market=market)

    raise EwScoreRuleResolutionError("Unsupported rule format")
