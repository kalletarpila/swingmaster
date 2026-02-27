from __future__ import annotations

import re
from pathlib import Path


def next_version_for_market(models_dir: Path, market: str, base_id: str = "EW_SCORE_ROLLING") -> int:
    pattern = re.compile(rf"^{re.escape(base_id)}_{re.escape(market)}_V([0-9]+)\.json$")
    max_version = 0
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
    return max_version + 1


def resolve_versioned_rule_id(models_dir: Path, market: str, base_id: str = "EW_SCORE_ROLLING") -> tuple[str, int]:
    version = next_version_for_market(models_dir=models_dir, market=market, base_id=base_id)
    return f"{base_id}_{market}_V{version}", version
