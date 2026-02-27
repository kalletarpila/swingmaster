from __future__ import annotations

from swingmaster.ew_score.training.versioning import next_version_for_market


def test_next_version_for_market() -> None:
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "EW_SCORE_ROLLING_FIN_V1.json").write_text("{}", encoding="utf-8")
        (root / "EW_SCORE_ROLLING_FIN_V3.json").write_text("{}", encoding="utf-8")
        (root / "EW_SCORE_ROLLING_SE_V2.json").write_text("{}", encoding="utf-8")

        assert next_version_for_market(root, "FIN") == 4
        assert next_version_for_market(root, "SE") == 3
