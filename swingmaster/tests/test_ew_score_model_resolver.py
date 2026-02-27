from __future__ import annotations

from pathlib import Path

from swingmaster.ew_score.models.resolve_model import resolve_ew_score_rule


def test_resolve_ew_score_rule_alias_and_legacy() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        models_dir = Path(tmp)
        (models_dir / "EW_SCORE_ROLLING_FIN_V1.json").write_text("{}", encoding="utf-8")
        (models_dir / "EW_SCORE_ROLLING_FIN_V3.json").write_text("{}", encoding="utf-8")
        (models_dir / "EW_SCORE_ROLLING_SE_V2.json").write_text("{}", encoding="utf-8")

        resolved_alias, _ = resolve_ew_score_rule("EW_SCORE_ROLLING_FIN", models_dir=models_dir)
        resolved_versioned, _ = resolve_ew_score_rule("EW_SCORE_ROLLING_FIN_V1", models_dir=models_dir)
        resolved_legacy_fallback, _ = resolve_ew_score_rule(
            "EW_SCORE_ROLLING_V1_FIN",
            models_dir=models_dir,
        )

        assert resolved_alias == "EW_SCORE_ROLLING_FIN_V3"
        assert resolved_versioned == "EW_SCORE_ROLLING_FIN_V1"
        assert resolved_legacy_fallback == "EW_SCORE_ROLLING_FIN_V3"

        (models_dir / "EW_SCORE_ROLLING_V1_FIN.json").write_text("{}", encoding="utf-8")
        resolved_legacy_exact, _ = resolve_ew_score_rule(
            "EW_SCORE_ROLLING_V1_FIN",
            models_dir=models_dir,
        )
        assert resolved_legacy_exact == "EW_SCORE_ROLLING_V1_FIN"
