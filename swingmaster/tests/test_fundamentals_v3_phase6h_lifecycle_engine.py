from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

import pytest

from swingmaster.fundamentals import v3_phase6d_lifecycle_recalibration as p6d
from swingmaster.fundamentals import v3_phase6f_valuation_engine as p6f
from swingmaster.fundamentals import v3_phase6g_legacy2_score_engine as p6g
from swingmaster.fundamentals import v3_phase6h_lifecycle_engine as p6h


def test_exact_model_version() -> None:
    assert p6h.MODEL_VERSION == "V3_LIFECYCLE_EBIT_FIRST_V1"


def test_exact_fingerprint() -> None:
    assert p6h.EXPECTED_LIFECYCLE_FINGERPRINT == "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


def test_exact_frozen_state_set() -> None:
    assert p6d.STATE_ORDER == [
        "DISTRESS_CONTRACTION", "EARLY_RECOVERY", "POSITIVE_INFLECTION", "PROFITABLE_GROWTH",
        "HIGH_GROWTH_EXPANSION", "MATURE_STABLE", "DECELERATING", "DECLINING", "NOT_READY",
    ]


@pytest.mark.parametrize("forbidden", ["legacy2_score", "v3_score", "valuation", "close", "market_cap", "technical", "rsi", "sma"])
def test_no_score_valuation_or_technical_inputs(forbidden: str) -> None:
    assert forbidden not in json.dumps(p6h.feature_contract()).lower()


def test_ebitda_not_required() -> None:
    assert all(row["uses_ebitda"] == 0 for row in p6h.feature_contract())


def test_ttm_id_preserved(tmp_path: Path) -> None:
    row = p6h.build_lifecycle_features(make_db(tmp_path), "2021-01-01", "2021-12-31")[0]
    assert row["ttm_id"] is not None


def test_endpoint_lineage_exact(tmp_path: Path) -> None:
    row = p6h.build_lifecycle_features(make_db(tmp_path), "2021-01-01", "2021-12-31")[0]
    assert row["endpoint_quarter_id"] == row["ttm_id"]
    assert row["endpoint_fiscal_year"] == 2021


def test_previous_comparison_endpoint_lineage_exact(tmp_path: Path) -> None:
    row = p6h.build_lifecycle_features(make_db(tmp_path), "2021-01-01", "2021-12-31")[0]
    assert row["prev4_ttm_id"] == row["ttm_id"] - 4


def test_revenue_ttm_yoy() -> None:
    assert p6d.safe_growth(120.0, 100.0) == 0.2


@pytest.mark.parametrize(
    ("prev", "cur", "state"),
    [
        (-10.0, -12.0, "NEGATIVE_AND_DETERIORATING"),
        (-10.0, -5.0, "NEGATIVE_BUT_IMPROVING"),
        (-1.0, 2.0, "CROSSING_TO_POSITIVE"),
        (1.0, 3.0, "POSITIVE_AND_GROWING"),
        (3.0, 1.0, "POSITIVE_AND_DECLINING"),
        (1.0, -1.0, "POSITIVE_TURNING_NEGATIVE"),
    ],
)
def test_ebit_signed_transition_states(prev: float, cur: float, state: str) -> None:
    assert p6d.classify_signed_transition(prev, cur) == state


@pytest.mark.parametrize(
    ("prev", "cur", "state"),
    [(-1.0, 1.0, "CROSSING_TO_POSITIVE"), (1.0, -1.0, "POSITIVE_TURNING_NEGATIVE"), (-2.0, -1.0, "NEGATIVE_BUT_IMPROVING")],
)
def test_fcf_transition_states(prev: float, cur: float, state: str) -> None:
    assert p6d.classify_signed_transition(prev, cur) == state


def test_exact_ebit_margin() -> None:
    assert p6d.safe_div(10.0, 100.0) == 0.1


def test_exact_ebit_margin_trend() -> None:
    assert 0.15 - 0.10 == pytest.approx(0.05)


def test_acceleration_feature(tmp_path: Path) -> None:
    rows = p6h.build_lifecycle_features(make_db(tmp_path), "2021-01-01", "2021-12-31")
    assert "revenue_growth_acceleration" in rows[0]
    assert "revenue_growth_1q_delta" in rows[0]


def test_no_naive_percent_growth_across_zero() -> None:
    assert p6d.safe_growth(1.0, -1.0) is None


def test_deterministic_single_raw_state(tmp_path: Path) -> None:
    row = p6h.apply_raw_state(p6h.build_lifecycle_features(make_db(tmp_path), "2021-01-01", "2021-12-31"), thresholds())[0]
    assert row["raw_state"] in p6d.STATE_ORDER


def test_state_precedence_exact() -> None:
    assert "distress" in p6h.state_precedence_md()


def test_not_ready_and_no_missing_distress_fallback() -> None:
    state, conf = p6d.classify_raw_state({"revenue_growth_yoy_ttm": None, "ebit_transition": "MISSING_DATA", "ebit_margin": None}, thresholds())
    assert state == "NOT_READY"
    assert conf == "NOT_READY"


def test_previous_state_memory(tmp_path: Path) -> None:
    rows = p6h.build_historical_lifecycle(make_db(tmp_path), thresholds(), "2021-01-01", "2021-12-31")
    assert rows[1]["previous_final_state"] == rows[0]["final_state"]


def test_confirmation_suppresses_single_candidate() -> None:
    raw = raw_sequence(["MATURE_STABLE", "DECLINING", "MATURE_STABLE"])
    final = p6h.apply_temporal_state(raw)
    assert final[1]["transition_reason"] == "SUPPRESSED_PENDING_CONFIRMATION"


def test_hysteresis_two_quarter_confirmation() -> None:
    raw = raw_sequence(["MATURE_STABLE", "DECLINING", "DECLINING"])
    final = p6h.apply_temporal_state(raw)
    assert final[2]["transition_reason"] == "CONFIRMED_TWO_QUARTERS"


def test_minimum_state_age_logic() -> None:
    final = p6h.apply_temporal_state(raw_sequence(["MATURE_STABLE", "MATURE_STABLE"]))
    assert final[1]["state_age"] == 2


def test_hard_inflection_exception() -> None:
    raw = raw_sequence(["MATURE_STABLE", "DECLINING"])
    raw[1]["ebit_transition"] = "POSITIVE_TURNING_NEGATIVE"
    final = p6h.apply_temporal_state(raw)
    assert final[1]["transition_reason"] == "HARD_INFLECTION"


def test_state_age() -> None:
    assert p6h.apply_temporal_state(raw_sequence(["MATURE_STABLE", "MATURE_STABLE", "MATURE_STABLE"]))[-1]["state_age"] == 3


def test_suppressed_transition_reason() -> None:
    assert p6h.apply_temporal_state(raw_sequence(["MATURE_STABLE", "DECELERATING"]))[1]["transition_reason"] == "SUPPRESSED_PENDING_CONFIRMATION"


def test_allowed_direct_jump() -> None:
    raw = raw_sequence(["MATURE_STABLE", "DISTRESS_CONTRACTION"])
    raw[1]["revenue_growth_yoy_ttm"] = -0.5
    assert p6h.apply_temporal_state(raw)[1]["final_state"] == "DISTRESS_CONTRACTION"


def test_blocked_transition() -> None:
    assert p6h.apply_temporal_state(raw_sequence(["MATURE_STABLE", "DISTRESS_CONTRACTION"]))[1]["final_state"] == "MATURE_STABLE"


def test_immediate_reversal_handling() -> None:
    final = p6h.apply_temporal_state(raw_sequence(["MATURE_STABLE", "DECLINING", "MATURE_STABLE"]))
    assert final[-1]["final_state"] == "MATURE_STABLE"


def test_missing_quarter_does_not_count_as_consecutive_confirmation() -> None:
    rows = raw_sequence(["MATURE_STABLE", "DECLINING"])
    rows[1]["endpoint_fiscal_quarter"] = "Q4"
    assert p6h.apply_temporal_state(rows)[1]["candidate_confirmation_count"] == 1


def test_fiscal_gap_behavior_deterministic() -> None:
    rows = p6h.apply_temporal_state(list(reversed(raw_sequence(["MATURE_STABLE", "DECLINING", "DECLINING"]))))
    assert rows[-1]["final_state"] == "DECLINING"


def test_readiness_exact_high() -> None:
    assert p6d.readiness({"revenue_growth_yoy_ttm": 0.1, "ebit_transition": "POSITIVE_AND_GROWING", "ebit_margin": 0.1, "fcf_transition": "POSITIVE_AND_GROWING", "fcf_margin": 0.1}) == (True, "HIGH")


def test_readiness_exact_medium() -> None:
    assert p6d.readiness({"revenue_growth_yoy_ttm": 0.1, "ebit_transition": "POSITIVE_AND_GROWING", "ebit_margin": 0.1, "fcf_transition": "MISSING_DATA", "fcf_margin": None}) == (True, "MEDIUM")


def test_confidence_independent_from_state() -> None:
    state, conf = p6d.classify_raw_state({"revenue_growth_yoy_ttm": 0.2, "ebit_transition": "POSITIVE_AND_GROWING", "ebit_margin": 0.2, "ebit_margin_change": 0.0, "fcf_transition": "MISSING_DATA", "fcf_margin": None}, thresholds())
    assert state != "NOT_READY"
    assert conf == "MEDIUM"


def test_chronological_processing(tmp_path: Path) -> None:
    rows = p6h.build_historical_lifecycle(make_db(tmp_path), thresholds(), "2021-01-01", "2021-12-31")
    assert rows == sorted(rows, key=lambda r: (int(r["company_id"]), p6h.fiscal_sort_key(r)))


def test_shuffled_input_order_gives_same_output(tmp_path: Path) -> None:
    assert p6h.prove_chronological_determinism(make_db(tmp_path), thresholds())["shuffled_input_changes_result"] is False


@pytest.mark.parametrize("year", [2021, 2022, 2023, 2024, 2025, 2026, 2020])
def test_year_parity(tmp_path: Path, year: int) -> None:
    db, root = phase6d_fixture(tmp_path)
    monkey_thresholds = p6h.verify_frozen_lifecycle(db, root)["thresholds"]
    row = next(r for r in p6h.parity_summary(db, monkey_thresholds) if r["year"] == year)
    assert row["mismatches"] == 0


def test_state_counts_parity(tmp_path: Path) -> None:
    db, root = phase6d_fixture(tmp_path)
    assert all(row["state_counts_match"] for row in p6h.parity_summary(db, p6h.verify_frozen_lifecycle(db, root)["thresholds"]))


def test_transition_matrix_parity(tmp_path: Path) -> None:
    db, root = phase6d_fixture(tmp_path)
    assert all(row["transition_summary_match"] for row in p6h.parity_summary(db, p6h.verify_frozen_lifecycle(db, root)["thresholds"]))


def test_state_duration_parity(tmp_path: Path) -> None:
    rows = p6h.build_historical_lifecycle(make_db(tmp_path), thresholds(), "2021-01-01", "2021-12-31")
    assert p6d.churn_analysis(rows, "final_state")["median_state_duration"] is not None


def test_case_level_parity(tmp_path: Path) -> None:
    rows = p6h.build_historical_lifecycle(make_db(tmp_path), thresholds(), "2021-01-01", "2021-12-31")
    assert all(row["parity_status"] == "MATCH" for row in p6h.case_parity(rows))


def test_deterministic_identity(tmp_path: Path) -> None:
    assert p6h.prove_idempotency(tmp_path / "idem.db", thresholds())["duplicates"] == 0


def test_duplicate_prevention(tmp_path: Path) -> None:
    assert p6h.prove_idempotency(tmp_path / "idem.db", thresholds())["second_apply"] == {"NOOP": 8}


def test_lifecycle_model_version_coexistence(tmp_path: Path) -> None:
    db = make_db(tmp_path)
    row = p6h.build_historical_lifecycle(db, thresholds(), "2021-01-01", "2021-12-31")[0]
    snap = p6h.build_lifecycle_snapshot(row)
    p6h.apply_lifecycle_snapshots(db, [snap], run_id="a")
    other = dict(snap, lifecycle_model_version="OTHER")
    p6h.apply_lifecycle_snapshots(db, [other], run_id="b")
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM v3_lifecycle").fetchone()[0] == 2


@pytest.mark.parametrize("field", ["raw_state", "final_state", "previous_final_state", "transition_reason", "state_age", "confidence", "lifecycle_fingerprint"])
def test_snapshot_persists_required_fields(tmp_path: Path, field: str) -> None:
    row = p6h.build_historical_lifecycle(make_db(tmp_path), thresholds(), "2021-01-01", "2021-12-31")[1]
    assert field in p6h.build_lifecycle_snapshot(row)


def test_first_test_apply(tmp_path: Path) -> None:
    assert p6h.prove_idempotency(tmp_path / "idem.db", thresholds())["first_apply"] == {"INSERTED": 8}


def test_second_apply_noop(tmp_path: Path) -> None:
    assert p6h.prove_idempotency(tmp_path / "idem.db", thresholds())["second_apply"] == {"NOOP": 8}


def test_future_quarter_does_not_mutate_history(tmp_path: Path) -> None:
    assert p6h.prove_idempotency(tmp_path / "idem.db", thresholds())["future_quarter_mutates_history"] is False


def test_input_query_order_irrelevant(tmp_path: Path) -> None:
    assert p6h.prove_chronological_determinism(make_db(tmp_path), thresholds())["shuffled_input_changes_result"] is False


def test_corrected_older_endpoint_recomputes_affected_company_forward(tmp_path: Path) -> None:
    assert p6h.prove_correction_recompute(tmp_path / "corr.db", thresholds())["affected_company_recomputed"] is True


def test_unrelated_companies_unchanged(tmp_path: Path) -> None:
    assert p6h.prove_correction_recompute(tmp_path / "corr.db", thresholds())["unrelated_companies_changed"] is False


def test_prior_unaffected_endpoints_unchanged(tmp_path: Path) -> None:
    assert p6h.prove_correction_recompute(tmp_path / "corr.db", thresholds())["prior_unaffected_endpoints_unchanged"] is True


def test_deterministic_convergence_defined(tmp_path: Path) -> None:
    assert p6h.prove_correction_recompute(tmp_path / "corr.db", thresholds())["deterministic"] is True


def test_lifecycle_fingerprint_unchanged() -> None:
    assert p6h.EXPECTED_LIFECYCLE_FINGERPRINT == "18c01984ee5ca69acaca64ab6ff2d5b1faa27d8400137f6d08200926e5658f9e"


def test_legacy2_score_fingerprint_unchanged() -> None:
    assert p6g.EXPECTED_SCORE_FINGERPRINT == "8c2d8400afa77f1437afef94cff5016d52a1525c18b80fa787db1bbe1e1a36d0"


def test_phase6g_behavior_unchanged() -> None:
    assert p6g.MODEL_VERSION == "V3_LEGACY2_FUNDAMENTAL_SCORE_V1"


def test_phase6f_valuation_behavior_unchanged() -> None:
    assert p6f.MODEL_VERSION == "V3_VALUATION_POST_PUBLICATION_SNAPSHOT_V1"


@pytest.mark.parametrize("key", ["canonical", "ttm", "score", "valuation", "lifecycle"])
def test_production_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    summary = run_fixture_phase(tmp_path, monkeypatch)
    assert summary["production_writes"][key] == 0


def test_empty_legacy_lifecycle_schema_rebuilt(tmp_path: Path) -> None:
    db = tmp_path / "schema.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY)")
        assert p6h.ensure_lifecycle_schema(conn) == "REBUILT_EMPTY_TABLE"


def test_non_empty_legacy_lifecycle_schema_refuses_rebuild(tmp_path: Path) -> None:
    db = tmp_path / "schema_non_empty.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE v3_lifecycle(lifecycle_id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO v3_lifecycle VALUES (1)")
        with pytest.raises(RuntimeError, match=p6h.CLASSIFICATION_SCHEMA_REQUIRED):
            p6h.ensure_lifecycle_schema(conn)


def test_next_phase_constant() -> None:
    assert p6h.NEXT_PHASE == "MASTER PLAN PHASE 6I - PRODUCTION REBUILD & PROVING"


def make_db(tmp_path: Path) -> Path:
    db = tmp_path / f"lifecycle_{uuid.uuid4().hex}.db"
    p6h.create_fixture_db(db)
    return db


def thresholds() -> dict[str, float]:
    return {
        "revenue_low_growth": -0.02,
        "revenue_strong_growth": 0.25,
        "revenue_very_strong_growth": 0.60,
        "positive_ebit_margin_floor": 0.0,
        "healthy_ebit_margin": 0.12,
        "strong_ebit_margin": 0.20,
        "margin_expansion": 0.02,
        "margin_contraction": -0.04,
        "severe_revenue_contraction": -0.19,
    }


def raw_sequence(states: list[str]) -> list[dict[str, object]]:
    rows = []
    for idx, state in enumerate(states, start=1):
        rows.append({
            "company_id": 1,
            "ticker": "AAA",
            "active": 1,
            "ttm_id": idx,
            "endpoint_quarter_id": idx,
            "endpoint_fiscal_year": 2021,
            "endpoint_fiscal_quarter": f"Q{idx}",
            "period_end": f"2021-{idx * 3:02d}-28",
            "ttm_available_date": f"2021-{idx * 3:02d}-28",
            "raw_state": state,
            "confidence": "HIGH",
            "lifecycle_ready": int(state != "NOT_READY"),
            "revenue_growth_yoy_ttm": 0.1,
            "ebit_transition": "POSITIVE_AND_GROWING",
            "ebit_margin": 0.1,
        })
    return rows


def phase6d_fixture(tmp_path: Path) -> tuple[Path, Path]:
    db = make_db(tmp_path)
    root = tmp_path / f"phase6d_{uuid.uuid4().hex}"
    p6d.run_phase6d_lifecycle_recalibration(v3_db=db, artifact_root=root, write_durable_docs=False)
    return db, root


def run_fixture_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    db, root = phase6d_fixture(tmp_path)
    monkeypatch.setattr(p6h, "EXPECTED_LIFECYCLE_FINGERPRINT", json.loads((root / "phase6d_lifecycle_fingerprint.json").read_text())["fingerprint"])
    return p6h.run_phase6h_lifecycle_engine(v3_db=db, artifact_root=tmp_path / f"out_{uuid.uuid4().hex}", lifecycle_artifact_root=root, write_durable_docs=False)
